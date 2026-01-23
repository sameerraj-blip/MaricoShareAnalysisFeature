/**
 * Chunking Service
 * Splits large CSV/Excel files into chunks during upload for faster query processing
 * Each chunk is stored separately and loaded only when needed based on query filters
 */

import * as fs from 'fs/promises';
import * as path from 'path';
import * as os from 'os';
import { uploadFileToBlob, getFileFromBlob } from './blobStorage.js';
import { streamParseCsv } from './streamingFileParser.js';
import { parseFile } from './fileParser.js';
import { DataSummary } from '../shared/schema.js';

export interface ChunkMetadata {
  chunkId: string;
  blobName: string;
  rowCount: number;
  rowStart: number;
  rowEnd: number;
  // Index metadata for fast filtering
  dateRanges?: {
    column: string;
    min: string | null;
    max: string | null;
  }[];
  valueRanges?: {
    column: string;
    min: number | null;
    max: number | null;
  }[];
  categories?: {
    column: string;
    values: (string | number)[];
  }[];
  // Column presence
  columns: string[];
}

export interface ChunkIndex {
  sessionId: string;
  totalRows: number;
  totalChunks: number;
  chunks: ChunkMetadata[];
  summary: DataSummary;
  createdAt: number;
}

const CHUNK_SIZE = 100000; // 100k rows per chunk (adjustable)
const CHUNK_DIR = path.join(os.tmpdir(), 'marico-chunks');

/**
 * Chunk a file during upload and store chunks separately
 */
export async function chunkFile(
  buffer: Buffer,
  sessionId: string,
  fileName: string,
  summary: DataSummary,
  onProgress?: (progress: { stage: string; progress: number; message?: string }) => void
): Promise<ChunkIndex> {
  // Ensure chunk directory exists
  await fs.mkdir(CHUNK_DIR, { recursive: true });

  onProgress?.({ stage: 'chunking', progress: 0, message: 'Starting file chunking...' });

  const chunks: ChunkMetadata[] = [];
  let totalRows = 0;
  let chunkIndex = 0;
  let currentChunk: Record<string, any>[] = [];
  let rowStart = 0;

  // Detect file type
  const isCsv = fileName.toLowerCase().endsWith('.csv');
  const isExcel = fileName.toLowerCase().match(/\.(xls|xlsx)$/i);

  if (isCsv) {
    // Stream parse CSV
    // First pass: count rows to get total
    let estimatedTotal = 0;
    const { rowCount, columns, processChunks } = await streamParseCsv(buffer, {
      chunkSize: 10000,
      onProgress: (processed, total) => {
        // Use total if available, otherwise estimate based on processed
        estimatedTotal = total || processed * 2; // Rough estimate
        onProgress?.({
          stage: 'chunking',
          progress: total ? Math.floor((processed / total) * 40) : Math.min(40, Math.floor(processed / 10000)),
          message: total ? `Processing ${processed} / ${total} rows...` : `Processing ${processed} rows...`,
        });
      },
    });

    totalRows = rowCount;
    let currentRowIndex = 0;

    // Process chunks and create file chunks
    await processChunks(async (chunk) => {
      for (const row of chunk) {
        currentChunk.push(row);
        currentRowIndex++;

        // When chunk is full, save it
        if (currentChunk.length >= CHUNK_SIZE) {
          const chunkMetadata = await saveChunk(
            currentChunk,
            sessionId,
            chunkIndex,
            rowStart,
            rowStart + currentChunk.length - 1,
            summary
          );
          chunks.push(chunkMetadata);
          chunkIndex++;
          rowStart = rowStart + currentChunk.length;
          currentChunk = [];

          onProgress?.({
            stage: 'chunking',
            progress: 40 + Math.floor((chunkIndex * 100 / Math.ceil(rowCount / CHUNK_SIZE)) * 40),
            message: `Saved chunk ${chunkIndex + 1} (${currentRowIndex} / ${rowCount} rows)...`,
          });
        }
      }
    });

    // Save remaining rows
    if (currentChunk.length > 0) {
      const chunkMetadata = await saveChunk(
        currentChunk,
        sessionId,
        chunkIndex,
        rowStart,
        rowStart + currentChunk.length - 1,
        summary
      );
      chunks.push(chunkMetadata);
    }
  } else if (isExcel) {
    // For Excel, parse in memory (smaller files) or use streaming if available
    onProgress?.({ stage: 'chunking', progress: 10, message: 'Parsing Excel file...' });
    const allData = await parseFile(buffer, fileName);
    totalRows = allData.length;

    // Split into chunks
    for (let i = 0; i < allData.length; i += CHUNK_SIZE) {
      const chunk = allData.slice(i, i + CHUNK_SIZE);
      const chunkMetadata = await saveChunk(
        chunk,
        sessionId,
        chunkIndex,
        i,
        Math.min(i + chunk.length - 1, allData.length - 1),
        summary
      );
      chunks.push(chunkMetadata);
      chunkIndex++;

      onProgress?.({
        stage: 'chunking',
        progress: 10 + Math.floor((chunkIndex * 100 / Math.ceil(allData.length / CHUNK_SIZE)) * 80),
        message: `Saved chunk ${chunkIndex} / ${Math.ceil(allData.length / CHUNK_SIZE)}...`,
      });
    }
  } else {
    throw new Error('Unsupported file type. Only CSV and Excel files are supported.');
  }

  // Create chunk index
  const chunkIndexData: ChunkIndex = {
    sessionId,
    totalRows,
    totalChunks: chunks.length,
    chunks,
    summary,
    createdAt: Date.now(),
  };

  // Save chunk index to blob storage
  const indexBlobName = `chunks/${sessionId}/index.json`;
  await uploadFileToBlob(
    Buffer.from(JSON.stringify(chunkIndexData, null, 2)),
    indexBlobName,
    'system',
    'application/json'
  );

  onProgress?.({ stage: 'complete', progress: 100, message: `File chunked into ${chunks.length} chunks!` });

  return chunkIndexData;
}

/**
 * Save a chunk to blob storage and compute metadata
 */
async function saveChunk(
  chunk: Record<string, any>[],
  sessionId: string,
  chunkIndex: number,
  rowStart: number,
  rowEnd: number,
  summary: DataSummary
): Promise<ChunkMetadata> {
  const chunkId = `chunk_${chunkIndex}`;
  const blobName = `chunks/${sessionId}/${chunkId}.json`;

  // Compute chunk metadata for fast filtering
  const dateRanges: ChunkMetadata['dateRanges'] = [];
  const valueRanges: ChunkMetadata['valueRanges'] = [];
  const categories: ChunkMetadata['categories'] = [];

  // Analyze date columns
  for (const dateCol of summary.dateColumns) {
    const dates = chunk
      .map(row => {
        const val = row[dateCol];
        if (!val) return null;
        const date = new Date(val);
        return isNaN(date.getTime()) ? null : date.toISOString();
      })
      .filter((d): d is string => d !== null);

    if (dates.length > 0) {
      dates.sort();
      dateRanges.push({
        column: dateCol,
        min: dates[0],
        max: dates[dates.length - 1],
      });
    }
  }

  // Analyze numeric columns
  for (const numCol of summary.numericColumns) {
    const values = chunk
      .map(row => {
        const val = row[numCol];
        if (typeof val === 'number' && !isNaN(val) && isFinite(val)) {
          return val;
        }
        const num = Number(val);
        return isNaN(num) || !isFinite(num) ? null : num;
      })
      .filter((v): v is number => v !== null);

    if (values.length > 0) {
      valueRanges.push({
        column: numCol,
        min: Math.min(...values),
        max: Math.max(...values),
      });
    }
  }

  // Analyze category columns (string columns with limited unique values)
  for (const col of summary.columns) {
    if (col.type === 'string' && !summary.dateColumns.includes(col.name) && !summary.numericColumns.includes(col.name)) {
      const uniqueValues = new Set<string | number>();
      for (const row of chunk) {
        const val = row[col.name];
        if (val !== null && val !== undefined && val !== '') {
          uniqueValues.add(val);
        }
      }

      // If unique values are limited (less than 100), track them
      if (uniqueValues.size > 0 && uniqueValues.size < 100) {
        categories.push({
          column: col.name,
          values: Array.from(uniqueValues),
        });
      }
    }
  }

  // Save chunk as JSON to blob storage
  const chunkBuffer = Buffer.from(JSON.stringify(chunk));
  await uploadFileToBlob(chunkBuffer, blobName, 'system', 'application/json');

  return {
    chunkId,
    blobName,
    rowCount: chunk.length,
    rowStart,
    rowEnd,
    dateRanges: dateRanges.length > 0 ? dateRanges : undefined,
    valueRanges: valueRanges.length > 0 ? valueRanges : undefined,
    categories: categories.length > 0 ? categories : undefined,
    columns: Object.keys(chunk[0] || {}),
  };
}

/**
 * Load chunk index from blob storage
 */
export async function loadChunkIndex(sessionId: string): Promise<ChunkIndex | null> {
  try {
    const indexBlobName = `chunks/${sessionId}/index.json`;
    const buffer = await getFileFromBlob(indexBlobName);
    const indexData = JSON.parse(buffer.toString('utf-8')) as ChunkIndex;
    return indexData;
  } catch (error) {
    console.error('Failed to load chunk index:', error);
    return null;
  }
}

/**
 * Find relevant chunks based on query filters
 */
export function findRelevantChunks(
  chunkIndex: ChunkIndex,
  filters: {
    timeFilters?: Array<{
      type: 'year' | 'month' | 'quarter' | 'dateRange' | 'relative';
      column?: string | null;
      years?: number[] | null;
      months?: string[] | null;
      startDate?: string | null;
      endDate?: string | null;
    }>;
    valueFilters?: Array<{
      column: string;
      operator: '>' | '>=' | '<' | '<=' | '=' | 'between' | '!=';
      value?: number | null;
      value2?: number | null;
    }>;
    exclusionFilters?: Array<{
      column: string;
      values: (string | number)[];
    }>;
  }
): ChunkMetadata[] {
  const relevantChunks: ChunkMetadata[] = [];

  for (const chunk of chunkIndex.chunks) {
    let isRelevant = true;

    // Check date filters
    if (filters.timeFilters && filters.timeFilters.length > 0) {
      for (const timeFilter of filters.timeFilters) {
        if (!chunk.dateRanges) {
          // No date metadata, include chunk to be safe
          continue;
        }

        const dateRange = chunk.dateRanges.find(dr => {
          if (timeFilter.column) {
            return dr.column === timeFilter.column;
          }
          // Use first date column if not specified
          return true;
        });

        if (!dateRange) {
          // No matching date column, include chunk
          continue;
        }

        // Check if chunk overlaps with filter
        if (timeFilter.type === 'year' && timeFilter.years) {
          const chunkMinYear = dateRange.min ? new Date(dateRange.min).getFullYear() : null;
          const chunkMaxYear = dateRange.max ? new Date(dateRange.max).getFullYear() : null;
          const hasOverlap = timeFilter.years.some(year => {
            if (chunkMinYear === null || chunkMaxYear === null) return true;
            return year >= chunkMinYear && year <= chunkMaxYear;
          });
          if (!hasOverlap) {
            isRelevant = false;
            break;
          }
        } else if (timeFilter.type === 'dateRange') {
          const filterStart = timeFilter.startDate ? new Date(timeFilter.startDate) : null;
          const filterEnd = timeFilter.endDate ? new Date(timeFilter.endDate) : null;
          const chunkStart = dateRange.min ? new Date(dateRange.min) : null;
          const chunkEnd = dateRange.max ? new Date(dateRange.max) : null;

          if (filterStart && chunkEnd && filterStart > chunkEnd) {
            isRelevant = false;
            break;
          }
          if (filterEnd && chunkStart && filterEnd < chunkStart) {
            isRelevant = false;
            break;
          }
        }
      }
    }

    // Check value filters
    if (isRelevant && filters.valueFilters && filters.valueFilters.length > 0) {
      for (const valueFilter of filters.valueFilters) {
        const valueRange = chunk.valueRanges?.find(vr => vr.column === valueFilter.column);
        if (!valueRange) {
          // No metadata for this column, include chunk to be safe
          continue;
        }

        // Check if chunk overlaps with filter
        if (valueFilter.operator === '>' || valueFilter.operator === '>=') {
          if (valueFilter.value !== null && valueRange.max !== null && valueFilter.value > valueRange.max) {
            isRelevant = false;
            break;
          }
        } else if (valueFilter.operator === '<' || valueFilter.operator === '<=') {
          if (valueFilter.value !== null && valueRange.min !== null && valueFilter.value < valueRange.min) {
            isRelevant = false;
            break;
          }
        } else if (valueFilter.operator === 'between') {
          if (
            valueFilter.value !== null &&
            valueFilter.value2 !== null &&
            valueRange.max !== null &&
            valueRange.min !== null
          ) {
            if (valueFilter.value > valueRange.max || valueFilter.value2 < valueRange.min) {
              isRelevant = false;
              break;
            }
          }
        }
      }
    }

    // Check exclusion filters
    if (isRelevant && filters.exclusionFilters && filters.exclusionFilters.length > 0) {
      for (const exclusionFilter of filters.exclusionFilters) {
        const category = chunk.categories?.find(c => c.column === exclusionFilter.column);
        if (category) {
          // If chunk contains any excluded values, exclude it
          const hasExcludedValue = exclusionFilter.values.some(val => category.values.includes(val));
          if (hasExcludedValue && category.values.length === exclusionFilter.values.length) {
            // All values in chunk are excluded
            isRelevant = false;
            break;
          }
        }
      }
    }

    if (isRelevant) {
      relevantChunks.push(chunk);
    }
  }

  // If no chunks match, return all chunks (fallback to full scan)
  return relevantChunks.length > 0 ? relevantChunks : chunkIndex.chunks;
}

/**
 * Load data from relevant chunks
 */
export async function loadChunkData(
  chunks: ChunkMetadata[],
  requiredColumns?: string[]
): Promise<Record<string, any>[]> {
  const allData: Record<string, any>[] = [];

  // Load chunks in parallel (limit concurrency)
  const CONCURRENCY_LIMIT = 5;
  for (let i = 0; i < chunks.length; i += CONCURRENCY_LIMIT) {
    const chunkBatch = chunks.slice(i, i + CONCURRENCY_LIMIT);
    const chunkDataPromises = chunkBatch.map(async (chunk) => {
      try {
        const buffer = await getFileFromBlob(chunk.blobName);
        const chunkData = JSON.parse(buffer.toString('utf-8')) as Record<string, any>[];

        // Filter columns if required
        if (requiredColumns && requiredColumns.length > 0) {
          return chunkData.map(row => {
            const filteredRow: Record<string, any> = {};
            requiredColumns.forEach(col => {
              if (row[col] !== undefined) {
                filteredRow[col] = row[col];
              }
            });
            return filteredRow;
          });
        }

        return chunkData;
      } catch (error) {
        console.error(`Failed to load chunk ${chunk.chunkId}:`, error);
        return [];
      }
    });

    const batchResults = await Promise.all(chunkDataPromises);
    allData.push(...batchResults.flat());
  }

  return allData;
}
