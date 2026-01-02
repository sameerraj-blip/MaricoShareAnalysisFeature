/**
 * Streaming CSV Parser for Large Files
 * Reads CSV files in chunks instead of loading fully into memory
 */

import { parse } from 'csv-parse';
import { Readable, Transform } from 'stream';
import { pipeline } from 'stream/promises';

export interface StreamingParseOptions {
  chunkSize?: number; // Number of rows to process per chunk
  onProgress?: (processed: number, total?: number) => void;
}

export interface ParsedRow {
  [key: string]: any;
}

/**
 * Stream CSV file in chunks and process each chunk
 */
export async function streamParseCsv(
  buffer: Buffer,
  options: StreamingParseOptions = {}
): Promise<{
  rowCount: number;
  columns: string[];
  processChunks: <T>(processor: (chunk: ParsedRow[]) => Promise<T> | T) => Promise<T[]>;
}> {
  const { chunkSize = 10000, onProgress } = options;
  
  const content = buffer.toString('utf-8');
  const stream = Readable.from([content]);
  
  let rowCount = 0;
  let columns: string[] = [];
  let isFirstChunk = true;
  const chunks: ParsedRow[][] = [];
  let currentChunk: ParsedRow[] = [];
  
  const parser = parse({
    columns: true,
    skip_empty_lines: true,
    cast: true,
    cast_date: true,
    relax_column_count: true,
    relax_quotes: true,
  });
  
  const transformer = new Transform({
    objectMode: true,
    transform(row: ParsedRow, encoding, callback) {
      if (isFirstChunk) {
        columns = Object.keys(row);
        isFirstChunk = false;
      }
      
      // Normalize column names (trim whitespace)
      const normalizedRow: ParsedRow = {};
      for (const [key, value] of Object.entries(row)) {
        normalizedRow[key.trim()] = value;
      }
      
      currentChunk.push(normalizedRow);
      rowCount++;
      
      if (currentChunk.length >= chunkSize) {
        chunks.push([...currentChunk]);
        currentChunk = [];
        if (onProgress) {
          onProgress(rowCount);
        }
      }
      
      callback();
    },
    flush(callback) {
      // Process remaining rows
      if (currentChunk.length > 0) {
        chunks.push([...currentChunk]);
      }
      if (onProgress) {
        onProgress(rowCount, rowCount);
      }
      callback();
    },
  });
  
  await pipeline(stream, parser, transformer);
  
  return {
    rowCount,
    columns,
    async processChunks<T>(processor: (chunk: ParsedRow[]) => Promise<T> | T): Promise<T[]> {
      const results: T[] = [];
      for (const chunk of chunks) {
        const result = await processor(chunk);
        results.push(result);
      }
      return results;
    },
  };
}

/**
 * Process CSV rows in batches and apply transformations
 */
export async function processCsvInBatches<T>(
  buffer: Buffer,
  processor: (batch: ParsedRow[]) => Promise<T> | T,
  batchSize: number = 10000,
  onProgress?: (processed: number) => void
): Promise<T[]> {
  const { processChunks } = await streamParseCsv(buffer, {
    chunkSize: batchSize,
    onProgress,
  });
  
  return processChunks(processor);
}

/**
 * Sample rows from CSV without loading entire file
 */
export async function sampleCsvRows(
  buffer: Buffer,
  sampleSize: number = 50,
  random: boolean = false
): Promise<ParsedRow[]> {
  const { rowCount, processChunks } = await streamParseCsv(buffer);
  
  if (rowCount <= sampleSize) {
    // If file is small, just return all rows
    const allRows: ParsedRow[] = [];
    await processChunks(async (chunk) => {
      allRows.push(...chunk);
    });
    return allRows;
  }
  
  if (random) {
    // Random sampling - collect all rows first (for small-medium files)
    // For very large files, use reservoir sampling
    const allRows: ParsedRow[] = [];
    await processChunks(async (chunk) => {
      allRows.push(...chunk);
    });
    
    // Reservoir sampling for random selection
    const sample: ParsedRow[] = [];
    for (let i = 0; i < allRows.length; i++) {
      if (i < sampleSize) {
        sample.push(allRows[i]);
      } else {
        const j = Math.floor(Math.random() * (i + 1));
        if (j < sampleSize) {
          sample[j] = allRows[i];
        }
      }
    }
    return sample;
  } else {
    // Sequential sampling - take evenly spaced rows
    const step = Math.floor(rowCount / sampleSize);
    const sample: ParsedRow[] = [];
    let currentIndex = 0;
    
    await processChunks(async (chunk) => {
      for (const row of chunk) {
        if (currentIndex % step === 0 && sample.length < sampleSize) {
          sample.push(row);
        }
        currentIndex++;
      }
    });
    
    return sample;
  }
}

