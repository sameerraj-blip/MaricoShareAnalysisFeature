/**
 * Large File Processor
 * Handles processing of large files (50MB+) using streaming and columnar storage
 */

import { ColumnarStorageService, DatasetMetadata, isDuckDBAvailable } from './columnarStorage.js';
import { streamParseCsv, processCsvInBatches, sampleCsvRows } from './streamingFileParser.js';
import { metadataService } from './metadataService.js';
import { DataSummary } from '../shared/schema.js';
import { convertDashToZeroForNumericColumns } from './fileParser.js';

export interface LargeFileProcessResult {
  rowCount: number;
  columns: string[];
  metadata: DatasetMetadata;
  summary: DataSummary;
  sampleRows: Record<string, any>[];
  storagePath: string;
}

export interface ProcessingProgress {
  stage: 'parsing' | 'loading' | 'computing' | 'complete';
  progress: number; // 0-100
  message?: string;
}

/**
 * Process large CSV file using streaming and columnar storage
 */
export async function processLargeFile(
  buffer: Buffer,
  sessionId: string,
  fileName: string,
  onProgress?: (progress: ProcessingProgress) => void
): Promise<LargeFileProcessResult> {
  const storage = new ColumnarStorageService({ sessionId });
  
  try {
    // Initialize DuckDB
    onProgress?.({ stage: 'parsing', progress: 5, message: 'Initializing columnar storage...' });
    await storage.initialize();

    // Step 1: Stream parse CSV and collect chunks
    onProgress?.({ stage: 'parsing', progress: 10, message: 'Parsing CSV file in chunks...' });
    const { rowCount, columns, processChunks } = await streamParseCsv(buffer, {
      chunkSize: 10000,
      onProgress: (processed) => {
        onProgress?.({
          stage: 'parsing',
          progress: 10 + Math.floor((processed / rowCount) * 30),
          message: `Parsed ${processed} rows...`,
        });
      },
    });

    // Step 2: Load data into DuckDB in batches
    onProgress?.({ stage: 'loading', progress: 40, message: 'Loading data into columnar storage...' });
    const chunks: Record<string, any>[][] = [];
    
    await processChunks(async (chunk) => {
      // Process chunk: normalize values
      const processedChunk = chunk.map(row => {
        const processedRow: Record<string, any> = {};
        for (const [key, value] of Object.entries(row)) {
          if (value === null || value === undefined) {
            processedRow[key] = null;
          } else if (typeof value === 'string') {
            const trimmed = value.trim();
            if (trimmed === '') {
              processedRow[key] = null;
            } else {
              // Try to convert string numbers
              const cleaned = trimmed.replace(/[%,$€£¥₹\s]/g, '').trim();
              const num = Number(cleaned);
              if (cleaned !== '' && !isNaN(num) && isFinite(num)) {
                processedRow[key] = num;
              } else {
                processedRow[key] = trimmed;
              }
            }
          } else {
            processedRow[key] = value;
          }
        }
        return processedRow;
      });
      
      chunks.push(processedChunk);
      
      const loaded = chunks.reduce((sum, c) => sum + c.length, 0);
      onProgress?.({
        stage: 'loading',
        progress: 40 + Math.floor((loaded / rowCount) * 30),
        message: `Loaded ${loaded} / ${rowCount} rows into columnar storage...`,
      });
    });

    // Load into DuckDB
    await storage.loadFromStreaming(chunks);

    // Step 3: Compute metadata
    onProgress?.({ stage: 'computing', progress: 70, message: 'Computing dataset metadata...' });
    const metadata = await storage.computeMetadata();
    
    onProgress?.({ stage: 'computing', progress: 85, message: 'Generating data summary...' });
    
    // Step 4: Get sample rows
    const sampleRows = await storage.getSampleRows(50);
    
    // Step 5: Convert to DataSummary format
    let summary = metadataService.convertToDataSummary(metadata, sampleRows);
    
    // Apply dash-to-zero conversion for numeric columns
    const sampleRowsProcessed = convertDashToZeroForNumericColumns(sampleRows, summary.numericColumns);
    summary = metadataService.convertToDataSummary(metadata, sampleRowsProcessed);
    
    // Cache metadata
    metadataService.cacheMetadata(sessionId, metadata, summary);

    onProgress?.({ stage: 'complete', progress: 100, message: 'Processing complete!' });

    return {
      rowCount,
      columns,
      metadata,
      summary,
      sampleRows: sampleRowsProcessed,
      storagePath: storage['dbPath'], // Access private property
    };
  } catch (error) {
    // Cleanup on error
    await storage.cleanup().catch(() => {
      // Ignore cleanup errors
    });
    throw error;
  }
  // Note: We don't close storage here - it will be used for queries later
  // Storage should be closed when session is deleted or after a timeout
}

/**
 * Check if file should use large file processing
 */
export function shouldUseLargeFileProcessing(fileSize: number): boolean {
  // Use large file processing for files >= 50MB, but only if DuckDB is available
  if (!isDuckDBAvailable()) {
    console.log('⚠️ DuckDB not available - large file processing disabled. Using traditional processing.');
    return false;
  }
  return fileSize >= 50 * 1024 * 1024;
}

/**
 * Get data from columnar storage for analysis
 * Returns sampled or aggregated data instead of full dataset
 */
export async function getDataForAnalysis(
  sessionId: string,
  requiredColumns?: string[],
  limit?: number
): Promise<Record<string, any>[]> {
  const storage = new ColumnarStorageService({ sessionId });
  await storage.initialize();

  try {
    if (requiredColumns && requiredColumns.length > 0) {
      // Query only required columns
      const columnsStr = requiredColumns.map(col => `"${col}"`).join(', ');
      const limitClause = limit ? `LIMIT ${limit}` : '';
      const query = `SELECT ${columnsStr} FROM data ${limitClause}`;
      return await storage.executeQuery(query);
    } else {
      // Get sample rows
      return await storage.getSampleRows(limit || 10000);
    }
  } finally {
    await storage.close();
  }
}

