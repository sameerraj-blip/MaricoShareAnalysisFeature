/**
 * Metadata Service
 * Computes and caches dataset metadata (row count, column types, null %, cardinality)
 */

import { ColumnarStorageService, DatasetMetadata } from './columnarStorage.js';
import { DataSummary } from '../shared/schema.js';

export interface CachedMetadata {
  metadata: DatasetMetadata;
  summary: DataSummary;
  computedAt: number;
  sessionId: string;
}

export class MetadataService {
  private cache: Map<string, CachedMetadata> = new Map();
  private readonly CACHE_TTL = 24 * 60 * 60 * 1000; // 24 hours

  /**
   * Compute metadata for a dataset
   */
  async computeMetadata(
    storage: ColumnarStorageService,
    tableName: string = 'data'
  ): Promise<DatasetMetadata> {
    return await storage.computeMetadata(tableName);
  }

  /**
   * Convert DuckDB metadata to DataSummary format
   */
  convertToDataSummary(metadata: DatasetMetadata, sampleRows: Record<string, any>[]): DataSummary {
    const numericColumns: string[] = [];
    const dateColumns: string[] = [];

    const columns = metadata.columns.map((col) => {
      // Determine type based on DuckDB type and metadata
      let type = 'string';
      
      if (col.type && (
        col.type.includes('DOUBLE') ||
        col.type.includes('INTEGER') ||
        col.type.includes('BIGINT') ||
        col.type.includes('DECIMAL') ||
        col.type.includes('FLOAT')
      )) {
        type = 'number';
        numericColumns.push(col.name);
      } else if (col.type && col.type.includes('DATE')) {
        type = 'date';
        dateColumns.push(col.name);
      }

      // Get sample values from sampleRows
      const sampleValues = sampleRows
        .slice(0, 3)
        .map(row => row[col.name])
        .filter(v => v !== null && v !== undefined);

      return {
        name: col.name,
        type,
        sampleValues,
      };
    });

    return {
      rowCount: metadata.rowCount,
      columnCount: metadata.columnCount,
      columns,
      numericColumns,
      dateColumns,
    };
  }

  /**
   * Cache metadata for a session
   */
  cacheMetadata(sessionId: string, metadata: DatasetMetadata, summary: DataSummary): void {
    this.cache.set(sessionId, {
      metadata,
      summary,
      computedAt: Date.now(),
      sessionId,
    });
  }

  /**
   * Get cached metadata
   */
  getCachedMetadata(sessionId: string): CachedMetadata | null {
    const cached = this.cache.get(sessionId);
    if (!cached) {
      return null;
    }

    // Check if cache is expired
    if (Date.now() - cached.computedAt > this.CACHE_TTL) {
      this.cache.delete(sessionId);
      return null;
    }

    return cached;
  }

  /**
   * Invalidate cache for a session
   */
  invalidateCache(sessionId: string): void {
    this.cache.delete(sessionId);
  }

  /**
   * Clear all expired cache entries
   */
  cleanupExpiredCache(): void {
    const now = Date.now();
    for (const [sessionId, cached] of this.cache.entries()) {
      if (now - cached.computedAt > this.CACHE_TTL) {
        this.cache.delete(sessionId);
      }
    }
  }
}

// Singleton instance
export const metadataService = new MetadataService();

// Cleanup expired cache every hour
setInterval(() => {
  metadataService.cleanupExpiredCache();
}, 60 * 60 * 1000);

