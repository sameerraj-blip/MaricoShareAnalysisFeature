/**
 * Columnar Storage Service using DuckDB
 * Converts CSV data to columnar format for efficient querying
 * 
 * Note: DuckDB is an optional dependency. If not installed, large file processing
 * will fall back to traditional methods.
 */

import * as fs from 'fs/promises';
import * as path from 'path';
import * as os from 'os';

// Dynamic import for DuckDB to handle optional dependency
let duckdb: any;
let Database: any;
let duckdbAvailable: boolean | null = null; // null = not checked yet, true/false = checked

async function loadDuckDB() {
  // If already checked and available, return cached Database
  if (duckdbAvailable === true && Database) {
    return { Database };
  }
  
  // If already checked and not available, throw error
  if (duckdbAvailable === false) {
    throw new Error('DuckDB is not available. Large file processing requires DuckDB. Please install it with: npm install duckdb');
  }
  
  // Try to load DuckDB
  try {
    duckdb = await import('duckdb');
    Database = duckdb.default?.Database || duckdb.Database || (duckdb as any).Database;
    if (!Database) {
      throw new Error('DuckDB Database class not found');
    }
    duckdbAvailable = true;
    return { Database };
  } catch (error) {
    duckdbAvailable = false;
    throw new Error(`DuckDB is not available: ${error instanceof Error ? error.message : String(error)}. Large file processing will use fallback methods. To enable DuckDB, install it with: npm install duckdb`);
  }
}

export function isDuckDBAvailable(): boolean {
  return duckdbAvailable === true;
}

export interface ColumnarStorageOptions {
  sessionId: string;
  tempDir?: string;
}

export interface DatasetMetadata {
  rowCount: number;
  columnCount: number;
  columns: Array<{
    name: string;
    type: string;
    nullCount: number;
    nullPercentage: number;
    cardinality: number;
    min?: number | string;
    max?: number | string;
    mean?: number;
    stdDev?: number;
  }>;
}

export class ColumnarStorageService {
  private db: Database | null = null;
  private sessionId: string;
  private tempDir: string;
  private dbPath: string;

  constructor(options: ColumnarStorageOptions) {
    this.sessionId = options.sessionId;
    this.tempDir = options.tempDir || path.join(os.tmpdir(), 'marico-columnar');
    this.dbPath = path.join(this.tempDir, `${options.sessionId}.duckdb`);
  }

  /**
   * Initialize DuckDB database
   */
  async initialize(): Promise<void> {
    // Load DuckDB module
    const { Database: DB } = await loadDuckDB();
    Database = DB;
    
    // Ensure temp directory exists
    await fs.mkdir(this.tempDir, { recursive: true });
    
    return new Promise((resolve, reject) => {
      try {
        // DuckDB can be initialized with a file path or in-memory
        this.db = new Database(this.dbPath, (err: Error | null) => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
      } catch (error) {
        reject(error);
      }
    });
  }

  /**
   * Load CSV data into DuckDB table
   */
  async loadCsvFromBuffer(
    buffer: Buffer,
    tableName: string = 'data'
  ): Promise<void> {
    if (!this.db) {
      throw new Error('Database not initialized. Call initialize() first.');
    }

    // Write buffer to temporary CSV file
    const tempCsvPath = path.join(this.tempDir, `${this.sessionId}_temp.csv`);
    await fs.writeFile(tempCsvPath, buffer);

    return new Promise((resolve, reject) => {
      try {
        const conn = this.db!.connect();
        
        // Escape path for SQL (replace backslashes and escape single quotes)
        const escapedPath = tempCsvPath.replace(/\\/g, '/').replace(/'/g, "''");
        
        // Create table from CSV
        conn.run(
          `CREATE TABLE ${tableName} AS SELECT * FROM read_csv_auto('${escapedPath}')`,
          (err: Error | null) => {
            if (err) {
              reject(err);
            } else {
              // Clean up temp CSV file
              fs.unlink(tempCsvPath).catch(() => {
                // Ignore cleanup errors
              });
              resolve();
            }
          }
        );
      } catch (error) {
        reject(error);
      }
    });
  }

  /**
   * Load data from streaming parser
   */
  async loadFromStreaming(
    chunks: Array<Record<string, any>[]>,
    tableName: string = 'data'
  ): Promise<void> {
    if (!this.db) {
      throw new Error('Database not initialized. Call initialize() first.');
    }

    return new Promise((resolve, reject) => {
      try {
        const conn = this.db!.connect();
        
        // Create table structure from first chunk
        if (chunks.length === 0 || chunks[0].length === 0) {
          reject(new Error('No data to load'));
          return;
        }

        const firstRow = chunks[0][0];
        const columns = Object.keys(firstRow);
        // Escape column names and use VARCHAR for all initially (DuckDB will infer types)
        const columnDefs = columns.map(col => `"${col.replace(/"/g, '""')}" VARCHAR`).join(', ');
        
        // Create table
        conn.run(`CREATE TABLE ${tableName} (${columnDefs})`, (err: Error | null) => {
          if (err) {
            reject(err);
            return;
          }

          // Insert data in batches
          let processed = 0;
          const totalRows = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
          
          const insertBatch = (batchIndex: number) => {
            if (batchIndex >= chunks.length) {
              resolve();
              return;
            }

            const chunk = chunks[batchIndex];
            if (chunk.length === 0) {
              insertBatch(batchIndex + 1);
              return;
            }

            // Build INSERT statement with proper escaping
            const escapedColumns = columns.map(col => `"${col.replace(/"/g, '""')}"`).join(', ');
            const values = chunk.map(row => {
              const rowValues = columns.map(col => {
                const val = row[col];
                if (val === null || val === undefined) {
                  return 'NULL';
                }
                if (typeof val === 'string') {
                  // Escape single quotes and wrap in quotes
                  return `'${String(val).replace(/'/g, "''")}'`;
                }
                if (typeof val === 'number') {
                  return String(val);
                }
                if (val instanceof Date) {
                  return `'${val.toISOString()}'`;
                }
                return `'${String(val).replace(/'/g, "''")}'`;
              });
              return `(${rowValues.join(', ')})`;
            });

            // Insert in smaller batches to avoid SQL statement size limits
            const INSERT_BATCH_SIZE = 100;
            const insertSubBatches = [];
            for (let i = 0; i < values.length; i += INSERT_BATCH_SIZE) {
              const batch = values.slice(i, i + INSERT_BATCH_SIZE);
              insertSubBatches.push(`INSERT INTO ${tableName} (${escapedColumns}) VALUES ${batch.join(', ')}`);
            }
            
            let subBatchIndex = 0;
            const insertNextSubBatch = () => {
              if (subBatchIndex >= insertSubBatches.length) {
                processed += chunk.length;
                if (processed % 10000 === 0) {
                  console.log(`  Loaded ${processed} / ${totalRows} rows into DuckDB...`);
                }
                insertBatch(batchIndex + 1);
                return;
              }
              
              conn.run(insertSubBatches[subBatchIndex], (err: Error | null) => {
                if (err) {
                  reject(err);
                  return;
                }
                subBatchIndex++;
                insertNextSubBatch();
              });
            };
            
            insertNextSubBatch();
          };

          insertBatch(0);
        });
      } catch (error) {
        reject(error);
      }
    });
  }

  /**
   * Compute and cache dataset metadata
   */
  async computeMetadata(tableName: string = 'data'): Promise<DatasetMetadata> {
    if (!this.db) {
      throw new Error('Database not initialized. Call initialize() first.');
    }

    return new Promise((resolve, reject) => {
      const conn = this.db!.connect();
      
      // Get row count
      conn.all(`SELECT COUNT(*) as count FROM ${tableName}`, (err: Error | null, rows: any[]) => {
        if (err) {
          reject(err);
          return;
        }

        const rowCount = rows[0]?.count || 0;

        // Get column information
        conn.all(`DESCRIBE ${tableName}`, (err: Error | null, columns: any[]) => {
          if (err) {
            reject(err);
            return;
          }

          // Get detailed stats for each column
          const columnPromises = columns.map((col: any) => {
            return new Promise<DatasetMetadata['columns'][0]>((resolveCol, rejectCol) => {
              const colName = col.column_name;
              
              // Escape column name for SQL
              const escapedColName = `"${colName.replace(/"/g, '""')}"`;
              
              // Get null count and cardinality
              conn.all(
                `SELECT 
                  COUNT(*) as total,
                  COUNT(${escapedColName}) as non_null,
                  COUNT(DISTINCT ${escapedColName}) as distinct_count
                FROM ${tableName}`,
                (err: Error | null, stats: any[]) => {
                  if (err) {
                    rejectCol(err);
                    return;
                  }

                  const stat = stats[0];
                  const nullCount = stat.total - stat.non_null;
                  const nullPercentage = stat.total > 0 ? (nullCount / stat.total) * 100 : 0;
                  const cardinality = stat.distinct_count;

                  // Try to get numeric stats if column is numeric
                  conn.all(
                    `SELECT 
                      MIN(${escapedColName}) as min_val,
                      MAX(${escapedColName}) as max_val,
                      AVG(CAST(${escapedColName} AS DOUBLE)) as mean_val,
                      STDDEV(CAST(${escapedColName} AS DOUBLE)) as stddev_val
                    FROM ${tableName}
                    WHERE ${escapedColName} IS NOT NULL`,
                    (err: Error | null, numStats: any[]) => {
                      if (err || !numStats[0]) {
                        resolveCol({
                          name: colName,
                          type: col.column_type || 'VARCHAR',
                          nullCount,
                          nullPercentage,
                          cardinality,
                        });
                        return;
                      }

                      const numStat = numStats[0];
                      resolveCol({
                        name: colName,
                        type: col.column_type || 'VARCHAR',
                        nullCount,
                        nullPercentage,
                        cardinality,
                        min: numStat.min_val,
                        max: numStat.max_val,
                        mean: numStat.mean_val ? parseFloat(numStat.mean_val) : undefined,
                        stdDev: numStat.stddev_val ? parseFloat(numStat.stddev_val) : undefined,
                      });
                    }
                  );
                }
              );
            });
          });

          Promise.all(columnPromises)
            .then((columnMetadata) => {
              resolve({
                rowCount,
                columnCount: columns.length,
                columns: columnMetadata,
              });
            })
            .catch(reject);
        });
      });
    });
  }

  /**
   * Get sampled rows from table
   */
  async getSampleRows(
    limit: number = 50,
    tableName: string = 'data'
  ): Promise<Record<string, any>[]> {
    if (!this.db) {
      throw new Error('Database not initialized. Call initialize() first.');
    }

    return new Promise((resolve, reject) => {
      const conn = this.db!.connect();
      
      conn.all(
        `SELECT * FROM ${tableName} LIMIT ${limit}`,
        (err: Error | null, rows: any[]) => {
          if (err) {
            reject(err);
          } else {
            resolve(rows as Record<string, any>[]);
          }
        }
      );
    });
  }

  /**
   * Get all rows from table (no limit)
   * Use with caution for very large datasets - consider using streamQuery instead
   */
  async getAllRows(
    tableName: string = 'data',
    columns?: string[]
  ): Promise<Record<string, any>[]> {
    if (!this.db) {
      throw new Error('Database not initialized. Call initialize() first.');
    }

    const columnsStr = columns && columns.length > 0
      ? columns.map(col => `"${col.replace(/"/g, '""')}"`).join(', ')
      : '*';

    return new Promise((resolve, reject) => {
      const conn = this.db!.connect();
      
      conn.all(
        `SELECT ${columnsStr} FROM ${tableName}`,
        (err: Error | null, rows: any[]) => {
          if (err) {
            reject(err);
          } else {
            resolve(rows as Record<string, any>[]);
          }
        }
      );
    });
  }

  /**
   * Get row count for a table
   */
  async getRowCount(tableName: string = 'data'): Promise<number> {
    if (!this.db) {
      throw new Error('Database not initialized. Call initialize() first.');
    }

    return new Promise((resolve, reject) => {
      const conn = this.db!.connect();
      
      conn.all(
        `SELECT COUNT(*) as count FROM ${tableName}`,
        (err: Error | null, rows: any[]) => {
          if (err) {
            reject(err);
          } else {
            resolve(rows[0]?.count || 0);
          }
        }
      );
    });
  }

  /**
   * Stream query results in chunks for memory-efficient processing
   * Useful for very large datasets (300k+ rows)
   */
  async *streamQuery<T = any>(
    query: string,
    chunkSize: number = 50000,
    tableName: string = 'data'
  ): AsyncGenerator<T[], void, unknown> {
    if (!this.db) {
      throw new Error('Database not initialized. Call initialize() first.');
    }

    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      // Add LIMIT and OFFSET to the query
      const paginatedQuery = query.includes('LIMIT') 
        ? query.replace(/LIMIT\s+\d+/i, `LIMIT ${chunkSize}`)
        : `${query} LIMIT ${chunkSize} OFFSET ${offset}`;
      
      const chunk = await this.executeQuery<T>(paginatedQuery);
      
      if (chunk.length === 0) {
        hasMore = false;
      } else {
        yield chunk;
        offset += chunkSize;
        hasMore = chunk.length === chunkSize;
      }
    }
  }

  /**
   * Execute aggregation query
   */
  async executeQuery<T = any>(query: string): Promise<T[]> {
    if (!this.db) {
      throw new Error('Database not initialized. Call initialize() first.');
    }

    return new Promise((resolve, reject) => {
      const conn = this.db!.connect();
      
      conn.all(query, (err: Error | null, rows: any[]) => {
        if (err) {
          reject(err);
        } else {
          resolve(rows as T[]);
        }
      });
    });
  }

  /**
   * Get aggregated statistics for numeric columns
   */
  async getNumericStats(
    columns: string[],
    tableName: string = 'data'
  ): Promise<Record<string, { min: number; max: number; mean: number; stdDev: number }>> {
    if (columns.length === 0) {
      return {};
    }

    const stats: Record<string, any> = {};
    
    for (const col of columns) {
      try {
        // Escape column name for SQL
        const escapedCol = `"${col.replace(/"/g, '""')}"`;
        const result = await this.executeQuery<{
          min_val: number;
          max_val: number;
          mean_val: number;
          stddev_val: number;
        }>(
          `SELECT 
            MIN(CAST(${escapedCol} AS DOUBLE)) as min_val,
            MAX(CAST(${escapedCol} AS DOUBLE)) as max_val,
            AVG(CAST(${escapedCol} AS DOUBLE)) as mean_val,
            STDDEV(CAST(${escapedCol} AS DOUBLE)) as stddev_val
          FROM ${tableName}
          WHERE ${escapedCol} IS NOT NULL`
        );

        if (result[0]) {
          stats[col] = {
            min: result[0].min_val,
            max: result[0].max_val,
            mean: result[0].mean_val,
            stdDev: result[0].stddev_val,
          };
        }
      } catch (error) {
        // Column might not be numeric, skip it
        console.warn(`Could not compute stats for column ${col}:`, error);
      }
    }

    return stats;
  }

  /**
   * Close database connection and cleanup
   */
  async close(): Promise<void> {
    return new Promise((resolve) => {
      if (this.db) {
        this.db.close((err) => {
          if (err) {
            console.error('Error closing DuckDB:', err);
          }
          this.db = null;
          resolve();
        });
      } else {
        resolve();
      }
    });
  }

  /**
   * Clean up database file
   */
  async cleanup(): Promise<void> {
    await this.close();
    try {
      await fs.unlink(this.dbPath);
    } catch (error) {
      // File might not exist, ignore
    }
  }
}

