# Large File CSV Ingestion Pipeline Refactor

## Overview

This refactor implements a streaming, columnar-based CSV ingestion pipeline to support large files (50MB+). The system now processes files in chunks, stores data in DuckDB (columnar format), and exposes APIs for aggregated/sampled data retrieval.

## Key Components

### 1. Streaming CSV Parser (`server/lib/streamingFileParser.ts`)
- Reads CSV files in chunks instead of loading fully into memory
- Processes data in batches (default: 10,000 rows per chunk)
- Supports progress callbacks for monitoring
- Provides sampling functions for random or sequential sampling

### 2. Columnar Storage Service (`server/lib/columnarStorage.ts`)
- Uses DuckDB for efficient columnar storage
- Converts CSV data to columnar format automatically
- Provides query interface for aggregated data
- Computes metadata (row count, column types, null %, cardinality)

### 3. Metadata Service (`server/lib/metadataService.ts`)
- Computes and caches dataset metadata
- Converts DuckDB metadata to DataSummary format
- 24-hour cache TTL for performance
- Automatic cleanup of expired cache entries

### 4. Large File Processor (`server/lib/largeFileProcessor.ts`)
- Orchestrates the entire large file processing pipeline
- Automatically detects files >= 50MB
- Uses streaming parser + columnar storage for large files
- Falls back to traditional processing for smaller files

### 5. Data API Routes (`server/routes/dataApi.ts`)
- `GET /api/data/:sessionId/sample` - Get sampled rows
- `GET /api/data/:sessionId/metadata` - Get dataset metadata
- `POST /api/data/:sessionId/query` - Execute aggregation queries
- `GET /api/data/:sessionId/stats` - Get numeric column statistics

## How It Works

### For Large Files (>= 50MB):

1. **Upload**: File is received and queued for processing
2. **Streaming Parse**: CSV is parsed in chunks (10k rows at a time)
3. **Columnar Storage**: Data is loaded into DuckDB in batches
4. **Metadata Computation**: Row count, column types, null %, cardinality are computed
5. **Sampling**: Representative sample (up to 50k rows) is extracted for AI analysis
6. **Storage**: Only sample rows stored in CosmosDB; full data in DuckDB

### For Small Files (< 50MB):

- Uses traditional in-memory processing (backward compatible)
- Full data stored in CosmosDB as before

## API Usage

### Get Sample Rows
```bash
GET /api/data/:sessionId/sample?limit=50&random=false
```

### Get Metadata
```bash
GET /api/data/:sessionId/metadata
```

### Execute Query
```bash
POST /api/data/:sessionId/query
Body: { "query": "SELECT COUNT(*) as count FROM data WHERE column = 'value'" }
```

### Get Numeric Stats
```bash
GET /api/data/:sessionId/stats?columns=col1,col2,col3
```

## Benefits

1. **Memory Efficient**: Only processes chunks at a time, never loads full file
2. **Fast Queries**: Columnar format enables fast aggregations
3. **Scalable**: Can handle files of any size (limited by disk space)
4. **Non-blocking**: All operations are asynchronous
5. **Cached Metadata**: Metadata is cached for 24 hours for quick access

## Installation

Add DuckDB to dependencies:
```bash
npm install duckdb
```

## Configuration

- Large file threshold: 50MB (configurable in `largeFileProcessor.ts`)
- Chunk size: 10,000 rows (configurable in `streamingFileParser.ts`)
- Cache TTL: 24 hours (configurable in `metadataService.ts`)
- DuckDB storage: `/tmp/marico-columnar/` (configurable in `columnarStorage.ts`)

## Notes

- DuckDB files are stored in temp directory and should be cleaned up periodically
- For production, consider using a persistent storage location for DuckDB files
- The system automatically falls back to traditional processing for smaller files
- All heavy operations are non-blocking and run in the upload queue

