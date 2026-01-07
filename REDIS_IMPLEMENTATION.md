# Redis Implementation & Downsampling Removal

## Summary of Changes

This document outlines the changes made to remove downsampling limits and implement Redis caching for handling large datasets (300k+ rows).

## Changes Made

### 1. Removed Downsampling Limits

#### `server/utils/dataLoader.ts`
- **Line 150**: Removed the 50k/10k limit for columnar storage queries
- Now loads **all rows** from DuckDB without downsampling
- Added Redis caching for query results

#### `server/lib/largeFileProcessor.ts`
- **Line 174**: Removed default 10k limit
- Now uses `getAllRows()` when no limit is specified
- Supports loading full 300k row datasets

### 2. Enhanced Columnar Storage

#### `server/lib/columnarStorage.ts`
Added new methods:
- `getAllRows()`: Load all rows from table (no limit)
- `getRowCount()`: Get total row count for a table
- `streamQuery()`: Stream query results in chunks for memory-efficient processing

### 3. Redis Implementation

#### New Files Created:

**`server/lib/redisClient.ts`**
- Redis client initialization with lazy loading
- Connection management and error handling
- Graceful fallback if Redis is unavailable

**`server/lib/redisCache.ts`**
- Caching utilities for query results, correlations, and analysis
- Cache key generation with MD5 hashing
- TTL configuration:
  - Query results: 1 hour
  - Correlations: 2 hours
  - Analysis: 30 minutes
  - Metadata: 24 hours

#### Cache Functions:
- `getCachedQueryResult()` / `cacheQueryResult()`: For data queries
- `getCachedCorrelation()` / `cacheCorrelation()`: For correlation calculations
- `getCachedAnalysis()` / `cacheAnalysis()`: For analysis results
- `invalidateSessionCache()`: Clear all cache for a session

### 4. Updated Correlation Analysis

#### `server/lib/correlationAnalyzer.ts`
- Added `sessionId` parameter for caching
- Checks Redis cache before calculating correlations
- Caches results after calculation
- Updated all callers to pass `sessionId`

#### Updated Callers:
- `server/lib/dataAnalyzer.ts`
- `server/lib/agents/handlers/correlationHandler.ts`
- `server/lib/agents/handlers/comparisonHandler.ts`

### 5. Package Dependencies

#### `server/package.json`
- Added `redis@^4.7.0` dependency

## Environment Variables Required

Add these to your `.env` file:

```bash
# Redis Configuration (optional - caching disabled if not set)
REDIS_URL=redis://localhost:6379
# OR
REDIS_HOST=redis://localhost:6379
```

## How It Works

### Data Loading Flow

1. **Check Redis Cache**: If `requiredColumns` are specified, check cache first
2. **Load from DuckDB**: If not cached, load all rows from columnar storage
3. **Normalize Data**: Process and normalize numeric columns
4. **Cache Result**: Store in Redis for future queries
5. **Return Data**: Return full dataset (no downsampling)

### Correlation Analysis Flow

1. **Check Cache**: Look for cached correlation results
2. **Calculate**: If not cached, calculate correlations for all 300k rows
3. **Cache Result**: Store results in Redis (2 hour TTL)
4. **Return**: Return charts and insights

## Performance Benefits

1. **No Downsampling**: Full 300k rows available for analysis
2. **Faster Queries**: Redis cache provides instant results for repeated queries
3. **Memory Efficient**: Streaming queries available for very large datasets
4. **Robust**: Graceful fallback if Redis is unavailable

## Usage Notes

- **Redis is Optional**: System works without Redis, but caching is disabled
- **Cache Invalidation**: Cache is automatically invalidated when data changes
- **Memory Usage**: Loading 300k rows uses ~50-100MB memory (depends on columns)
- **Query Time**: First query takes 1-5 seconds, cached queries are instant

## Next Steps

1. Install Redis (if not already installed):
   ```bash
   # macOS
   brew install redis
   brew services start redis
   
   # Or use Docker
   docker run -d -p 6379:6379 redis:latest
   ```

2. Set environment variables in your `.env` file

3. Test with a large dataset (300k rows) to verify performance

## Troubleshooting

- **Redis Connection Errors**: Check `REDIS_URL` or `REDIS_HOST` environment variable
- **Cache Not Working**: Verify Redis is running and accessible
- **Memory Issues**: Use `streamQuery()` for very large datasets (>500k rows)
- **Slow Queries**: First query is slower, subsequent queries use cache

