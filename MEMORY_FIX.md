# Memory Optimization Fix

## Problem
JavaScript heap out of memory error when processing large datasets or chart data. The error occurred during `Array.map()` operations on large chart data arrays.

## Root Cause
1. **Large Chart Data Arrays**: Charts with 100,000+ data points were being processed in memory
2. **Parallel Processing**: Multiple charts processed simultaneously with `Promise.all()` causing memory spikes
3. **No Data Limits**: No limits on chart data size before processing
4. **Default Heap Limit**: Node.js default heap limit (~2GB) was insufficient

## Solutions Implemented

### 1. Chart Data Size Limits
- **Location**: `server/utils/uploadQueue.ts` and `server/services/chat/chatResponse.service.ts`
- **Change**: Limited chart data to maximum 50,000 data points
- **Method**: 
  - For line/area charts: Even sampling to preserve visual quality
  - For other charts: Take first N data points
- **Impact**: Prevents processing of extremely large datasets

### 2. Batch Processing for Chart Sanitization
- **Location**: `server/utils/uploadQueue.ts`
- **Change**: Process chart data sanitization in batches of 10,000 rows
- **Impact**: Reduces memory spikes during data transformation

### 3. Sequential Chart Processing
- **Location**: `server/services/chat/chatResponse.service.ts`
- **Change**: Changed from `Promise.all()` (parallel) to sequential processing
- **Impact**: Prevents memory spikes from processing multiple large charts simultaneously

### 4. Increased Node.js Heap Size
- **Location**: `server/package.json`
- **Change**: Added `NODE_OPTIONS='--max-old-space-size=4096'` to dev and start scripts
- **Impact**: Increases available memory from ~2GB to 4GB as a safety buffer

## Code Changes

### uploadQueue.ts
```typescript
// Before: Processed all chart data at once
const sanitizedData = (chart.data || []).map(row => { ... });

// After: Limited size + batch processing
if (chartData.length > MAX_CHART_DATA_POINTS) {
  // Sample or limit data
}
// Process in batches of 10,000
```

### chatResponse.service.ts
```typescript
// Before: Parallel processing
return await Promise.all(charts.map(async (c) => { ... }));

// After: Sequential processing with limits
for (const c of charts) {
  if (dataForChart.length > MAX_CHART_DATA_POINTS) {
    // Limit data
  }
  // Process one at a time
}
```

## Testing
1. Test with large datasets (100K+ rows)
2. Test with multiple charts in response
3. Monitor memory usage during processing
4. Verify charts still display correctly with sampled data

## Future Improvements
1. **Streaming Chart Generation**: Generate charts in streaming fashion
2. **Lazy Loading**: Load chart data on-demand instead of all at once
3. **Data Compression**: Compress chart data before storage
4. **Pagination**: Implement pagination for very large chart datasets
5. **Worker Threads**: Move heavy processing to worker threads

## Monitoring
Watch for:
- Memory usage patterns
- Processing time for large datasets
- Chart rendering quality with sampled data
- User feedback on chart accuracy

