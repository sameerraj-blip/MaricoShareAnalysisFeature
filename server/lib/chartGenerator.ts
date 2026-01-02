import { ChartSpec } from '../shared/schema.js';
import { findMatchingColumn } from './agents/utils/columnMatcher.js';
import { normalizeDateToPeriod, parseFlexibleDate, DatePeriod, isDateColumnName } from './dateUtils.js';
import { optimizeChartData, downsampleChartData } from './chartDownsampling.js';

// Maximum data points for visualization to ensure good performance
// Updated to 5000 as per requirements - all downsampling now handled by chartDownsampling.ts
const MAX_POINTS_LINE_CHART = 5000;  // For line/area charts
const MAX_POINTS_SCATTER = 5000;     // For scatter plots
const MAX_POINTS_CORRELATION = 5000; // For correlation charts

// Helper to clean numeric values (strip %, commas, etc.)
function toNumber(value: any): number {
  if (value === null || value === undefined || value === '') return NaN;
  const cleaned = String(value).replace(/[%,]/g, '').trim();
  return Number(cleaned);
}

// Helper function to convert Date objects to strings for schema validation
function convertValueForSchema(value: any): string | number | null {
  if (value === null || value === undefined) return null;
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'number') return isNaN(value) || !isFinite(value) ? null : value;
  if (typeof value === 'string') return value;
  // For other types, convert to string
  return String(value);
}

/**
 * Largest-Triangle-Three-Buckets (LTTB) downsampling algorithm
 * Preserves visual shape better than simple decimation for line charts
 * Based on: https://github.com/sveinn-steinarsson/flot-downsample
 */
function downsampleLTTB(
  data: Record<string, any>[],
  xKey: string,
  yKey: string,
  threshold: number
): Record<string, any>[] {
  if (data.length <= threshold) {
    return data;
  }

  const dataLength = data.length;
  if (threshold >= dataLength || threshold === 0) {
    return data;
  }

  const sampled: Record<string, any>[] = [];
  const every = (dataLength - 2) / (threshold - 2);
  let a = 0;
  let nextA = 0;
  let maxAreaPoint: Record<string, any>;
  let maxArea: number;
  let area: number;
  let rangeA: number;
  let rangeB: number;

  sampled.push(data[a]); // Always add the first point

  for (let i = 0; i < threshold - 2; i++) {
    // Calculate point range for this bucket
    rangeA = Math.floor((i + 1) * every) + 1;
    rangeB = Math.floor((i + 2) * every) + 1;
    if (rangeB > dataLength) {
      rangeB = dataLength;
    }

    // Calculate point range average point
    const avgX = (data[rangeA][xKey] + data[rangeB][xKey]) / 2;
    const avgY = (data[rangeA][yKey] + data[rangeB][yKey]) / 2;

    // Get the range for this bucket
    const rangeOffs = Math.floor((i + 0) * every) + 1;
    const rangeTo = Math.floor((i + 1) * every) + 1;

    // Point a
    const pointAX = data[a][xKey];
    const pointAY = data[a][yKey];

    maxArea = -1;
    maxAreaPoint = data[rangeOffs];

    for (let j = rangeOffs; j < rangeTo && j < dataLength; j++) {
      // Calculate triangle area over three buckets
      area = Math.abs(
        (pointAX - avgX) * (data[j][yKey] - pointAY) -
        (pointAX - data[j][xKey]) * (avgY - pointAY)
      ) * 0.5;
      if (area > maxArea) {
        maxArea = area;
        maxAreaPoint = data[j];
        nextA = j; // Next a is this b
      }
    }

    sampled.push(maxAreaPoint);
    a = nextA; // This a is the next a (chosen b)
  }

  sampled.push(data[dataLength - 1]); // Always add last point

  return sampled;
}

/**
 * Smart downsampling for scatter plots
 * Uses stratified sampling to ensure good coverage across the data range
 */
function downsampleScatter(
  data: Record<string, any>[],
  xKey: string,
  yKey: string,
  threshold: number
): Record<string, any>[] {
  if (data.length <= threshold) {
    return data;
  }

  // Sort data by x values for better sampling
  const sorted = [...data].sort((a, b) => {
    const aVal = toNumber(a[xKey]);
    const bVal = toNumber(b[xKey]);
    if (isNaN(aVal) || isNaN(bVal)) return 0;
    return aVal - bVal;
  });

  // Use stratified sampling: divide into buckets and sample evenly from each
  const bucketSize = Math.ceil(sorted.length / threshold);
  const sampled: Record<string, any>[] = [];

  for (let i = 0; i < sorted.length; i += bucketSize) {
    const bucket = sorted.slice(i, Math.min(i + bucketSize, sorted.length));
    // Take the middle point from each bucket (or first if bucket is small)
    const index = Math.floor(bucket.length / 2);
    sampled.push(bucket[index]);
  }

  // Ensure we don't exceed threshold
  if (sampled.length > threshold) {
    const step = Math.floor(sampled.length / threshold);
    return sampled.filter((_, idx) => idx % step === 0).slice(0, threshold);
  }

  return sampled;
}

/**
 * Simple decimation downsampling (fallback for non-numeric x-axis)
 */
function downsampleSimple(
  data: Record<string, any>[],
  threshold: number
): Record<string, any>[] {
  if (data.length <= threshold) {
    return data;
  }

  const step = Math.floor(data.length / threshold);
  return data.filter((_, idx) => idx % step === 0).slice(0, threshold);
}

// Helper to parse date strings - use the flexible date parser
function parseDate(dateStr: string): Date | null {
  return parseFlexibleDate(dateStr);
}

// Helper to compare values for sorting - handles dates properly
function compareValues(a: any, b: any): number {
  const aStr = String(a);
  const bStr = String(b);
  
  // Try to parse as dates
  const aDate = parseDate(aStr);
  const bDate = parseDate(bStr);
  
  if (aDate && bDate) {
    // Both are dates, compare chronologically
    return aDate.getTime() - bDate.getTime();
  }
  
  // Fall back to string comparison
  return aStr.localeCompare(bStr);
}

/**
 * Process data in batches synchronously for large datasets
 * Used for aggregations that need to be merged
 */
function processChartDataSyncStreaming(
  data: Record<string, any>[],
  chartSpec: ChartSpec
): Record<string, any>[] {
  const batchSize = 10000;
  const { x, y, aggregate = 'none' } = chartSpec;
  const availableColumns = Object.keys(data[0] || {});
  const matchedX = findMatchingColumn(x, availableColumns) || x;
  const matchedY = findMatchingColumn(y, availableColumns) || y;
  
  console.log(`📊 Processing ${data.length} rows in batches of ${batchSize} for aggregation`);
  
  const batchResults: Record<string, any>[][] = [];
  for (let i = 0; i < data.length; i += batchSize) {
    const batch = data.slice(i, i + batchSize);
    const batchResult = processChartData(batch, { ...chartSpec, aggregate: 'none' }); // Process without aggregation first
    // Then aggregate this batch
    const aggregated = aggregateData(batchResult, matchedX, matchedY, aggregate);
    batchResults.push(aggregated);
  }
  
  // Merge aggregated results
  return mergeAggregatedResults(batchResults, matchedX, matchedY, aggregate);
}

/**
 * Process data in batches for large datasets
 * This reduces memory usage and improves performance for datasets >10k rows
 */
async function processInBatches<T>(
  data: Record<string, any>[],
  batchSize: number,
  processor: (batch: Record<string, any>[]) => T
): Promise<T[]> {
  const batches: T[] = [];
  for (let i = 0; i < data.length; i += batchSize) {
    const batch = data.slice(i, i + batchSize);
    batches.push(processor(batch));
  }
  return batches;
}

/**
 * Merge aggregated results from multiple batches
 * Used when processing large datasets in batches
 */
function mergeAggregatedResults(
  batchResults: Record<string, any>[][],
  groupBy: string,
  valueColumn: string,
  aggregateType: string
): Record<string, any>[] {
  if (batchResults.length === 0) return [];
  if (batchResults.length === 1) return batchResults[0];
  
  const merged = new Map<string, { values: number[]; displayLabel?: string }>();
  
  for (const batchResult of batchResults) {
    for (const row of batchResult) {
      const key = String(row[groupBy]);
      const value = toNumber(row[valueColumn]);
      
      if (!isNaN(value)) {
        if (!merged.has(key)) {
          merged.set(key, { values: [], displayLabel: row[groupBy] });
        }
        merged.get(key)!.values.push(value);
      }
    }
  }
  
  const result: Record<string, any>[] = [];
  for (const [key, { values, displayLabel }] of Array.from(merged.entries())) {
    let aggregatedValue: number;
    switch (aggregateType) {
      case 'sum':
        aggregatedValue = values.reduce((a, b) => a + b, 0);
        break;
      case 'mean':
        aggregatedValue = values.reduce((a, b) => a + b, 0) / values.length;
        break;
      case 'count':
        aggregatedValue = values.length;
        break;
      default:
        aggregatedValue = values[0];
    }
    result.push({
      [groupBy]: displayLabel || key,
      [valueColumn]: aggregatedValue,
    });
  }
  
  return result;
}

/**
 * Process chart data with streaming support for large datasets
 */
export async function processChartDataStreaming(
  data: Record<string, any>[],
  chartSpec: ChartSpec,
  batchSize: number = 10000
): Promise<Record<string, any>[]> {
  const { type, aggregate = 'none' } = chartSpec;
  
  // For large datasets with aggregation, use batch processing
  if (data.length > batchSize && aggregate !== 'none') {
    console.log(`📊 Processing ${data.length} rows in batches of ${batchSize} for aggregation`);
    
    const batchResults = await processInBatches(
      data,
      batchSize,
      (batch) => {
        // Process each batch as if it were the full dataset
        // We'll merge the results afterward
        const tempSpec = { ...chartSpec };
        return processChartData(batch, tempSpec);
      }
    );
    
    // Merge aggregated results
    const { x, y } = chartSpec;
    const availableColumns = Object.keys(data[0] || {});
    const matchedX = findMatchingColumn(x, availableColumns) || x;
    const matchedY = findMatchingColumn(y, availableColumns) || y;
    
    return mergeAggregatedResults(batchResults, matchedX, matchedY, aggregate);
  }
  
  // For non-aggregated or small datasets, use regular processing
  return processChartData(data, chartSpec);
}

export function processChartData(
  data: Record<string, any>[],
  chartSpec: ChartSpec
): Record<string, any>[] {
  const { type, x, y, y2, aggregate = 'none' } = chartSpec;
  
  console.log(`🔍 Processing chart: "${chartSpec.title}"`);
  console.log(`   Type: ${type}, X: "${x}", Y: "${y}", Aggregate: ${aggregate}`);
  
  // Check if data is empty
  if (!data || data.length === 0) {
    console.warn(`❌ No data provided for chart: ${chartSpec.title}`);
    return [];
  }
  
  console.log(`   Data rows available: ${data.length}`);
  
  // For large datasets without aggregation, use streaming for line/area charts
  if (data.length > 10000 && (type === 'line' || type === 'area') && aggregate === 'none') {
    console.log(`📊 Large dataset detected (${data.length} rows), processing in batches`);
    // Process synchronously in batches for line/area charts
    const batchSize = 10000;
    const result: Record<string, any>[] = [];
    for (let i = 0; i < data.length; i += batchSize) {
      const batch = data.slice(i, i + batchSize);
      const batchResult = processChartData(batch, { ...chartSpec });
      result.push(...batchResult);
    }
    // Sort the merged result
    const availableColumns = Object.keys(data[0] || {});
    const matchedX = findMatchingColumn(x, availableColumns) || x;
    return result.sort((a, b) => compareValues(a[matchedX], b[matchedX]));
  }
  
  // Check if columns exist in data
  const firstRow = data[0];
  if (!firstRow) {
    console.warn(`❌ No rows in data for chart: ${chartSpec.title}`);
    return [];
  }
  
  const availableColumns = Object.keys(firstRow);
  console.log(`   Available columns: [${availableColumns.join(', ')}]`);
  
  // Use flexible column matching instead of exact hasOwnProperty checks
  // This handles whitespace differences, case variations, and other imperfections
  const matchedX = findMatchingColumn(x, availableColumns);
  const matchedY = findMatchingColumn(y, availableColumns);
  const matchedY2 = y2 ? findMatchingColumn(y2, availableColumns) : null;
  
  if (!matchedX) {
    console.warn(`❌ Column "${x}" not found in data for chart: ${chartSpec.title}`);
    console.log(`   Available columns: [${availableColumns.join(', ')}]`);
    return [];
  }
  
  if (!matchedY) {
    console.warn(`❌ Column "${y}" not found in data for chart: ${chartSpec.title}`);
    console.log(`   Available columns: [${availableColumns.join(', ')}]`);
    return [];
  }
  
  // Optional secondary series existence check (for dual-axis line charts)
  if (y2 && !matchedY2) {
    console.warn(`❌ Column "${y2}" not found in data for secondary series of chart: ${chartSpec.title}`);
    console.log(`   Available columns: [${availableColumns.join(', ')}]`);
    console.log(`   Attempting fuzzy matching for y2 column...`);
    // Try more aggressive fuzzy matching
    const y2Lower = y2.toLowerCase().trim();
    const fuzzyMatch = availableColumns.find(col => {
      const colLower = col.toLowerCase().trim();
      return colLower.includes(y2Lower) || y2Lower.includes(colLower) || 
             colLower.replace(/\s+/g, '').includes(y2Lower.replace(/\s+/g, '')) ||
             y2Lower.replace(/\s+/g, '').includes(colLower.replace(/\s+/g, ''));
    });
    if (fuzzyMatch) {
      console.log(`   ✅ Found fuzzy match for y2: "${y2}" -> "${fuzzyMatch}"`);
      matchedY2 = fuzzyMatch;
    } else {
      console.error(`   ❌ No match found for y2 column "${y2}". Chart will only show primary Y series.`);
    }
  }
  
  // Update chart spec with matched column names to ensure consistency
  chartSpec.x = matchedX;
  chartSpec.y = matchedY;
  if (y2 && matchedY2) {
    chartSpec.y2 = matchedY2;
  } else if (y2 && !matchedY2) {
    // Remove y2 from spec if column not found
    console.warn(`   ⚠️ Removing y2 from chart spec because column "${y2}" was not found`);
    delete (chartSpec as any).y2;
    delete (chartSpec as any).y2Label;
  }
  
  // Use matched column names for data access
  const xCol = matchedX;
  const yCol = matchedY;
  const y2Col = matchedY2;
  
  // Check for valid data in the columns (using matched column names)
  // For bar charts, we'll validate after aggregation since we filter non-numeric values during aggregation
  const shouldValidateAfterAggregation = type === 'bar' && aggregate && aggregate !== 'none';
  
  if (!shouldValidateAfterAggregation) {
    const xValues = data.map(row => row[xCol]).filter(v => v !== null && v !== undefined && v !== '');
    const yValues = data.map(row => row[yCol]).filter(v => v !== null && v !== undefined && v !== '');
    const y2Values = y2Col ? data.map(row => row[y2Col]).filter(v => v !== null && v !== undefined && v !== '') : [];
    
    console.log(`   X column "${xCol}" (matched from "${x}"): ${xValues.length} valid values (sample: ${xValues.slice(0, 3).join(', ')})`);
    console.log(`   Y column "${yCol}" (matched from "${y}"): ${yValues.length} valid values (sample: ${yValues.slice(0, 3).join(', ')})`);
    if (y2Col) {
      console.log(`   Y2 column "${y2Col}" (matched from "${y2}"): ${y2Values.length} valid values (sample: ${y2Values.slice(0, 3).join(', ')})`);
    } else if (y2) {
      console.warn(`   ⚠️ Y2 column "${y2}" was requested but not found. Chart will only show primary Y series.`);
    }
    
    if (xValues.length === 0) {
      console.warn(`❌ No valid X values in column "${xCol}" for chart: ${chartSpec.title}`);
      return [];
    }
    
    if (yValues.length === 0) {
      console.warn(`❌ No valid Y values in column "${yCol}" for chart: ${chartSpec.title}`);
      return [];
    }
  } else {
    // For bar charts with aggregation, just check that columns exist
    const hasXColumn = data.length > 0 && data[0].hasOwnProperty(xCol);
    const hasYColumn = data.length > 0 && data[0].hasOwnProperty(yCol);
    
    if (!hasXColumn) {
      console.warn(`❌ Column "${xCol}" not found in data for chart: ${chartSpec.title}`);
      return [];
    }
    
    if (!hasYColumn) {
      console.warn(`❌ Column "${yCol}" not found in data for chart: ${chartSpec.title}`);
      return [];
    }
    
    console.log(`   Bar chart with aggregation - will validate after aggregation`);
  }

  if (type === 'scatter') {
    // For scatter plots, filter numeric values and sample if needed
    let scatterData = data
      .map((row) => {
        const xValue = toNumber(row[xCol]);
        const yValue = toNumber(row[yCol]);
        const mappedRow: Record<string, any> = {
          [xCol]: isNaN(xValue) ? null : xValue,
          [yCol]: isNaN(yValue) ? null : yValue,
        };
        // Convert any Date objects in other columns to strings (in case they're included)
        for (const [key, value] of Object.entries(row)) {
          if (key !== xCol && key !== yCol && value instanceof Date) {
            mappedRow[key] = convertValueForSchema(value);
          }
        }
        return mappedRow;
      })
      .filter((row) => !isNaN(row[xCol]) && !isNaN(row[yCol]));

    console.log(`   Scatter plot: ${scatterData.length} valid numeric points`);

    // Apply optimization to ensure max points limit
    const optimized = optimizeChartData(scatterData, chartSpec);
    if (optimized.length < scatterData.length) {
      console.log(`   ✅ Optimized scatter plot from ${scatterData.length} to ${optimized.length} points`);
    }
    return optimized;
  }

  if (type === 'pie') {
    // Check if data is already aggregated (if number of unique x values equals number of rows)
    const uniqueXValues = new Set(data.map(row => String(row[xCol])));
    const isAlreadyAggregated = uniqueXValues.size === data.length;
    
    let allData: Record<string, any>[];
    
    // Check if x column is a date column and detect period from data or query
    const isDateCol = isDateColumnName(xCol);
    let detectedPeriod: DatePeriod | null = null;
    if (isDateCol && data.length > 0) {
      // Sample a few values to detect format
      const sample = data.slice(0, Math.min(5, data.length)).map(r => String(r[xCol]));
      // If all samples look like month-year format, use month period
      if (sample.every(v => /^[A-Za-z]{3}[-/]?\d{2,4}$/i.test(v.trim()))) {
        detectedPeriod = 'month';
      } else {
        // Try to parse as dates to detect period
        const parsedDates = sample.map(v => parseFlexibleDate(v)).filter(d => d !== null);
        if (parsedDates.length > 0) {
          // If we can parse dates, default to month period for pie charts
          detectedPeriod = 'month';
        }
      }
    }
    
    if (isAlreadyAggregated) {
      // Data is already aggregated, but we may still need to normalize dates
      console.log(`   Pie chart: Data is already aggregated (${data.length} unique groups)`);
      
      if (isDateCol && detectedPeriod) {
        // Normalize date values even in already-aggregated data
        console.log(`   Normalizing date values with period: ${detectedPeriod}`);
        const normalizedMap = new Map<string, { displayLabel: string; values: number[] }>();
        
        for (const row of data) {
          const dateValue = String(row[xCol]);
          const normalized = normalizeDateToPeriod(dateValue, detectedPeriod);
          const key = normalized ? normalized.normalizedKey : dateValue;
          const displayLabel = normalized ? normalized.displayLabel : dateValue;
          const yValue = toNumber(row[yCol]);
          
          if (!isNaN(yValue)) {
            if (!normalizedMap.has(key)) {
              normalizedMap.set(key, { displayLabel, values: [] });
            }
            normalizedMap.get(key)!.values.push(yValue);
          }
        }
        
        // Sum up values for each normalized period
        allData = Array.from(normalizedMap.entries()).map(([key, { displayLabel, values }]) => {
          const row: Record<string, any> = {
            [xCol]: displayLabel,
            [yCol]: values.reduce((sum, val) => sum + val, 0),
          };
          // Ensure no Date objects
          for (const [k, v] of Object.entries(row)) {
            row[k] = convertValueForSchema(v);
          }
          return row;
        }).sort((a, b) => toNumber(b[yCol]) - toNumber(a[yCol]));
        
        console.log(`   After normalization: ${allData.length} unique periods`);
      } else {
        // Not a date column or no period detected, use as-is
      allData = data
        .map(row => {
          const mappedRow: Record<string, any> = {
            [xCol]: convertValueForSchema(row[xCol]),
            [yCol]: toNumber(row[yCol]),
          };
          return mappedRow;
        })
        .filter(row => !isNaN(row[yCol]))
        .sort((a, b) => toNumber(b[yCol]) - toNumber(a[yCol]));
      }
    } else {
      // Need to aggregate
      console.log(`   Processing pie chart with aggregation: ${aggregate || 'sum'}`);
      const aggregated = aggregateData(data, xCol, yCol, aggregate || 'sum', detectedPeriod, isDateCol);
      console.log(`   Aggregated data points: ${aggregated.length}`);
      // Convert Date objects to strings for schema validation and sort
      allData = aggregated
        .map(row => {
          const sanitizedRow: Record<string, any> = {};
          for (const [key, value] of Object.entries(row)) {
            sanitizedRow[key] = convertValueForSchema(value);
          }
          return sanitizedRow;
        })
        .sort((a, b) => toNumber(b[yCol]) - toNumber(a[yCol]));
    }
    
    // Calculate total of all items to ensure percentages add up to 100%
    const total = allData.reduce((sum, row) => sum + toNumber(row[yCol]), 0);
    console.log(`   Total value for all categories: ${total}`);
    
    // Take top 5 items
    const top5 = allData.slice(0, 5);
    const remaining = allData.slice(5);
    
    // Calculate sum of remaining items
    const remainingSum = remaining.reduce((sum, row) => sum + toNumber(row[yCol]), 0);
    
    // Build result: top 5 + "Others" category if there are remaining items
    const result = [...top5];
    
    if (remaining.length > 0 && remainingSum > 0) {
      // Create "Others" category with the sum of remaining items
      const othersLabel = `Other ${remaining.length > 1 ? `${remaining.length} items` : 'item'}`;
      result.push({
        [xCol]: othersLabel,
        [yCol]: remainingSum,
      });
      console.log(`   Added "Others" category with ${remaining.length} items, sum: ${remainingSum}`);
    }
    
    // Verify total (should be 100% of original total)
    const resultTotal = result.reduce((sum, row) => sum + toNumber(row[yCol]), 0);
    console.log(`   Pie chart result: ${result.length} segments (top 5 + ${remaining.length > 0 ? 'Others' : 'none'})`);
    console.log(`   Result total: ${resultTotal}, Original total: ${total}, Match: ${Math.abs(resultTotal - total) < 0.01 ? '✅' : '⚠️'}`);
    
    return result;
  }

  if (type === 'bar') {
    // Check if this is a correlation bar chart (has 'variable' and 'correlation' columns)
    // Correlation bar charts already have processed data and shouldn't be aggregated
    const isCorrelationBarChart = (xCol === 'variable' && yCol === 'correlation') ||
                                   (data.length > 0 && data[0].hasOwnProperty('variable') && data[0].hasOwnProperty('correlation'));
    
    if (isCorrelationBarChart) {
      // Correlation bar chart - data is already processed, just return as-is
      // The sorting is already done in correlationAnalyzer.ts based on the requested order
      console.log(`   Processing correlation bar chart (data already processed and sorted)`);
      const result = data
        .map(row => ({
          variable: row.variable || row[xCol],
          correlation: toNumber(row.correlation || row[yCol]),
        }))
        .filter(row => !isNaN(row.correlation));
      
      console.log(`   Correlation bar chart result: ${result.length} bars`);
      return result;
    }
    
    // Regular bar chart - aggregate and sort appropriately
    console.log(`   Processing bar chart with aggregation: ${aggregate || 'sum'}`);
    // Check if x column is a date column and detect period from data
    const isDateCol = isDateColumnName(xCol);
    let detectedPeriod: DatePeriod | null = null;
    if (isDateCol && data.length > 0) {
      // Sample a few values to detect format
      const sample = data.slice(0, Math.min(5, data.length)).map(r => String(r[xCol]));
      // If all samples look like month-year format, use month period
      if (sample.every(v => /^[A-Za-z]{3}[-/]?\d{2,4}$/i.test(v.trim()))) {
        detectedPeriod = 'month';
      }
    }
    const aggregated = aggregateData(data, xCol, yCol, aggregate || 'sum', detectedPeriod, isDateCol);
    console.log(`   Aggregated data points: ${aggregated.length}`);
    
    // Validate aggregated results - ensure we have data after aggregation
    if (aggregated.length === 0) {
      console.warn(`❌ No valid aggregated data points for bar chart. Check that "${xCol}" and "${yCol}" columns exist and "${yCol}" contains numeric values.`);
      return [];
    }
    
    // Check if X column contains dates by:
    // 1. Checking if column name suggests it's a date column
    // 2. Testing a sample of values to see if they parse as dates
    const xColLower = xCol.toLowerCase();
    const nameSuggestsDate = /\b(date|month|week|year|time|period)\b/i.test(xColLower);
    const sampleXValues = aggregated.slice(0, Math.min(10, aggregated.length)).map(row => String(row[xCol]));
    const dateParseCount = sampleXValues.filter(val => parseDate(val) !== null).length;
    const hasDates = nameSuggestsDate || (dateParseCount >= Math.min(3, sampleXValues.length * 0.5));
    
    let result: Record<string, any>[];
    if (hasDates) {
      // X-axis is dates - sort chronologically by date
      console.log(`   X-axis contains dates (${dateParseCount}/${sampleXValues.length} samples parsed as dates), sorting chronologically`);
      result = aggregated
        .sort((a, b) => compareValues(a[xCol], b[xCol]));
      // No limit - show all date-based bars
    } else {
      // X-axis is not dates - sort by Y value (descending)
      result = aggregated
        .sort((a, b) => toNumber(b[yCol]) - toNumber(a[yCol]));
      // No limit - show all bars
    }
    
    // Convert Date objects to strings for schema validation
    result = result.map(row => {
      const sanitizedRow: Record<string, any> = {};
      for (const [key, value] of Object.entries(row)) {
        sanitizedRow[key] = convertValueForSchema(value);
      }
      return sanitizedRow;
    });
    
    console.log(`   Bar chart result: ${result.length} bars`);
    return result;
  }

  if (type === 'line' || type === 'area') {
    console.log(`   Processing ${type} chart`);
    
    // Sort by x and optionally aggregate
    if (aggregate && aggregate !== 'none') {
      console.log(`   Using aggregation: ${aggregate}`);
      // Check if x column is a date column and detect period from data
      const isDateCol = isDateColumnName(xCol);
      let detectedPeriod: DatePeriod | null = null;
      if (isDateCol && data.length > 0) {
        // Sample a few values to detect format
        const sample = data.slice(0, Math.min(5, data.length)).map(r => String(r[xCol]));
        // If all samples look like month-year format, use month period
        if (sample.every(v => /^[A-Za-z]{3}[-/]?\d{2,4}$/i.test(v.trim()))) {
          detectedPeriod = 'month';
        }
      }
      const aggregated = aggregateData(data, xCol, yCol, aggregate, detectedPeriod, isDateCol);
      console.log(`   Aggregated data points: ${aggregated.length}`);
      // Use date-aware sorting
      let result = aggregated.sort((a, b) => compareValues(a[xCol], b[xCol]));
      // Convert Date objects to strings for schema validation
      result = result.map(row => {
        const sanitizedRow: Record<string, any> = {};
        for (const [key, value] of Object.entries(row)) {
          sanitizedRow[key] = convertValueForSchema(value);
        }
        return sanitizedRow;
      });
      
      // Apply optimization to ensure max points limit
      const optimized = optimizeChartData(result, chartSpec);
      if (optimized.length < result.length) {
        console.log(`   ✅ Optimized from ${result.length} to ${optimized.length} points after aggregation`);
      }
      
      console.log(`   ${type} chart result: ${optimized.length} points (sorted chronologically)`);
      return optimized;
    }

    let result = data
      .map((row) => {
        const mappedRow: Record<string, any> = {
          [xCol]: convertValueForSchema(row[xCol]),
          [yCol]: toNumber(row[yCol]),
        };
        
        // Include y2 if it was requested, but only if it's a valid number
        // Convert NaN to null so schema validation passes (null is acceptable)
        if (y2Col) {
          const y2Value = toNumber(row[y2Col]);
          // Only include y2 field if it's a valid number, otherwise omit it
          // This prevents NaN from being sent to frontend and causing validation errors
          if (!isNaN(y2Value) && isFinite(y2Value)) {
            mappedRow[y2Col] = y2Value;
          }
          // If NaN, we simply don't include the y2 field - frontend will handle missing values
        }
        
        return mappedRow;
      })
      .filter((row) => {
        // Keep row if primary Y is valid (required)
        const yValid = !isNaN(row[yCol]) && isFinite(row[yCol]);
        if (!yValid) return false;
        
        // For y2: if y2Col exists, we want to include rows even if y2 is missing/NaN
        // (so we can show primary Y series even if y2 has no data)
        // The y2 field will simply be omitted from the row if it's NaN
        return true;
      })
      // Use date-aware sorting for chronological order
      .sort((a, b) => compareValues(a[xCol], b[xCol]));
    
    // Log y2 data availability
    if (y2Col) {
      const y2ValidCount = result.filter(row => y2Col in row && !isNaN(row[y2Col]) && isFinite(row[y2Col])).length;
      const y2TotalCount = result.length;
      const y2MissingCount = y2TotalCount - y2ValidCount;
      console.log(`   Y2 column "${y2Col}": ${y2ValidCount}/${y2TotalCount} rows have valid numeric values`);
      if (y2ValidCount === 0) {
        console.warn(`   ⚠️ No valid Y2 values found! Chart will only show primary Y series.`);
        // Remove y2 from chart spec if no valid data
        delete (chartSpec as any).y2;
        delete (chartSpec as any).y2Label;
      } else if (y2MissingCount > 0) {
        console.log(`   ℹ️ Y2 has ${y2MissingCount} rows with missing/NaN values (field omitted for those rows)`);
      }
    }
    
    // Apply optimization to ensure max points limit
    const optimized = optimizeChartData(result, chartSpec);
    if (optimized.length < result.length) {
      console.log(`   ✅ Optimized from ${result.length} to ${optimized.length} points`);
    }
    
    console.log(`   ${type} chart result: ${optimized.length} points (sorted chronologically)`);
    return optimized;
  }

  console.warn(`❌ Unknown chart type: ${type} for chart: ${chartSpec.title}`);
  return [];
}

function aggregateData(
  data: Record<string, any>[],
  groupBy: string,
  valueColumn: string,
  aggregateType: string,
  datePeriod?: DatePeriod | null,
  isDateColumn?: boolean
): Record<string, any>[] {
  console.log(`     Aggregating by "${groupBy}" with "${aggregateType}" of "${valueColumn}"${datePeriod ? ` (period: ${datePeriod})` : ''}`);
  
  const grouped = new Map<string, { values: number[]; displayLabel?: string }>();
  let validValues = 0;
  let invalidValues = 0;

  for (const row of data) {
    let key: string;
    let displayLabel: string | undefined;
    
    if (isDateColumn && datePeriod) {
      const normalized = normalizeDateToPeriod(String(row[groupBy]), datePeriod);
      if (normalized) {
        key = normalized.normalizedKey;
        displayLabel = normalized.displayLabel;
      } else {
        key = String(row[groupBy]);
      }
    } else {
      key = String(row[groupBy]);
    }
    
    const value = toNumber(row[valueColumn]);

    if (!isNaN(value)) {
      validValues++;
      if (!grouped.has(key)) {
        grouped.set(key, { values: [], displayLabel });
      }
      grouped.get(key)!.values.push(value);
    } else {
      invalidValues++;
    }
  }

  console.log(`     Valid values: ${validValues}, Invalid values: ${invalidValues}`);
  console.log(`     Unique groups: ${grouped.size}`);

  const result: Record<string, any>[] = [];

  for (const [key, { values, displayLabel }] of Array.from(grouped.entries())) {
    let aggregatedValue: number;

    switch (aggregateType) {
      case 'sum':
        aggregatedValue = values.reduce((a: number, b: number) => a + b, 0);
        break;
      case 'mean':
        aggregatedValue = values.reduce((a: number, b: number) => a + b, 0) / values.length;
        break;
      case 'count':
        aggregatedValue = values.length;
        break;
      default:
        aggregatedValue = values[0];
    }

    result.push({
      [groupBy]: displayLabel || key,  // Use display label if available
      [valueColumn]: aggregatedValue,
    });
  }

  console.log(`     Aggregation result: ${result.length} groups`);
  return result;
}
