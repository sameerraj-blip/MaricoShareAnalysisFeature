/**
 * Chart Downsampling and Aggregation Service
 * Optimizes chart generation for large datasets by:
 * - Server-side downsampling and aggregation
 * - Limiting chart points to maximum of 5,000
 * - Time-based resampling for time series
 * - Sending only aggregated results to frontend
 */

import { ChartSpec } from '../shared/schema.js';
import { parseFlexibleDate, normalizeDateToPeriod, DatePeriod, isDateColumnName } from './dateUtils.js';
import { findMatchingColumn } from './agents/utils/columnMatcher.js';

// Maximum data points for visualization
const MAX_CHART_POINTS = 5000;

/**
 * Helper to clean numeric values
 */
function toNumber(value: any): number {
  if (value === null || value === undefined || value === '') return NaN;
  if (typeof value === 'number') return isNaN(value) || !isFinite(value) ? NaN : value;
  const cleaned = String(value).replace(/[%,$€£¥₹\s]/g, '').trim();
  return Number(cleaned);
}

/**
 * Time-based resampling for time series data
 * Aggregates data by time periods (day, week, month, quarter, year)
 */
export function resampleTimeSeries(
  data: Record<string, any>[],
  xColumn: string,
  yColumn: string,
  period: DatePeriod = 'day',
  aggregate: 'sum' | 'mean' | 'count' = 'mean'
): Record<string, any>[] {
  if (data.length === 0) return [];

  // Group data by time period
  const grouped = new Map<string, { values: number[]; date: Date | null }>();

  for (const row of data) {
    const dateStr = String(row[xColumn]);
    const date = parseFlexibleDate(dateStr);
    const yValue = toNumber(row[yColumn]);

    if (isNaN(yValue)) continue;

    if (date) {
      // Normalize to period
      const normalized = normalizeDateToPeriod(dateStr, period);
      if (normalized) {
        const key = normalized.normalizedKey;
        if (!grouped.has(key)) {
          grouped.set(key, { values: [], date: date });
        }
        grouped.get(key)!.values.push(yValue);
      }
    } else {
      // If date parsing fails, use original value as key
      const key = dateStr;
      if (!grouped.has(key)) {
        grouped.set(key, { values: [], date: null });
      }
      grouped.get(key)!.values.push(yValue);
    }
  }

  // Aggregate values for each period
  const result: Record<string, any>[] = [];
  for (const [key, { values, date }] of grouped.entries()) {
    let aggregatedValue: number;
    switch (aggregate) {
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
      [xColumn]: key,
      [yColumn]: aggregatedValue,
    });
  }

  // Sort by date if possible
  return result.sort((a, b) => {
    const dateA = parseFlexibleDate(String(a[xColumn]));
    const dateB = parseFlexibleDate(String(b[xColumn]));
    if (dateA && dateB) {
      return dateA.getTime() - dateB.getTime();
    }
    return String(a[xColumn]).localeCompare(String(b[xColumn]));
  });
}

/**
 * Determine optimal time period for resampling based on data range
 */
function determineOptimalPeriod(
  data: Record<string, any>[],
  xColumn: string
): DatePeriod | null {
  if (data.length < 2) return null;

  const dates = data
    .map(row => parseFlexibleDate(String(row[xColumn])))
    .filter((d): d is Date => d !== null);

  if (dates.length < 2) return null;

  dates.sort((a, b) => a.getTime() - b.getTime());
  const timeSpan = dates[dates.length - 1].getTime() - dates[0].getTime();
  const days = timeSpan / (1000 * 60 * 60 * 24);

  // Determine period based on time span and data points
  if (days > 365 * 2) return 'year';
  if (days > 90) return 'month';
  if (days > 14) return 'week';
  return 'day';
}

/**
 * Aggregate-based downsampling for large datasets
 * Groups data into buckets and aggregates values within each bucket
 */
export function aggregateDownsample(
  data: Record<string, any>[],
  xColumn: string,
  yColumn: string,
  maxPoints: number = MAX_CHART_POINTS,
  aggregate: 'sum' | 'mean' | 'count' = 'mean'
): Record<string, any>[] {
  if (data.length <= maxPoints) return data;

  // Check if x-axis is numeric
  const firstX = toNumber(data[0]?.[xColumn]);
  const isNumericX = !isNaN(firstX);

  if (isNumericX) {
    // For numeric x-axis, create evenly spaced buckets
    const sorted = [...data].sort((a, b) => {
      const aVal = toNumber(a[xColumn]);
      const bVal = toNumber(b[xColumn]);
      return aVal - bVal;
    });

    const minX = toNumber(sorted[0][xColumn]);
    const maxX = toNumber(sorted[sorted.length - 1][xColumn]);
    const range = maxX - minX;
    const bucketSize = range / maxPoints;

    const buckets = new Map<number, number[]>();
    for (const row of sorted) {
      const xVal = toNumber(row[xColumn]);
      const yVal = toNumber(row[yColumn]);
      if (isNaN(yVal)) continue;

      const bucketKey = Math.floor((xVal - minX) / bucketSize);
      if (!buckets.has(bucketKey)) {
        buckets.set(bucketKey, []);
      }
      buckets.get(bucketKey)!.push(yVal);
    }

    // Aggregate each bucket
    const result: Record<string, any>[] = [];
    for (const [bucketKey, values] of buckets.entries()) {
      let aggregatedValue: number;
      switch (aggregate) {
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

      const xValue = minX + (bucketKey + 0.5) * bucketSize;
      result.push({
        [xColumn]: xValue,
        [yColumn]: aggregatedValue,
      });
    }

    return result.sort((a, b) => toNumber(a[xColumn]) - toNumber(b[xColumn]));
  } else {
    // For non-numeric x-axis, use simple grouping
    const grouped = new Map<string, number[]>();
    for (const row of data) {
      const key = String(row[xColumn]);
      const yVal = toNumber(row[yColumn]);
      if (isNaN(yVal)) continue;

      if (!grouped.has(key)) {
        grouped.set(key, []);
      }
      grouped.get(key)!.push(yVal);
    }

    // If still too many groups, sample them
    if (grouped.size > maxPoints) {
      const keys = Array.from(grouped.keys());
      const step = Math.floor(keys.length / maxPoints);
      const sampledKeys = keys.filter((_, idx) => idx % step === 0).slice(0, maxPoints);
      
      const result: Record<string, any>[] = [];
      for (const key of sampledKeys) {
        const values = grouped.get(key)!;
        let aggregatedValue: number;
        switch (aggregate) {
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
          [xColumn]: key,
          [yColumn]: aggregatedValue,
        });
      }
      return result;
    }

    // Aggregate values for each group
    const result: Record<string, any>[] = [];
    for (const [key, values] of grouped.entries()) {
      let aggregatedValue: number;
      switch (aggregate) {
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
        [xColumn]: key,
        [yColumn]: aggregatedValue,
      });
    }

    return result;
  }
}

/**
 * Largest-Triangle-Three-Buckets (LTTB) downsampling algorithm
 * Preserves visual shape better than simple decimation for line charts
 */
function downsampleLTTB(
  data: Record<string, any>[],
  xKey: string,
  yKey: string,
  threshold: number
): Record<string, any>[] {
  if (data.length <= threshold) return data;

  const dataLength = data.length;
  if (threshold >= dataLength || threshold === 0) return data;

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
    rangeA = Math.floor((i + 1) * every) + 1;
    rangeB = Math.floor((i + 2) * every) + 1;
    if (rangeB > dataLength) {
      rangeB = dataLength;
    }

    const avgX = (toNumber(data[rangeA][xKey]) + toNumber(data[rangeB][xKey])) / 2;
    const avgY = (toNumber(data[rangeA][yKey]) + toNumber(data[rangeB][yKey])) / 2;

    const rangeOffs = Math.floor((i + 0) * every) + 1;
    const rangeTo = Math.floor((i + 1) * every) + 1;

    const pointAX = toNumber(data[a][xKey]);
    const pointAY = toNumber(data[a][yKey]);

    maxArea = -1;
    maxAreaPoint = data[rangeOffs];

    for (let j = rangeOffs; j < rangeTo && j < dataLength; j++) {
      area = Math.abs(
        (pointAX - avgX) * (toNumber(data[j][yKey]) - pointAY) -
        (pointAX - toNumber(data[j][xKey])) * (avgY - pointAY)
      ) * 0.5;
      if (area > maxArea) {
        maxArea = area;
        maxAreaPoint = data[j];
        nextA = j;
      }
    }

    sampled.push(maxAreaPoint);
    a = nextA;
  }

  sampled.push(data[dataLength - 1]); // Always add last point
  return sampled;
}

/**
 * Main downsampling function that applies appropriate strategy based on chart type and data
 */
export function downsampleChartData(
  data: Record<string, any>[],
  chartSpec: ChartSpec,
  maxPoints: number = MAX_CHART_POINTS
): Record<string, any>[] {
  if (data.length <= maxPoints) return data;

  const { type, x, y, aggregate = 'none' } = chartSpec;
  const availableColumns = Object.keys(data[0] || {});
  const matchedX = findMatchingColumn(x, availableColumns) || x;
  const matchedY = findMatchingColumn(y, availableColumns) || y;

  // Check if x-axis is a date column
  const isDateCol = isDateColumnName(matchedX);
  const hasDates = isDateCol || data.some(row => parseFlexibleDate(String(row[matchedX])) !== null);

  // For time series charts, use time-based resampling
  if ((type === 'line' || type === 'area') && hasDates && aggregate === 'none') {
    const period = determineOptimalPeriod(data, matchedX);
    if (period) {
      console.log(`📊 Time series detected: Resampling to ${period} periods (${data.length} → ~${Math.ceil(data.length / (period === 'day' ? 1 : period === 'week' ? 7 : period === 'month' ? 30 : 365))} points)`);
      const resampled = resampleTimeSeries(data, matchedX, matchedY, period, 'mean');
      // If still too many points, apply additional downsampling
      if (resampled.length > maxPoints) {
        return downsampleLTTB(resampled, matchedX, matchedY, maxPoints);
      }
      return resampled;
    }
  }

  // For charts with aggregation, use aggregate-based downsampling
  if (aggregate !== 'none') {
    console.log(`📊 Using aggregation-based downsampling (${data.length} → ${maxPoints} points)`);
    return aggregateDownsample(data, matchedX, matchedY, maxPoints, aggregate as 'sum' | 'mean' | 'count');
  }

  // For line/area charts, use LTTB if x is numeric
  if ((type === 'line' || type === 'area') && !hasDates) {
    const firstX = toNumber(data[0]?.[matchedX]);
    if (!isNaN(firstX)) {
      console.log(`📊 Using LTTB downsampling for line chart (${data.length} → ${maxPoints} points)`);
      return downsampleLTTB(data, matchedX, matchedY, maxPoints);
    }
  }

  // For scatter plots, use stratified sampling
  if (type === 'scatter') {
    console.log(`📊 Using stratified sampling for scatter plot (${data.length} → ${maxPoints} points)`);
    return aggregateDownsample(data, matchedX, matchedY, maxPoints, 'mean');
  }

  // Default: simple decimation
  console.log(`📊 Using simple decimation (${data.length} → ${maxPoints} points)`);
  const step = Math.floor(data.length / maxPoints);
  return data.filter((_, idx) => idx % step === 0).slice(0, maxPoints);
}

/**
 * Ensure chart data never exceeds max points and is properly aggregated
 */
export function optimizeChartData(
  data: Record<string, any>[],
  chartSpec: ChartSpec
): Record<string, any>[] {
  // Always apply downsampling if data exceeds max points
  if (data.length > MAX_CHART_POINTS) {
    return downsampleChartData(data, chartSpec, MAX_CHART_POINTS);
  }
  return data;
}

