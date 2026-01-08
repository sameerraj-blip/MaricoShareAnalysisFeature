/**
 * Smart Axis Scaling Utility
 * Calculates optimal axis domains based on statistical measures (mean, median, mode)
 * to improve chart readability and focus on the relevant data range
 */

/**
 * Calculate mean of numeric values
 */
function calculateMean(values: number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((sum, val) => sum + val, 0) / values.length;
}

/**
 * Calculate median of numeric values
 */
function calculateMedian(values: number[]): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[mid - 1] + sorted[mid]) / 2
    : sorted[mid];
}

/**
 * Calculate mode (most frequent value) of numeric values
 * For continuous data, we use binning to find the most frequent range
 */
function calculateMode(values: number[]): number | null {
  if (values.length === 0) return null;
  
  // For continuous data, use binning approach
  // Create bins and find the bin with most values
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min;
  
  if (range === 0) return min; // All values are the same
  
  // Use Sturges' formula for number of bins: k = ceil(1 + log2(n))
  const n = values.length;
  const numBins = Math.ceil(1 + Math.log2(n));
  const binWidth = range / numBins;
  
  const bins = new Map<number, number>();
  
  for (const value of values) {
    const binIndex = Math.floor((value - min) / binWidth);
    const binCenter = min + (binIndex + 0.5) * binWidth;
    bins.set(binCenter, (bins.get(binCenter) || 0) + 1);
  }
  
  // Find bin with maximum frequency
  let maxFreq = 0;
  let modeBin = null;
  for (const [binCenter, freq] of bins.entries()) {
    if (freq > maxFreq) {
      maxFreq = freq;
      modeBin = binCenter;
    }
  }
  
  return modeBin;
}

/**
 * Calculate standard deviation
 */
function calculateStdDev(values: number[], mean: number): number {
  if (values.length === 0) return 0;
  const variance = values.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / values.length;
  return Math.sqrt(variance);
}

/**
 * Calculate quartiles (Q1, Q3) for IQR-based scaling
 */
function calculateQuartiles(values: number[]): { q1: number; q3: number } {
  if (values.length === 0) return { q1: 0, q3: 0 };
  const sorted = [...values].sort((a, b) => a - b);
  
  const q1Index = Math.floor(sorted.length * 0.25);
  const q3Index = Math.floor(sorted.length * 0.75);
  
  return {
    q1: sorted[q1Index] || 0,
    q3: sorted[q3Index] || 0,
  };
}

/**
 * Smart axis domain calculation based on statistical measures
 * 
 * Strategy:
 * 1. Calculate mean, median, mode, std dev, and quartiles
 * 2. Use IQR (Interquartile Range) to detect outliers
 * 3. Set domain to focus on the central distribution while including relevant outliers
 * 4. Add intelligent padding based on data spread
 * 
 * @param values Array of numeric values
 * @param options Configuration options
 * @returns [min, max] domain tuple or null if invalid
 */
export function calculateSmartDomain(
  values: number[],
  options: {
    useIQR?: boolean; // Use IQR to filter outliers (default: true)
    iqrMultiplier?: number; // Multiplier for IQR outlier detection (default: 1.5)
    paddingPercent?: number; // Percentage padding to add (default: 5%)
    minPadding?: number; // Minimum absolute padding (default: 0)
    includeOutliers?: boolean; // Whether to include outliers in domain (default: true, but with reduced weight)
  } = {}
): [number, number] | null {
  if (values.length === 0) return null;
  
  const {
    useIQR = true,
    iqrMultiplier = 1.5,
    paddingPercent = 5,
    minPadding = 0,
    includeOutliers = true,
  } = options;
  
  // Filter out invalid values
  const validValues = values.filter(v => typeof v === 'number' && isFinite(v) && !isNaN(v));
  if (validValues.length === 0) return null;
  
  // Calculate statistics
  const mean = calculateMean(validValues);
  const median = calculateMedian(validValues);
  const mode = calculateMode(validValues);
  const stdDev = calculateStdDev(validValues, mean);
  const { q1, q3 } = calculateQuartiles(validValues);
  const iqr = q3 - q1;
  
  // Calculate min/max
  let dataMin = Math.min(...validValues);
  let dataMax = Math.max(...validValues);
  
  // Use IQR-based outlier detection if enabled
  if (useIQR && iqr > 0) {
    const lowerBound = q1 - iqrMultiplier * iqr;
    const upperBound = q3 + iqrMultiplier * iqr;
    
    if (includeOutliers) {
      // Include outliers but give more weight to central distribution
      // Use a weighted approach: 70% weight to IQR range, 30% to full range
      const iqrMin = Math.max(lowerBound, dataMin);
      const iqrMax = Math.min(upperBound, dataMax);
      
      dataMin = 0.7 * Math.min(q1, iqrMin) + 0.3 * dataMin;
      dataMax = 0.7 * Math.max(q3, iqrMax) + 0.3 * dataMax;
    } else {
      // Exclude outliers, focus on central distribution
      dataMin = Math.max(lowerBound, q1);
      dataMax = Math.min(upperBound, q3);
    }
  }
  
  // Alternative: Use mean ± std dev for symmetric distributions
  // This is useful when data is normally distributed
  const useMeanStdDev = stdDev > 0 && Math.abs(mean - median) < 0.1 * stdDev;
  
  if (useMeanStdDev && !useIQR) {
    // For symmetric distributions, use mean ± 2.5 std dev (covers ~98% of data)
    const stdDevRange = 2.5 * stdDev;
    dataMin = Math.max(mean - stdDevRange, Math.min(...validValues));
    dataMax = Math.min(mean + stdDevRange, Math.max(...validValues));
  }
  
  // Ensure we don't go beyond actual data bounds
  dataMin = Math.max(dataMin, Math.min(...validValues));
  dataMax = Math.min(dataMax, Math.max(...validValues));
  
  // Calculate padding
  const range = dataMax - dataMin;
  const padding = Math.max(
    (range * paddingPercent) / 100,
    minPadding,
    range === 0 ? 1 : 0 // If all values are the same, add 1 unit padding
  );
  
  const domainMin = dataMin - padding;
  const domainMax = dataMax + padding;
  
  // Ensure domain is valid
  if (!isFinite(domainMin) || !isFinite(domainMax) || domainMin >= domainMax) {
    // Fallback to simple min/max with padding
    const fallbackMin = Math.min(...validValues);
    const fallbackMax = Math.max(...validValues);
    const fallbackRange = fallbackMax - fallbackMin;
    const fallbackPadding = fallbackRange > 0 ? fallbackRange * 0.1 : 1;
    return [fallbackMin - fallbackPadding, fallbackMax + fallbackPadding];
  }
  
  return [domainMin, domainMax];
}

/**
 * Calculate smart domain for chart data column
 * 
 * @param data Chart data array
 * @param columnName Column name to extract values from
 * @param options Configuration options
 * @returns [min, max] domain tuple or null if invalid
 */
export function calculateSmartDomainForColumn(
  data: Record<string, any>[],
  columnName: string,
  options?: Parameters<typeof calculateSmartDomain>[1]
): [number, number] | null {
  const values = data
    .map(row => {
      const value = row[columnName];
      if (value === null || value === undefined || value === '') return NaN;
      const num = typeof value === 'number' ? value : Number(value);
      return isNaN(num) || !isFinite(num) ? NaN : num;
    })
    .filter(v => !isNaN(v));
  
  if (values.length === 0) return null;
  
  return calculateSmartDomain(values, options);
}

/**
 * Calculate smart domains for both X and Y axes of a chart
 * 
 * @param data Chart data array
 * @param xColumn X-axis column name
 * @param yColumn Y-axis column name
 * @param y2Column Optional secondary Y-axis column name
 * @param options Configuration options
 * @returns Object with xDomain, yDomain, and optionally y2Domain
 */
export function calculateSmartDomainsForChart(
  data: Record<string, any>[],
  xColumn: string,
  yColumn: string,
  y2Column?: string,
  options?: {
    xOptions?: Parameters<typeof calculateSmartDomain>[1];
    yOptions?: Parameters<typeof calculateSmartDomain>[1];
    y2Options?: Parameters<typeof calculateSmartDomain>[1];
  }
): {
  xDomain?: [number, number];
  yDomain?: [number, number];
  y2Domain?: [number, number];
} {
  const result: {
    xDomain?: [number, number];
    yDomain?: [number, number];
    y2Domain?: [number, number];
  } = {};
  
  // Check if X column is numeric (for date/time columns, we might not want smart scaling)
  const firstXValue = data[0]?.[xColumn];
  const isXNumeric = typeof firstXValue === 'number' || (!isNaN(Number(firstXValue)) && firstXValue !== null);
  
  if (isXNumeric) {
    const xDomain = calculateSmartDomainForColumn(data, xColumn, options?.xOptions);
    if (xDomain) result.xDomain = xDomain;
  }
  
  // Calculate Y domain
  const yDomain = calculateSmartDomainForColumn(data, yColumn, options?.yOptions);
  if (yDomain) result.yDomain = yDomain;
  
  // Calculate Y2 domain if provided
  if (y2Column) {
    const y2Domain = calculateSmartDomainForColumn(data, y2Column, options?.y2Options);
    if (y2Domain) result.y2Domain = y2Domain;
  }
  
  return result;
}










