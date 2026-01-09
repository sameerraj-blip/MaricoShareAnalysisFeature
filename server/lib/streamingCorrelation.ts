/**
 * Streaming Correlation Calculator
 * Computes Pearson correlation coefficient using single-pass algorithm
 * Memory-efficient: O(1) space complexity, O(n) time complexity
 */

interface CorrelationState {
  n: number;           // Number of valid pairs
  sumX: number;        // Sum of X values
  sumY: number;        // Sum of Y values
  sumXY: number;       // Sum of X * Y
  sumX2: number;       // Sum of X^2
  sumY2: number;      // Sum of Y^2
}

interface CorrelationResult {
  correlation: number;
  nPairs: number;
  slope: number;
  intercept: number;
}

/**
 * Initialize correlation state
 */
export function initCorrelationState(): CorrelationState {
  return {
    n: 0,
    sumX: 0,
    sumY: 0,
    sumXY: 0,
    sumX2: 0,
    sumY2: 0,
  };
}

/**
 * Update correlation state with a single pair (incremental)
 * This is the core streaming operation - O(1) per update
 */
export function updateCorrelationState(
  state: CorrelationState,
  x: number,
  y: number
): void {
  if (isNaN(x) || isNaN(y) || !isFinite(x) || !isFinite(y)) {
    return; // Skip invalid pairs
  }

  state.n += 1;
  state.sumX += x;
  state.sumY += y;
  state.sumXY += x * y;
  state.sumX2 += x * x;
  state.sumY2 += y * y;
}

/**
 * Compute final correlation coefficient from state
 * Uses standard Pearson correlation formula
 */
export function computeCorrelation(state: CorrelationState): CorrelationResult | null {
  if (state.n < 2) {
    return null; // Need at least 2 pairs
  }

  const n = state.n;
  const numerator = n * state.sumXY - state.sumX * state.sumY;
  const denominator = Math.sqrt(
    (n * state.sumX2 - state.sumX * state.sumX) * 
    (n * state.sumY2 - state.sumY * state.sumY)
  );

  if (denominator === 0) {
    return null; // No variance
  }

  const correlation = numerator / denominator;

  // Calculate linear regression for trend line
  const slope = (n * state.sumXY - state.sumX * state.sumY) / (n * state.sumX2 - state.sumX * state.sumX);
  const intercept = (state.sumY - slope * state.sumX) / n;

  return {
    correlation: isNaN(correlation) ? NaN : correlation,
    nPairs: n,
    slope: isNaN(slope) ? 0 : slope,
    intercept: isNaN(intercept) ? 0 : intercept,
  };
}

/**
 * Streaming correlation calculator for large datasets
 * Processes data in chunks to avoid memory issues
 */
export async function computeStreamingCorrelation(
  dataIterator: AsyncIterableIterator<Record<string, any>> | IterableIterator<Record<string, any>>,
  xColumn: string,
  yColumn: string,
  onProgress?: (processed: number, total?: number) => void
): Promise<{
  correlation: number;
  nPairs: number;
  slope: number;
  intercept: number;
  xMin: number;
  xMax: number;
  yMin: number;
  yMax: number;
}> {
  const state = initCorrelationState();
  let processed = 0;
  let xMin = Infinity;
  let xMax = -Infinity;
  let yMin = Infinity;
  let yMax = -Infinity;

  // Helper to convert value to number
  const toNumber = (value: any): number => {
    if (value === null || value === undefined || value === '') return NaN;
    if (typeof value === 'number') return isNaN(value) || !isFinite(value) ? NaN : value;
    const cleaned = String(value).replace(/[%,]/g, '').trim();
    return Number(cleaned);
  };

  // Process data in chunks to avoid blocking event loop
  const CHUNK_SIZE = 10000;
  let chunk: Record<string, any>[] = [];
  let chunkCount = 0;

  for await (const row of dataIterator) {
    const x = toNumber(row[xColumn]);
    const y = toNumber(row[yColumn]);

    if (!isNaN(x) && !isNaN(y) && isFinite(x) && isFinite(y)) {
      updateCorrelationState(state, x, y);
      
      // Track min/max for domain calculation
      if (x < xMin) xMin = x;
      if (x > xMax) xMax = x;
      if (y < yMin) yMin = y;
      if (y > yMax) yMax = y;
    }

    processed++;
    chunk.push(row);

    // Process chunk and yield control periodically
    if (chunk.length >= CHUNK_SIZE) {
      chunkCount++;
      if (onProgress) {
        onProgress(processed);
      }
      
      // Yield control to event loop every chunk
      await new Promise(resolve => setImmediate(resolve));
      chunk = [];
    }
  }

  // Final progress update
  if (onProgress) {
    onProgress(processed);
  }

  const result = computeCorrelation(state);
  if (!result) {
    throw new Error('Insufficient data for correlation calculation');
  }

  return {
    ...result,
    xMin: isFinite(xMin) ? xMin : 0,
    xMax: isFinite(xMax) ? xMax : 0,
    yMin: isFinite(yMin) ? yMin : 0,
    yMax: isFinite(yMax) ? yMax : 0,
  };
}

/**
 * Generate representative sample for visualization
 * Uses stratified sampling to preserve distribution
 */
export function sampleForVisualization(
  dataIterator: AsyncIterableIterator<Record<string, any>> | IterableIterator<Record<string, any>>,
  xColumn: string,
  yColumn: string,
  maxPoints: number = 2000
): Promise<Array<Record<string, number | null>>> {
  return new Promise(async (resolve, reject) => {
    const validPoints: Array<{ x: number; y: number; row: Record<string, any> }> = [];
    const toNumber = (value: any): number => {
      if (value === null || value === undefined || value === '') return NaN;
      if (typeof value === 'number') return isNaN(value) || !isFinite(value) ? NaN : value;
      const cleaned = String(value).replace(/[%,]/g, '').trim();
      return Number(cleaned);
    };

    try {
      // First pass: collect valid points (we need to sample, but can't load all)
      // Use reservoir sampling for memory efficiency
      let index = 0;
      const reservoir: typeof validPoints = [];

      for await (const row of dataIterator) {
        const x = toNumber(row[xColumn]);
        const y = toNumber(row[yColumn]);

        if (!isNaN(x) && !isNaN(y) && isFinite(x) && isFinite(y)) {
          if (index < maxPoints) {
            reservoir.push({ x, y, row });
          } else {
            // Replace with decreasing probability
            const j = Math.floor(Math.random() * (index + 1));
            if (j < maxPoints) {
              reservoir[j] = { x, y, row };
            }
          }
          index++;
        }
      }

      // Convert to chart data format
      const sampled = reservoir.map(({ x, y, row }) => ({
        [xColumn]: x,
        [yColumn]: y,
      }));

      resolve(sampled);
    } catch (error) {
      reject(error);
    }
  });
}

