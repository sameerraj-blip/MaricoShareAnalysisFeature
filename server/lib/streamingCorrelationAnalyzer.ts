/**
 * Streaming Correlation Analyzer
 * Production-safe correlation computation for large datasets (100k+ rows)
 * Uses single-pass algorithm and sampled visualization
 */

import { ChartSpec } from '../shared/schema.js';

// Adaptive visualization point limit based on dataset size
// For small datasets (<10k), show all points
// For medium datasets (10k-100k), show up to 20k points
// For large datasets (100k-500k), show up to 100k points
// For very large datasets (500k+), show up to 150k points
function getMaxVisualizationPoints(totalRows: number): number {
  if (totalRows < 10000) {
    return totalRows; // Show all points for small datasets
  } else if (totalRows < 100000) {
    return Math.min(20000, totalRows); // Up to 20k for medium datasets
  } else if (totalRows < 500000) {
    return Math.min(100000, totalRows); // Up to 100k for large datasets
  } else {
    return Math.min(150000, totalRows); // Up to 150k for very large datasets
  }
}

const CHUNK_SIZE = 10000; // Process in chunks to avoid blocking event loop

interface CorrelationComputationResult {
  correlation: number;
  nPairs: number;
  slope: number;
  intercept: number;
  xMin: number;
  xMax: number;
  yMin: number;
  yMax: number;
  visualizationData: Array<Record<string, number | null>>;
  method: string;
}

/**
 * Compute correlation using streaming single-pass algorithm
 * Processes data in chunks to avoid blocking event loop
 */
async function computeCorrelationStreaming(
  data: Record<string, any>[],
  xColumn: string,
  yColumn: string,
  onProgress?: (processed: number, total: number) => void
): Promise<CorrelationComputationResult> {
  const toNumber = (value: any): number => {
    if (value === null || value === undefined || value === '') return NaN;
    if (typeof value === 'number') return isNaN(value) || !isFinite(value) ? NaN : value;
    const cleaned = String(value).replace(/[%,]/g, '').trim();
    return Number(cleaned);
  };

  // Single-pass correlation state
  let n = 0;
  let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0, sumY2 = 0;
  let xMin = Infinity, xMax = -Infinity;
  let yMin = Infinity, yMax = -Infinity;
  
  // Determine max visualization points based on dataset size
  const maxVisualizationPoints = getMaxVisualizationPoints(data.length);
  console.log(`📊 Adaptive visualization limit: ${maxVisualizationPoints.toLocaleString()} points (from ${data.length.toLocaleString()} total rows)`);
  
  // Reservoir for sampling (adaptive limit based on dataset size)
  const reservoir: Array<{ x: number; y: number }> = [];
  let reservoirIndex = 0;

  // Process data in chunks to avoid blocking
  for (let i = 0; i < data.length; i++) {
    const x = toNumber(data[i][xColumn]);
    const y = toNumber(data[i][yColumn]);

    if (!isNaN(x) && !isNaN(y) && isFinite(x) && isFinite(y)) {
      // Update correlation state (O(1) per update)
      n++;
      sumX += x;
      sumY += y;
      sumXY += x * y;
      sumX2 += x * x;
      sumY2 += y * y;

      // Track min/max
      if (x < xMin) xMin = x;
      if (x > xMax) xMax = x;
      if (y < yMin) yMin = y;
      if (y > yMax) yMax = y;

      // Reservoir sampling for visualization (adaptive limit)
      if (reservoirIndex < maxVisualizationPoints) {
        reservoir.push({ x, y });
      } else {
        const j = Math.floor(Math.random() * (reservoirIndex + 1));
        if (j < maxVisualizationPoints) {
          reservoir[j] = { x, y };
        }
      }
      reservoirIndex++;
    }

    // Yield control to event loop every chunk
    if (i > 0 && i % CHUNK_SIZE === 0) {
      if (onProgress) {
        onProgress(i, data.length);
      }
      await new Promise(resolve => setImmediate(resolve));
    }
  }

  // Final progress update
  if (onProgress) {
    onProgress(data.length, data.length);
  }

  if (n < 2) {
    throw new Error('Insufficient data for correlation calculation');
  }

  // Calculate correlation coefficient
  const numerator = n * sumXY - sumX * sumY;
  const denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));
  const correlation = denominator === 0 ? NaN : numerator / denominator;

  // Calculate regression for trend line
  const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
  const intercept = (sumY - slope * sumX) / n;

  // Convert reservoir to chart data format
  const visualizationData: Array<Record<string, number | null>> = reservoir.map(({ x, y }) => ({
    [xColumn]: x,
    [yColumn]: y,
  }));

  return {
    correlation: isNaN(correlation) ? 0 : correlation,
    nPairs: n,
    slope: isNaN(slope) ? 0 : slope,
    intercept: isNaN(intercept) ? 0 : intercept,
    xMin: isFinite(xMin) ? xMin : 0,
    xMax: isFinite(xMax) ? xMax : 0,
    yMin: isFinite(yMin) ? yMin : 0,
    yMax: isFinite(yMax) ? yMax : 0,
    visualizationData,
    method: 'streaming-single-pass-sampled-visualization',
  };
}


/**
 * Generate correlation chart with streaming computation
 */
export async function generateStreamingCorrelationChart(
  data: Record<string, any>[],
  targetVariable: string,
  factorVariable: string,
  onProgress?: (processed: number, total: number) => void
): Promise<ChartSpec> {
  console.log(`📊 Computing streaming correlation (${data.length} rows): ${factorVariable} vs ${targetVariable}`);
  
  const result = await computeCorrelationStreaming(
    data,
    factorVariable,
    targetVariable,
    onProgress
  );

  // Calculate domains with padding
  const xRange = result.xMax - result.xMin;
  const yRange = result.yMax - result.yMin;
  const xPadding = xRange > 0 ? xRange * 0.05 : 1;
  const yPadding = yRange > 0 ? yRange * 0.05 : 1;

  const xDomain: [number, number] = [
    result.xMin - xPadding,
    result.xMax + xPadding,
  ];
  const yDomain: [number, number] = [
    result.yMin - yPadding,
    result.yMax + yPadding,
  ];

  // Calculate trend line from regression
  const trendLine: Array<Record<string, number>> = [
    { [factorVariable]: xDomain[0], [targetVariable]: result.slope * xDomain[0] + result.intercept },
    { [factorVariable]: xDomain[1], [targetVariable]: result.slope * xDomain[1] + result.intercept },
  ];

  return {
    type: 'scatter',
    title: `${factorVariable} vs ${targetVariable} (r=${result.correlation.toFixed(2)})`,
    x: factorVariable,
    y: targetVariable,
    xLabel: factorVariable,
    yLabel: targetVariable,
    data: result.visualizationData, // Only sampled points for visualization
    xDomain,
    yDomain,
    trendLine,
    _isCorrelationChart: true,
    _targetVariable: targetVariable,
    _factorVariable: factorVariable,
    _correlationMetadata: {
      correlation: result.correlation,
      nPairs: result.nPairs,
      method: result.method,
      totalDataPoints: result.nPairs,
      visualizationPoints: result.visualizationData.length,
    },
  };
}

