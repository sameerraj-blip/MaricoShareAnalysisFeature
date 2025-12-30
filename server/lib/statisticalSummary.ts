import { DataSummary } from '../shared/schema.js';

/**
 * Column Statistics Interface
 */
export interface ColumnStats {
  column: string;
  type: 'numeric' | 'categorical' | 'date';
  count: number;
  nullCount: number;
  // For numeric columns
  mean?: number;
  median?: number;
  min?: number;
  max?: number;
  stdDev?: number;
  q1?: number;
  q3?: number;
  // For categorical columns
  uniqueValues?: number;
  topValues?: Array<{ value: any; count: number }>;
  // For date columns
  dateRange?: { min: string; max: string };
}

/**
 * Statistical Summary for a dataset
 */
export interface StatisticalSummary {
  rowCount: number;
  columnCount: number;
  columns: ColumnStats[];
}

/**
 * Calculate statistics for a numeric column
 */
function calculateNumericStats(
  values: number[],
  columnName: string
): Partial<ColumnStats> {
  if (values.length === 0) {
    return {
      count: 0,
      nullCount: 0,
    };
  }

  const sorted = [...values].sort((a, b) => a - b);
  const count = values.length;
  const sum = values.reduce((a, b) => a + b, 0);
  const mean = sum / count;
  const median = sorted[Math.floor(count / 2)];
  const q1 = sorted[Math.floor(count * 0.25)];
  const q3 = sorted[Math.floor(count * 0.75)];
  const min = sorted[0];
  const max = sorted[count - 1];

  // Calculate standard deviation
  const variance = values.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / count;
  const stdDev = Math.sqrt(variance);

  return {
    column: columnName,
    type: 'numeric',
    count,
    nullCount: 0, // Will be set by caller
    mean: Number.isFinite(mean) ? mean : undefined,
    median: Number.isFinite(median) ? median : undefined,
    min: Number.isFinite(min) ? min : undefined,
    max: Number.isFinite(max) ? max : undefined,
    stdDev: Number.isFinite(stdDev) ? stdDev : undefined,
    q1: Number.isFinite(q1) ? q1 : undefined,
    q3: Number.isFinite(q3) ? q3 : undefined,
  };
}

/**
 * Calculate statistics for a categorical column
 */
function calculateCategoricalStats(
  values: any[],
  columnName: string
): Partial<ColumnStats> {
  if (values.length === 0) {
    return {
      count: 0,
      nullCount: 0,
    };
  }

  const valueCounts = new Map<any, number>();
  values.forEach(val => {
    if (val !== null && val !== undefined && val !== '') {
      const count = valueCounts.get(val) || 0;
      valueCounts.set(val, count + 1);
    }
  });

  const uniqueValues = valueCounts.size;
  const topValues = Array.from(valueCounts.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([value, count]) => ({ value, count }));

  return {
    column: columnName,
    type: 'categorical',
    count: values.length,
    nullCount: values.filter(v => v === null || v === undefined || v === '').length,
    uniqueValues,
    topValues: topValues.length > 0 ? topValues : undefined,
  };
}

/**
 * Calculate statistics for a date column
 */
function calculateDateStats(
  values: any[],
  columnName: string
): Partial<ColumnStats> {
  if (values.length === 0) {
    return {
      count: 0,
      nullCount: 0,
    };
  }

  const validDates = values
    .filter(v => v !== null && v !== undefined && v !== '')
    .map(v => String(v))
    .sort();

  if (validDates.length === 0) {
    return {
      column: columnName,
      type: 'date',
      count: values.length,
      nullCount: values.length,
    };
  }

  return {
    column: columnName,
    type: 'date',
    count: values.length,
    nullCount: values.length - validDates.length,
    dateRange: {
      min: validDates[0],
      max: validDates[validDates.length - 1],
    },
  };
}

/**
 * Create statistical summary for specified columns
 * This provides AI with data insights without sending raw data
 */
export function createStatisticalSummary(
  data: Record<string, any>[],
  summary: DataSummary,
  requiredColumns: string[]
): StatisticalSummary {
  if (!data || data.length === 0) {
    return {
      rowCount: 0,
      columnCount: 0,
      columns: [],
    };
  }

  const columnStats: ColumnStats[] = [];
  const availableColumns = Object.keys(data[0] || {});

  // Match required columns to available columns (case-insensitive)
  const matchedColumns = requiredColumns
    .map(reqCol => {
      const reqLower = reqCol.toLowerCase().trim();
      return availableColumns.find(
        availCol => availCol.toLowerCase().trim() === reqLower
      ) || reqCol;
    })
    .filter(col => availableColumns.includes(col));

  // If no matches, use all available columns (fallback)
  const columnsToProcess = matchedColumns.length > 0 ? matchedColumns : availableColumns;

  for (const colName of columnsToProcess) {
    const columnData = data.map(row => row[colName]);
    const nonNullData = columnData.filter(v => v !== null && v !== undefined && v !== '');

    // Determine column type from summary
    const isNumeric = summary.numericColumns.includes(colName);
    const isDate = summary.dateColumns.includes(colName);
    const isCategorical = !isNumeric && !isDate;

    let stats: Partial<ColumnStats>;

    if (isNumeric) {
      const numericValues = nonNullData
        .map(v => {
          if (typeof v === 'number') return v;
          const cleaned = String(v).replace(/[%,$€£¥₹\s]/g, '').trim();
          const num = Number(cleaned);
          return isNaN(num) ? null : num;
        })
        .filter((v): v is number => v !== null && typeof v === 'number' && isFinite(v));

      stats = calculateNumericStats(numericValues, colName);
      stats.nullCount = columnData.length - numericValues.length;
    } else if (isDate) {
      stats = calculateDateStats(columnData, colName);
    } else {
      stats = calculateCategoricalStats(columnData, colName);
    }

    columnStats.push(stats as ColumnStats);
  }

  return {
    rowCount: data.length,
    columnCount: columnStats.length,
    columns: columnStats,
  };
}

/**
 * Format statistical summary as a compact string for AI prompts
 */
export function formatStatisticalSummary(summary: StatisticalSummary): string {
  if (summary.columns.length === 0) {
    return `Dataset has ${summary.rowCount} rows and ${summary.columnCount} columns.`;
  }

  const parts: string[] = [];
  parts.push(`Dataset: ${summary.rowCount} rows, ${summary.columnCount} columns\n`);

  summary.columns.forEach(col => {
    if (col.type === 'numeric') {
      parts.push(
        `${col.column} (numeric): count=${col.count}, nulls=${col.nullCount}, ` +
        `mean=${col.mean?.toFixed(2) || 'N/A'}, median=${col.median?.toFixed(2) || 'N/A'}, ` +
        `min=${col.min?.toFixed(2) || 'N/A'}, max=${col.max?.toFixed(2) || 'N/A'}, ` +
        `std=${col.stdDev?.toFixed(2) || 'N/A'}`
      );
    } else if (col.type === 'categorical') {
      const topValuesStr = col.topValues
        ?.slice(0, 5)
        .map(tv => `${tv.value}(${tv.count})`)
        .join(', ') || 'none';
      parts.push(
        `${col.column} (categorical): count=${col.count}, nulls=${col.nullCount}, ` +
        `unique=${col.uniqueValues}, top=[${topValuesStr}]`
      );
    } else if (col.type === 'date') {
      parts.push(
        `${col.column} (date): count=${col.count}, nulls=${col.nullCount}, ` +
        `range=[${col.dateRange?.min || 'N/A'} to ${col.dateRange?.max || 'N/A'}]`
      );
    }
  });

  return parts.join('\n');
}



