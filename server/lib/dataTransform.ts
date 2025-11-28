import { ParsedQuery, TimeFilter, ValueFilter, ExclusionFilter, AggregationRequest, SortRequest, TopBottomRequest, AggregationOperation } from '../shared/queryTypes.js';
import { DataSummary } from '../shared/schema.js';
import { normalizeDateToPeriod, DatePeriod, parseFlexibleDate } from './dateUtils.js';

interface TransformationResult {
  data: Record<string, any>[];
  descriptions: string[];
}

const MONTH_MAP: Record<string, number> = {
  january: 0,
  jan: 0,
  february: 1,
  feb: 1,
  march: 2,
  mar: 2,
  april: 3,
  apr: 3,
  may: 4,
  june: 5,
  jun: 5,
  july: 6,
  jul: 6,
  august: 7,
  aug: 7,
  september: 8,
  sept: 8,
  sep: 8,
  october: 9,
  oct: 9,
  november: 10,
  nov: 10,
  december: 11,
  dec: 11,
};

function toNumber(value: any): number {
  if (value === null || value === undefined || value === '') return NaN;
  const cleaned = String(value).replace(/[%,]/g, '').trim();
  return Number(cleaned);
}

function parseDate(value: any): Date | null {
  // Use the flexible date parser from dateUtils for consistent parsing
  return parseFlexibleDate(value);
}

function resolveDateColumn(summary: DataSummary, column?: string): string | undefined {
  if (column) return column;
  return summary.dateColumns[0];
}

function applyTimeFilter(
  data: Record<string, any>[],
  summary: DataSummary,
  filter: TimeFilter
): { data: Record<string, any>[]; description?: string } {
  const column = resolveDateColumn(summary, filter.column);
  if (!column) return { data };

  const result = data.filter((row) => {
    const date = parseDate(row[column]);
    if (!date) return false;

    switch (filter.type) {
      case 'year':
        if (!filter.years || !filter.years.length) return true;
        return filter.years.includes(date.getFullYear());
      case 'month':
        if (!filter.months || !filter.months.length) return true;
        const monthName = date.toLocaleString('en-US', { month: 'long' });
        // Check full month name match
        const monthMatch = filter.months.some((m) => {
          if (monthName.toLowerCase() === m.toLowerCase()) return true;
          
          // Also check if the filter month matches when we parse the row's date value
          // This handles cases where data has "Apr-24" format and filter is "April"
          const rowDateStr = String(row[column]);
          const parsedRowDate = parseDate(rowDateStr);
          if (parsedRowDate) {
            const rowMonthName = parsedRowDate.toLocaleString('en-US', { month: 'long' });
            const rowYear = parsedRowDate.getFullYear();
            // Check if month and year match (for month-year formats like "Apr-24")
            if (rowMonthName.toLowerCase() === m.toLowerCase() && 
                rowYear === date.getFullYear()) {
              return true;
            }
          }
          return false;
        });
        return monthMatch;
      case 'quarter':
        if (!filter.quarters || !filter.quarters.length) return true;
        const quarter = Math.floor(date.getMonth() / 3) + 1;
        return filter.quarters.includes(quarter as 1 | 2 | 3 | 4);
      case 'dateRange': {
        if (!filter.startDate && !filter.endDate) return true;
        
        // Parse filter dates and ensure they're at start/end of day for inclusive comparison
        let startDate: Date | null = null;
        let endDate: Date | null = null;
        
        if (filter.startDate) {
          startDate = new Date(filter.startDate);
          // Set to start of day (00:00:00) to ensure inclusive lower bound
          startDate.setHours(0, 0, 0, 0);
        }
        
        if (filter.endDate) {
          endDate = new Date(filter.endDate);
          // Set to end of day (23:59:59.999) to ensure inclusive upper bound
          endDate.setHours(23, 59, 59, 999);
        }
        
        // Normalize the row date to start of day for consistent comparison
        const rowDate = new Date(date);
        rowDate.setHours(0, 0, 0, 0);
        
        const afterStart = startDate ? rowDate >= startDate : true;
        const beforeEnd = endDate ? rowDate <= endDate : true;
        return afterStart && beforeEnd;
      }
      case 'relative': {
        if (!filter.relative) return true;
        const { unit, direction, amount } = filter.relative;
        const now = data
          .map((row) => parseDate(row[column]))
          .filter((d): d is Date => d !== null)
          .reduce((latest: Date | null, current) => {
            if (!latest || current > latest) return current;
            return latest;
          }, null) || new Date();
        const comparison = new Date(now);
        switch (unit) {
          case 'month':
            comparison.setMonth(comparison.getMonth() - (direction === 'past' ? amount : -amount));
            break;
          case 'quarter':
            comparison.setMonth(comparison.getMonth() - (direction === 'past' ? amount * 3 : -amount * 3));
            break;
          case 'year':
            comparison.setFullYear(comparison.getFullYear() - (direction === 'past' ? amount : -amount));
            break;
          case 'week':
            comparison.setDate(comparison.getDate() - (direction === 'past' ? amount * 7 : -amount * 7));
            break;
        }
        if (direction === 'past') {
          return date >= comparison && date <= now;
        }
        return date >= now && date <= comparison;
      }
      default:
        return true;
    }
  });

  let description: string | undefined;
  switch (filter.type) {
    case 'year':
      description = `Year${filter.years && filter.years.length > 1 ? 's' : ''} ${filter.years?.join(', ')}`;
      break;
    case 'month':
      description = `Month${filter.months && filter.months.length > 1 ? 's' : ''} ${filter.months?.join(', ')}`;
      break;
    case 'quarter':
      description = `Quarter${filter.quarters && filter.quarters.length > 1 ? 's' : ''} ${filter.quarters?.join(', ')}`;
      break;
    case 'dateRange':
      description = `Dates ${filter.startDate || ''} to ${filter.endDate || ''}`.trim();
      break;
    case 'relative':
      if (filter.relative) {
        description = `${filter.relative.direction === 'past' ? 'Last' : 'Next'} ${filter.relative.amount} ${filter.relative.unit}${filter.relative.amount > 1 ? 's' : ''}`;
      }
      break;
  }

  return { data: result, description };
}

function applyValueFilter(data: Record<string, any>[], filter: ValueFilter): { data: Record<string, any>[]; description?: string } {
  // Calculate reference value once if needed (for mean, median, etc.)
  let referenceVal: number | null = null;
  if (filter.reference) {
    referenceVal = referenceValue(data[0] || {}, filter, data);
  }
  
  const result = data.filter((row) => {
    const value = toNumber(row[filter.column]);
    if (isNaN(value)) return false;
    
    const compareValue: number | null = filter.reference && referenceVal !== null ? referenceVal : (filter.value ?? null);
    
    switch (filter.operator) {
      case '>':
        return compareValue !== null ? value > compareValue : value > Number.MIN_VALUE;
      case '>=':
        return compareValue !== null ? value >= compareValue : value >= Number.MIN_VALUE;
      case '<':
        return compareValue !== null ? value < compareValue : value < Number.MAX_VALUE;
      case '<=':
        return compareValue !== null ? value <= compareValue : value <= Number.MAX_VALUE;
      case '=':
        return compareValue !== null ? value === compareValue : false;
      case '!=':
        return compareValue !== null ? value !== compareValue : true;
      case 'between':
        if (filter.reference) return true;
        if (filter.value === undefined || filter.value2 === undefined) return true;
        const min = Math.min(filter.value, filter.value2);
        const max = Math.max(filter.value, filter.value2);
        return value >= min && value <= max;
      default:
        return true;
    }
  });

  let description: string | undefined;
  // Reuse referenceVal calculated above (or calculate if not already calculated)
  if (filter.reference && referenceVal === null) {
    referenceVal = referenceValue(data[0] || {}, filter, data);
  }
  const displayValue = filter.reference 
    ? `${filter.reference} (${referenceVal !== null ? referenceVal.toFixed(2) : 'N/A'})` 
    : filter.value;
  
  console.log(`   Filter result: ${result.length} rows passed filter (from ${data.length} total)`);
  if (result.length === 0 && data.length > 0) {
    console.warn(`⚠️ Filter removed ALL rows! This might indicate the filter condition is too strict.`);
    console.warn(`   Filter: ${filter.column} ${filter.operator} ${displayValue}`);
    console.warn(`   Sample values from first 5 rows:`, data.slice(0, 5).map(r => ({ [filter.column]: r[filter.column] })));
  }
  
  switch (filter.operator) {
    case '>':
    case '>=':
    case '<':
    case '<=':
    case '!=':
    case '=':
      description = `${filter.column} ${filter.operator} ${displayValue}`;
      break;
    case 'between':
      description = `${filter.column} between ${filter.value} and ${filter.value2}`;
      break;
  }

  return { data: result, description };
}

function referenceValue(
  row: Record<string, any>, 
  filter: ValueFilter, 
  allData?: Record<string, any>[]
): number {
  // If we have allData, calculate statistics from it
  if (allData && filter.reference) {
    const values = allData
      .map((r) => toNumber(r[filter.column]))
      .filter((v) => !isNaN(v));
    
    if (values.length === 0) {
      return filter.value ?? 0;
    }
    
    switch (filter.reference) {
      case 'mean':
      case 'avg':
        return values.reduce((sum, val) => sum + val, 0) / values.length;
      case 'median': {
        const sorted = [...values].sort((a, b) => a - b);
        const mid = Math.floor(sorted.length / 2);
        return sorted.length % 2 === 0
          ? (sorted[mid - 1] + sorted[mid]) / 2
          : sorted[mid];
      }
      case 'min':
        return Math.min(...values);
      case 'max':
        return Math.max(...values);
      case 'p25': {
        const sorted = [...values].sort((a, b) => a - b);
        const idx = Math.floor(sorted.length * 0.25);
        return sorted[idx] ?? sorted[0] ?? 0;
      }
      case 'p75': {
        const sorted = [...values].sort((a, b) => a - b);
        const idx = Math.floor(sorted.length * 0.75);
        return sorted[idx] ?? sorted[sorted.length - 1] ?? 0;
      }
      default:
        return filter.value ?? toNumber(row[filter.column]);
    }
  }
  
  // Fallback to provided value or row value
  return filter.value ?? toNumber(row[filter.column]);
}

function applyExclusions(data: Record<string, any>[], filter: ExclusionFilter): { data: Record<string, any>[]; description?: string } {
  const result = data.filter((row) => !filter.values.includes(row[filter.column]));
  return {
    data: result,
    description: `Excluding ${filter.values.join(', ')} from ${filter.column}`,
  };
}

function applyTopBottom(data: Record<string, any>[], request: TopBottomRequest): { data: Record<string, any>[]; description?: string } {
  const sorted = [...data].sort((a, b) => {
    const aVal = toNumber(a[request.column]);
    const bVal = toNumber(b[request.column]);
    return request.type === 'top' ? bVal - aVal : aVal - bVal;
  });
  return {
    data: sorted.slice(0, request.count),
    description: `${request.type === 'top' ? 'Top' : 'Bottom'} ${request.count} by ${request.column}`,
  };
}

function applySort(data: Record<string, any>[], sort?: SortRequest[]): Record<string, any>[] {
  if (!sort || !sort.length) return data;
  const sorted = [...data];
  sorted.sort((a, b) => {
    for (const spec of sort) {
      const aVal = a[spec.column];
      const bVal = b[spec.column];
      if (aVal === bVal) continue;
      const direction = spec.direction === 'asc' ? 1 : -1;
      if (typeof aVal === 'number' && typeof bVal === 'number') {
        return (aVal - bVal) * direction;
      }
      return String(aVal).localeCompare(String(bVal)) * direction;
    }
    return 0;
  });
  return sorted;
}

function applyAggregations(
  data: Record<string, any>[],
  summary: DataSummary,
  groupBy: string[] = [],
  aggregations: AggregationRequest[] = [],
  dateAggregationPeriod?: DatePeriod | null
): { data: Record<string, any>[]; description?: string } {
  if (!groupBy.length || !aggregations.length) {
    return { data };
  }

  // Map to track which columns are date columns and need normalization
  const dateColumnMap = new Map<string, { original: string; period: DatePeriod }>();
  const normalizedGroupBy: string[] = [];
  
  for (const col of groupBy) {
    const isDateCol = summary.dateColumns.includes(col);
    if (isDateCol && dateAggregationPeriod) {
      // Create a normalized column name for grouping
      const normalizedCol = `${col}_${dateAggregationPeriod}`;
      normalizedGroupBy.push(normalizedCol);
      dateColumnMap.set(normalizedCol, { original: col, period: dateAggregationPeriod });
    } else {
      normalizedGroupBy.push(col);
    }
  }

  const groups = new Map<string, { rows: Record<string, any>[]; displayLabels: Map<string, string> }>();
  
  // Store display labels for normalized dates
  const displayLabelMap = new Map<string, string>();

  const keyForRow = (row: Record<string, any>) => {
    return normalizedGroupBy.map((col) => {
      if (dateColumnMap.has(col)) {
        const { original, period } = dateColumnMap.get(col)!;
        const dateValue = String(row[original]);
        const normalized = normalizeDateToPeriod(dateValue, period);
        if (normalized) {
          // Store display label for this normalized key
          displayLabelMap.set(normalized.normalizedKey, normalized.displayLabel);
          return normalized.normalizedKey;
        }
        return dateValue;
      }
      return String(row[col]);
    }).join('||');
  };

  for (const row of data) {
    const key = keyForRow(row);
    if (!groups.has(key)) {
      groups.set(key, { rows: [], displayLabels: new Map() });
    }
    groups.get(key)!.rows.push(row);
  }

  // For percent_change, we need to calculate across groups, so handle it separately
  const hasPercentChange = aggregations.some(agg => agg.operation === 'percent_change');
  
  let aggregatedRows: Record<string, any>[] = [];
  
  if (hasPercentChange && groupBy.length === 1) {
    // For percent_change, we need to sort by the groupBy column and calculate changes
    // First, create regular aggregated rows
    for (const [key, { rows }] of Array.from(groups.entries())) {
      const base: Record<string, any> = {};
      const keyParts = key.split('||');
      normalizedGroupBy.forEach((col, idx) => {
        if (dateColumnMap.has(col)) {
          // Use display label for normalized date columns
          const originalCol = dateColumnMap.get(col)!.original;
          const normalizedKey = keyParts[idx];
          base[originalCol] = displayLabelMap.get(normalizedKey) || normalizedKey;
        } else {
          base[col] = keyParts[idx];
        }
      });

      for (const agg of aggregations) {
        if (agg.operation === 'percent_change') {
          // Skip percent_change for now, will calculate after sorting
          continue;
        }
        const values = rows.map((r: Record<string, any>) => toNumber(r[agg.column])).filter((v: number) => !isNaN(v));
        let resultValue: number | null = null;
        // TypeScript: percent_change is already filtered out above, so we can safely narrow the type
        const op = agg.operation as Exclude<AggregationOperation, 'percent_change'>;
        switch (op) {
          case 'sum':
            resultValue = values.reduce((sum, val) => sum + val, 0);
            break;
          case 'mean':
          case 'avg':
            resultValue = values.reduce((sum, val) => sum + val, 0) / (values.length || 1);
            break;
          case 'count':
            resultValue = values.length;
            break;
          case 'min':
            resultValue = values.length ? Math.min(...values) : null;
            break;
          case 'max':
            resultValue = values.length ? Math.max(...values) : null;
            break;
          case 'median':
            if (values.length) {
              const sortedVals = [...values].sort((a, b) => a - b);
              const mid = Math.floor(sortedVals.length / 2);
              resultValue =
                sortedVals.length % 2 === 0
                  ? (sortedVals[mid - 1] + sortedVals[mid]) / 2
                  : sortedVals[mid];
            }
            break;
        }
        const targetName = agg.alias || `${agg.column}_${agg.operation}`;
        base[targetName] = resultValue;
      }

      aggregatedRows.push(base);
    }
    
    // Sort by groupBy column for percent_change calculation
    const groupByCol = normalizedGroupBy[0];
    const originalGroupByCol = dateColumnMap.has(groupByCol) ? dateColumnMap.get(groupByCol)!.original : groupByCol;
    aggregatedRows.sort((a, b) => {
      const aVal = a[originalGroupByCol];
      const bVal = b[originalGroupByCol];
      // Try to parse as date if possible
      const aDate = parseDate(aVal);
      const bDate = parseDate(bVal);
      if (aDate && bDate) {
        return aDate.getTime() - bDate.getTime();
      }
      // Fallback to string comparison
      return String(aVal).localeCompare(String(bVal));
    });
    
    // Now calculate percent_change for each aggregation
    for (const agg of aggregations) {
      if (agg.operation === 'percent_change') {
        const targetName = agg.alias || `${agg.column}_${agg.operation}`;
        
        for (let i = 0; i < aggregatedRows.length; i++) {
          const currentRow = aggregatedRows[i];
          const previousRow = i > 0 ? aggregatedRows[i - 1] : null;
          
          // Get the current value (might need to aggregate first if not already aggregated)
          let currentValue: number | null = null;
          if (previousRow) {
            // Reconstruct the key to find the group
            const currentKey = normalizedGroupBy.map((col) => {
              if (dateColumnMap.has(col)) {
                const originalCol = dateColumnMap.get(col)!.original;
                const displayValue = currentRow[originalCol];
                // Find the normalized key from display label
                for (const [normKey, displayLabel] of displayLabelMap.entries()) {
                  if (displayLabel === displayValue) {
                    return normKey;
                  }
                }
                return displayValue;
              }
              return String(currentRow[col]);
            }).join('||');
            
            const currentGroupData = groups.get(currentKey);
            const currentGroupRows = currentGroupData?.rows || [];
            const currentValues = currentGroupRows.map((r: Record<string, any>) => toNumber(r[agg.column])).filter((v: number) => !isNaN(v));
            if (currentValues.length > 0) {
              // Use mean for the current group
              currentValue = currentValues.reduce((sum: number, val: number) => sum + val, 0) / currentValues.length;
            }
            
            // Get the previous value
            const previousKey = normalizedGroupBy.map((col) => {
              if (dateColumnMap.has(col)) {
                const originalCol = dateColumnMap.get(col)!.original;
                const displayValue = previousRow[originalCol];
                // Find the normalized key from display label
                for (const [normKey, displayLabel] of displayLabelMap.entries()) {
                  if (displayLabel === displayValue) {
                    return normKey;
                  }
                }
                return displayValue;
              }
              return String(previousRow[col]);
            }).join('||');
            
            const previousGroupData = groups.get(previousKey);
            const previousGroupRows = previousGroupData?.rows || [];
            const previousValues = previousGroupRows.map((r: Record<string, any>) => toNumber(r[agg.column])).filter((v: number) => !isNaN(v));
            if (previousValues.length > 0 && currentValue !== null) {
              const previousValue = previousValues.reduce((sum: number, val: number) => sum + val, 0) / previousValues.length;
              if (previousValue !== 0 && !isNaN(previousValue) && !isNaN(currentValue)) {
                // Calculate percent change: ((current - previous) / previous) * 100
                currentRow[targetName] = ((currentValue - previousValue) / previousValue) * 100;
              } else {
                currentRow[targetName] = null;
              }
            } else {
              currentRow[targetName] = null;
            }
          } else {
            // First row has no previous value
            currentRow[targetName] = null;
          }
        }
      }
    }
  } else {
    // Regular aggregation (no percent_change or multiple groupBy columns)
    for (const [key, { rows }] of Array.from(groups.entries())) {
      const base: Record<string, any> = {};
      const keyParts = key.split('||');
      normalizedGroupBy.forEach((col, idx) => {
        if (dateColumnMap.has(col)) {
          // Use display label for normalized date columns
          const originalCol = dateColumnMap.get(col)!.original;
          const normalizedKey = keyParts[idx];
          base[originalCol] = displayLabelMap.get(normalizedKey) || normalizedKey;
        } else {
          base[col] = keyParts[idx];
        }
      });

      for (const agg of aggregations) {
        const values = rows.map((r: Record<string, any>) => toNumber(r[agg.column])).filter((v: number) => !isNaN(v));
        let resultValue: number | null = null;
        // TypeScript: percent_change is handled separately, so we can safely narrow the type
        const op = agg.operation as Exclude<AggregationOperation, 'percent_change'>;
        switch (op) {
          case 'sum':
            resultValue = values.reduce((sum: number, val: number) => sum + val, 0);
            break;
          case 'mean':
          case 'avg':
            resultValue = values.reduce((sum: number, val: number) => sum + val, 0) / (values.length || 1);
            break;
          case 'count':
            resultValue = values.length;
            break;
          case 'min':
            resultValue = values.length ? Math.min(...values) : null;
            break;
          case 'max':
            resultValue = values.length ? Math.max(...values) : null;
            break;
          case 'median':
            if (values.length) {
              const sortedVals = [...values].sort((a, b) => a - b);
              const mid = Math.floor(sortedVals.length / 2);
              resultValue =
                sortedVals.length % 2 === 0
                  ? (sortedVals[mid - 1] + sortedVals[mid]) / 2
                  : sortedVals[mid];
            }
            break;
        }
        const targetName = agg.alias || `${agg.column}_${agg.operation}`;
        base[targetName] = resultValue;
      }

      aggregatedRows.push(base);
    }
  }

  return {
    data: aggregatedRows,
    description: `Grouped by ${groupBy.join(', ')} with ${aggregations
      .map((agg) => `${agg.operation}(${agg.column})`)
      .join(', ')}`,
  };
}

export function applyQueryTransformations(
  data: Record<string, any>[],
  summary: DataSummary,
  parsed: ParsedQuery
): TransformationResult {
  let workingData = [...data];
  const descriptions: string[] = [];

  if (parsed.timeFilters) {
    for (const filter of parsed.timeFilters) {
      const { data: filtered, description } = applyTimeFilter(workingData, summary, filter);
      workingData = filtered;
      if (description) descriptions.push(description);
    }
  }

  if (parsed.valueFilters) {
    for (const filter of parsed.valueFilters) {
      console.log(`🔍 Applying value filter: ${filter.column} ${filter.operator} ${filter.reference || filter.value}`);
      console.log(`   Data before filter: ${workingData.length} rows`);
      const { data: filtered, description } = applyValueFilter(workingData, filter);
      workingData = filtered;
      console.log(`   Data after filter: ${workingData.length} rows`);
      if (description) descriptions.push(description);
    }
  }

  if (parsed.exclusionFilters) {
    for (const filter of parsed.exclusionFilters) {
      const { data: filtered, description } = applyExclusions(workingData, filter);
      workingData = filtered;
      if (description) descriptions.push(description);
    }
  }

  if (parsed.groupBy && parsed.aggregations && parsed.groupBy.length && parsed.aggregations.length) {
    console.log(`📊 Applying aggregations: groupBy=[${parsed.groupBy.join(', ')}], aggregations=[${parsed.aggregations.map(a => `${a.operation}(${a.column})${a.alias ? ` as ${a.alias}` : ''}`).join(', ')}]`);
    console.log(`   Data before aggregation: ${workingData.length} rows`);
    
    if (workingData.length === 0) {
      console.warn(`⚠️ Cannot aggregate: No data available (filter may have removed all rows)`);
    } else {
      const { data: aggregated, description } = applyAggregations(workingData, summary, parsed.groupBy, parsed.aggregations, parsed.dateAggregationPeriod);
      workingData = aggregated;
      console.log(`   Data after aggregation: ${workingData.length} rows`);
      if (workingData.length > 0) {
        const columns = Object.keys(workingData[0]);
        console.log(`   Aggregated columns: [${columns.join(', ')}]`);
        console.log(`   Sample aggregated row:`, JSON.stringify(workingData[0], null, 2));
      } else {
        console.warn(`⚠️ Aggregation produced 0 rows!`);
      }
      if (description) descriptions.push(description);
    }
  }

  if (parsed.topBottom) {
    const { data: limited, description } = applyTopBottom(workingData, parsed.topBottom);
    workingData = limited;
    if (description) descriptions.push(description);
  }

  workingData = applySort(workingData, parsed.sort || undefined);

  if (parsed.limit && parsed.limit > 0) {
    workingData = workingData.slice(0, parsed.limit);
    descriptions.push(`Limited to ${parsed.limit} rows`);
  }

  return {
    data: workingData,
    descriptions,
  };
}
