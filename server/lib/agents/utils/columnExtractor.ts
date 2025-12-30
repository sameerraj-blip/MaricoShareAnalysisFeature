import { AnalysisIntent } from '../intentClassifier.js';
import { ChartSpec, DataSummary } from '../../../shared/schema.js';
import { ParsedQuery } from '../../../shared/queryTypes.js';

/**
 * Extract mentioned column names from question text
 */
function extractMentionedColumns(question: string, summary: DataSummary): string[] {
  const mentioned: string[] = [];
  const questionLower = question.toLowerCase();
  
  for (const col of summary.columns) {
    const colLower = col.name.toLowerCase();
    // Check if column name appears in question
    if (questionLower.includes(colLower) || colLower.includes(questionLower.split(' ')[0])) {
      mentioned.push(col.name);
    }
  }
  
  return mentioned;
}

/**
 * Extract all required columns from a query based on:
 * - Question text (mentioned columns)
 * - Intent (targetVariable, variables)
 * - Parsed query (filters, aggregations, groupBy, sort)
 * - Chart specifications (x, y, y2 columns)
 * - Data summary (always include date columns for time-based queries)
 */
export function extractRequiredColumns(
  question: string,
  intent: AnalysisIntent,
  parsedQuery: ParsedQuery | null,
  chartSpecs: ChartSpec[] | null,
  summary: DataSummary
): string[] {
  const columns = new Set<string>();
  
  // 1. Extract from question text (mentioned columns)
  const mentionedColumns = extractMentionedColumns(question, summary);
  mentionedColumns.forEach(col => columns.add(col));
  
  // 2. Extract from intent
  if (intent.targetVariable) {
    columns.add(intent.targetVariable);
  }
  if (intent.variables && intent.variables.length > 0) {
    intent.variables.forEach(v => columns.add(v));
  }
  
  // 3. Extract from parsed query
  if (parsedQuery) {
    // Value filters
    if (parsedQuery.valueFilters) {
      parsedQuery.valueFilters.forEach(f => columns.add(f.column));
    }
    
    // Time filters
    if (parsedQuery.timeFilters) {
      parsedQuery.timeFilters.forEach(f => {
        if (f.column) {
          columns.add(f.column);
        }
      });
    }
    
    // Exclusion filters
    if (parsedQuery.exclusionFilters) {
      parsedQuery.exclusionFilters.forEach(f => columns.add(f.column));
    }
    
    // Group by columns
    if (parsedQuery.groupBy) {
      parsedQuery.groupBy.forEach(c => columns.add(c));
    }
    
    // Aggregation columns
    if (parsedQuery.aggregations) {
      parsedQuery.aggregations.forEach(a => columns.add(a.column));
    }
    
    // Sort columns
    if (parsedQuery.sort) {
      parsedQuery.sort.forEach(s => columns.add(s.column));
    }
    
    // Top/bottom columns
    if (parsedQuery.topBottom) {
      columns.add(parsedQuery.topBottom.column);
    }
    
    // Variables
    if (parsedQuery.variables) {
      parsedQuery.variables.forEach(v => columns.add(v));
    }
    
    if (parsedQuery.secondaryVariables) {
      parsedQuery.secondaryVariables.forEach(v => columns.add(v));
    }
  }
  
  // 4. Extract from chart specifications
  if (chartSpecs && chartSpecs.length > 0) {
    chartSpecs.forEach(spec => {
      if (spec.x) columns.add(spec.x);
      if (spec.y) columns.add(spec.y);
      if (spec.y2) columns.add(spec.y2);
      if (spec.y2Series && spec.y2Series.length > 0) {
        spec.y2Series.forEach(col => columns.add(col));
      }
    });
  }
  
  // 5. Always include date columns (needed for time-based queries and aggregations)
  // This ensures time-based operations work correctly
  summary.dateColumns.forEach(c => columns.add(c));
  
  // 6. If no specific columns found, include all numeric columns as fallback
  // This ensures basic analysis can still work
  if (columns.size === 0 && summary.numericColumns.length > 0) {
    // Only add first few numeric columns as fallback to avoid loading everything
    summary.numericColumns.slice(0, 5).forEach(c => columns.add(c));
  }
  
  return Array.from(columns);
}

/**
 * Extract required columns from chat history (previous charts)
 * Useful for follow-up queries that reference previous visualizations
 */
export function extractColumnsFromHistory(
  chatHistory: Array<{ charts?: ChartSpec[] }>,
  summary: DataSummary
): string[] {
  const columns = new Set<string>();
  
  // Look through recent messages for chart specs
  for (let i = chatHistory.length - 1; i >= 0 && i >= chatHistory.length - 5; i--) {
    const msg = chatHistory[i];
    if (msg.charts && msg.charts.length > 0) {
      msg.charts.forEach(spec => {
        if (spec.x) columns.add(spec.x);
        if (spec.y) columns.add(spec.y);
        if (spec.y2) columns.add(spec.y2);
        if (spec.y2Series && spec.y2Series.length > 0) {
          spec.y2Series.forEach(col => columns.add(col));
        }
      });
    }
  }
  
  return Array.from(columns);
}

