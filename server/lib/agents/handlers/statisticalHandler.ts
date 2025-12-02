import { BaseHandler, HandlerContext, HandlerResponse } from './baseHandler.js';
import { AnalysisIntent } from '../intentClassifier.js';
import { findMatchingColumn } from '../utils/columnMatcher.js';
import { ChartSpec, DataSummary } from '../../../shared/schema.js';

/**
 * Statistical Handler
 * Handles queries about max, min, highest, lowest, average, sum, count, etc.
 */
export class StatisticalHandler extends BaseHandler {
  canHandle(intent: AnalysisIntent): boolean {
    return intent.type === 'statistical';
  }

  async handle(intent: AnalysisIntent, context: HandlerContext): Promise<HandlerResponse> {
    console.log('📊 StatisticalHandler processing intent:', intent.type);
    
    const validation = this.validateData(intent, context);
    if (!validation.valid) {
      return this.createErrorResponse(
        validation.errors.join(', '),
        intent,
        validation.suggestions
      );
    }

    const question = intent.originalQuestion || intent.customRequest || '';
    const questionLower = question.toLowerCase();
    
    // Extract target variable
    const targetVariable = intent.targetVariable;
    if (!targetVariable) {
      // Try to extract from question
      const allColumns = context.summary.columns.map(c => c.name);
      for (const col of allColumns) {
        if (questionLower.includes(col.toLowerCase())) {
          const matched = findMatchingColumn(col, allColumns);
          if (matched) {
            return this.analyzeStatisticalQuery(matched, question, context);
          }
        }
      }
      
      return {
        answer: "I need to know which variable you'd like to analyze. For example: 'Which month had the highest revenue?'",
        requiresClarification: true,
      };
    }

    // Find matching column
    const allColumns = context.summary.columns.map(c => c.name);
    const targetCol = findMatchingColumn(targetVariable, allColumns);

    if (!targetCol) {
      const suggestions = this.findSimilarColumns(targetVariable, context.summary);
      return {
        answer: `I couldn't find a column matching "${targetVariable}". ${suggestions.length > 0 ? `Did you mean: ${suggestions.join(', ')}?` : `Available columns: ${allColumns.slice(0, 5).join(', ')}${allColumns.length > 5 ? '...' : ''}`}`,
        requiresClarification: true,
        suggestions,
      };
    }

    return this.analyzeStatisticalQuery(targetCol, question, context);
  }

  private async analyzeStatisticalQuery(
    targetCol: string,
    question: string,
    context: HandlerContext
  ): Promise<HandlerResponse> {
    const questionLower = question.toLowerCase();
    const data = context.data;
    const summary = context.summary;

    // Check if target is numeric
    const isNumeric = summary.numericColumns.includes(targetCol);
    
    // Find date/time column for "which month" queries
    const dateColumn = summary.dateColumns[0] || 
                      findMatchingColumn('Month', summary.columns.map(c => c.name)) ||
                      findMatchingColumn('Date', summary.columns.map(c => c.name)) ||
                      findMatchingColumn('Week', summary.columns.map(c => c.name)) ||
                      null;

    // Handle "which month/row has the highest/max/best" queries
    // "best" in this context means highest/maximum value
    const isBestWorstQuery = questionLower.includes('best') || questionLower.includes('worst');
    const isWhichQuery = questionLower.includes('which') && 
                        (questionLower.includes('month') || questionLower.includes('row') || questionLower.includes('period') || dateColumn);
    const isMaxMinQuery = questionLower.includes('highest') || questionLower.includes('max') || questionLower.includes('maximum') ||
                         questionLower.includes('lowest') || questionLower.includes('min') || questionLower.includes('minimum');
    
    if (isWhichQuery && (isMaxMinQuery || isBestWorstQuery)) {
      
      if (!isNumeric) {
        return {
          answer: `The column "${targetCol}" is not numeric. I can only find the highest/lowest values for numeric columns.`,
          requiresClarification: true,
        };
      }

      // "best" means highest/max, "worst" means lowest/min
      const isMax = questionLower.includes('highest') || questionLower.includes('max') || questionLower.includes('maximum') || 
                   (questionLower.includes('best') && !questionLower.includes('worst'));
      
      // Find the row with max/min value
      let bestRow: Record<string, any> | null = null;
      let bestValue: number | null = null;
      
      for (const row of data) {
        const value = this.parseNumericValue(row[targetCol]);
        if (value !== null && !isNaN(value)) {
          if (bestValue === null || (isMax ? value > bestValue : value < bestValue)) {
            bestValue = value;
            bestRow = row;
          }
        }
      }

      if (!bestRow || bestValue === null) {
        return {
          answer: `I couldn't find any valid numeric values in the "${targetCol}" column.`,
        };
      }

      // Get the identifier (month, date, or row index)
      let identifier = 'that row';
      if (dateColumn && bestRow[dateColumn]) {
        identifier = String(bestRow[dateColumn]);
      } else {
        // Try to find any non-numeric column that could identify the row
        for (const col of summary.columns.map(c => c.name)) {
          if (col !== targetCol && !summary.numericColumns.includes(col) && bestRow[col]) {
            identifier = String(bestRow[col]);
            break;
          }
        }
      }

      const valueStr = isNumeric && bestValue % 1 !== 0 
        ? bestValue.toFixed(2) 
        : String(bestValue);
      
      const answer = `The ${isMax ? 'highest' : 'lowest'} value for ${targetCol} is ${valueStr}, which occurs in ${identifier}.`;

      // Create a chart showing the data point
      const charts: ChartSpec[] = [];
      if (dateColumn) {
        charts.push({
          type: 'line',
          title: `${targetCol} Over Time`,
          x: dateColumn,
          y: targetCol,
          xLabel: dateColumn,
          yLabel: targetCol,
          aggregate: 'none',
        });
      }

      return {
        answer,
        charts: charts.length > 0 ? charts : undefined,
      };
    }

    // Handle other statistical queries (average, sum, count, etc.)
    if (isNumeric) {
      const values = data
        .map(row => this.parseNumericValue(row[targetCol]))
        .filter(v => v !== null && !isNaN(v)) as number[];

      if (values.length === 0) {
        return {
          answer: `I couldn't find any valid numeric values in the "${targetCol}" column.`,
        };
      }

      const sum = values.reduce((a, b) => a + b, 0);
      const avg = sum / values.length;
      const sorted = [...values].sort((a, b) => a - b);
      const median = sorted.length % 2 === 0
        ? (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2
        : sorted[Math.floor(sorted.length / 2)];
      const min = Math.min(...values);
      const max = Math.max(...values);

      let answer = `Here are the statistics for ${targetCol}:\n\n`;
      answer += `- Count: ${values.length} values\n`;
      answer += `- Average: ${avg.toFixed(2)}\n`;
      answer += `- Median: ${median.toFixed(2)}\n`;
      answer += `- Minimum: ${min.toFixed(2)}\n`;
      answer += `- Maximum: ${max.toFixed(2)}\n`;
      answer += `- Sum: ${sum.toFixed(2)}\n`;

      return { answer };
    }

    // Fallback for non-numeric statistical queries
    return {
      answer: `I can provide statistics for numeric columns. The column "${targetCol}" is not numeric. Available numeric columns: ${summary.numericColumns.slice(0, 5).join(', ')}${summary.numericColumns.length > 5 ? '...' : ''}`,
      requiresClarification: true,
    };
  }

  private parseNumericValue(value: any): number | null {
    if (value === null || value === undefined || value === '') return null;
    if (typeof value === 'number') return isNaN(value) ? null : value;
    const cleaned = String(value).replace(/[%,]/g, '').trim();
    const parsed = Number(cleaned);
    return isNaN(parsed) ? null : parsed;
  }

  protected findSimilarColumns(searchName: string, summary: DataSummary): string[] {
    const allColumns = summary.columns.map(c => c.name);
    const suggestions: string[] = [];
    const searchLower = searchName.toLowerCase();

    for (const col of allColumns) {
      const colLower = col.toLowerCase();
      if (colLower.includes(searchLower) || searchLower.includes(colLower)) {
        suggestions.push(col);
        if (suggestions.length >= 5) break;
      }
    }

    return suggestions;
  }

  protected createErrorResponse(
    error: Error | string,
    intent: AnalysisIntent,
    suggestions?: string[]
  ): HandlerResponse {
    const errorMessage = error instanceof Error ? error.message : error;
    return {
      answer: `I encountered an issue analyzing "${intent.targetVariable || 'the data'}": ${errorMessage}. ${suggestions && suggestions.length > 0 ? `Did you mean: ${suggestions.join(', ')}?` : ''}`,
      error: errorMessage,
      requiresClarification: true,
      suggestions,
    };
  }
}

