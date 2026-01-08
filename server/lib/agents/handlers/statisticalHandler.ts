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
    
    // Check if this is a value lookup request (e.g., "What is the value of X on Y date?")
    if (this.isValueLookupRequest(question)) {
      console.log('🔍 Detected value lookup request in StatisticalHandler');
      return this.handleValueLookup(intent, context, question);
    }
    
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

  /**
   * Check if question is asking for a specific value lookup
   * Examples: "What is the value of X on Y?", "What is X on date Y?", "Show me X for Y"
   */
  private isValueLookupRequest(question: string): boolean {
    const lower = question.toLowerCase();
    // Patterns that indicate a specific value lookup
    const lookupPatterns = [
      /what\s+is\s+(?:the\s+)?(?:value\s+of\s+)?[\w\s]+\s+(?:on|for|in)\s+/i,
      /what\s+is\s+[\w\s]+\s+(?:on|for|in)\s+[\d\w\s-]+/i,
      /show\s+me\s+[\w\s]+\s+(?:on|for|in)\s+[\d\w\s-]+/i,
      /(?:get|find|retrieve)\s+[\w\s]+\s+(?:on|for|in)\s+[\d\w\s-]+/i,
    ];
    
    return lookupPatterns.some(pattern => pattern.test(lower));
  }

  /**
   * Handle specific value lookup requests
   * Examples: "What is the value of qty_ordered on 11-2020"
   */
  private async handleValueLookup(
    intent: AnalysisIntent,
    context: HandlerContext,
    question: string
  ): Promise<HandlerResponse> {
    const allColumns = context.summary.columns.map(c => c.name);
    const numericColumns = context.summary.numericColumns || [];
    const dateColumns = context.summary.dateColumns || [];
    const data = context.data;
    
    // Extract the column name from the question
    let targetColumn: string | null = null;
    const questionLower = question.toLowerCase();
    
    // Try to find the column mentioned in the question
    for (const col of allColumns) {
      const colLower = col.toLowerCase();
      // Check if column name appears in question (before "on", "for", "in")
      const beforeDatePattern = new RegExp(`\\b${colLower.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s+(?:on|for|in)\\s+`, 'i');
      if (beforeDatePattern.test(questionLower) || questionLower.includes(`value of ${colLower}`) || questionLower.includes(`value of ${col}`)) {
        targetColumn = col;
        break;
      }
    }
    
    // If not found, try to extract from intent
    if (!targetColumn && intent.targetVariable) {
      targetColumn = findMatchingColumn(intent.targetVariable, allColumns);
    }
    
    // If still not found, try to extract from variables array
    if (!targetColumn && intent.variables && intent.variables.length > 0) {
      targetColumn = findMatchingColumn(intent.variables[0], allColumns);
    }
    
    if (!targetColumn) {
      return {
        answer: 'I couldn\'t identify which column you\'re asking about. Please specify the column name, for example: "What is the value of qty_ordered on 11-2020?"',
        requiresClarification: true,
        suggestions: numericColumns.slice(0, 5),
      };
    }
    
    // Find date column
    const dateColumn = dateColumns[0] || 
      findMatchingColumn('order_date', allColumns) ||
      findMatchingColumn('date', allColumns) ||
      findMatchingColumn('Month', allColumns) ||
      findMatchingColumn('Date', allColumns);
    
    if (!dateColumn) {
      return {
        answer: 'I couldn\'t find a date column in your dataset. Please ensure your dataset has a date/time column.',
        requiresClarification: true,
      };
    }
    
    // Extract date from question using date parsing
    const { parseFlexibleDate } = await import('../../dateUtils.js');
    
    // Try to extract date from question - look for patterns like "11-2020", "Nov 2020", "November 2020", etc.
    const datePatterns = [
      /\b(\d{1,2})[-/](\d{2,4})\b/, // 11-2020, 11/2020
      /\b([A-Za-z]{3,})\s+(\d{2,4})\b/i, // Nov 2020, November 2020
      /\b(\d{4})[-/](\d{1,2})\b/, // 2020-11
    ];
    
    let targetDate: Date | null = null;
    let dateStr: string | null = null;
    
    for (const pattern of datePatterns) {
      const match = question.match(pattern);
      if (match) {
        dateStr = match[0];
        targetDate = parseFlexibleDate(dateStr);
        if (targetDate) {
          break;
        }
      }
    }
    
    // Also try parsing the entire question for dates
    if (!targetDate) {
      // Look for any date-like string in the question
      const words = question.split(/\s+/);
      for (const word of words) {
        const parsed = parseFlexibleDate(word);
        if (parsed) {
          targetDate = parsed;
          dateStr = word;
          break;
        }
      }
    }
    
    if (!targetDate || !dateStr) {
      return {
        answer: `I couldn't identify the date in your question. Please specify a date, for example: "What is the value of ${targetColumn} on 11-2020?" or "What is the value of ${targetColumn} on November 2020?"`,
        requiresClarification: true,
      };
    }
    
    console.log(`🔍 Looking up ${targetColumn} for date: ${dateStr} (parsed as ${targetDate.toISOString()})`);
    
    // Normalize the target date to month-year for matching
    const targetMonth = targetDate.getMonth();
    const targetYear = targetDate.getFullYear();
    
    // Filter data to find matching rows
    const matchingRows: Record<string, any>[] = [];
    
    for (const row of data) {
      const rowDateStr = String(row[dateColumn] || '');
      if (!rowDateStr) continue;
      
      const rowDate = parseFlexibleDate(rowDateStr);
      if (rowDate) {
        // Check if it matches the target month and year
        if (rowDate.getMonth() === targetMonth && rowDate.getFullYear() === targetYear) {
          matchingRows.push(row);
        }
      }
    }
    
    if (matchingRows.length === 0) {
      return {
        answer: `I couldn't find any data for ${targetColumn} in ${dateStr}. Please check that the date exists in your dataset.`,
        requiresClarification: true,
      };
    }
    
    // Get the value(s) from matching rows
    const values = matchingRows
      .map(row => {
        const val = row[targetColumn];
        if (val === null || val === undefined || val === '') return null;
        // Try to parse as number
        if (typeof val === 'number') return isNaN(val) ? null : val;
        const cleaned = String(val).replace(/[%,]/g, '').trim();
        const parsed = Number(cleaned);
        return isNaN(parsed) ? val : parsed; // Return original if not numeric
      })
      .filter(v => v !== null);
    
    if (values.length === 0) {
      return {
        answer: `I found rows for ${dateStr}, but the ${targetColumn} column has no values for those rows.`,
        requiresClarification: true,
      };
    }
    
    // Format the answer
    let answer: string;
    if (values.length === 1) {
      const value = values[0];
      const valueStr = typeof value === 'number' && value % 1 !== 0 
        ? value.toFixed(2) 
        : String(value);
      answer = `The value of ${targetColumn} on ${dateStr} is ${valueStr}.`;
    } else {
      // Multiple rows found - show summary
      const numericValues = values.filter(v => typeof v === 'number') as number[];
      if (numericValues.length > 0) {
        const sum = numericValues.reduce((a, b) => a + b, 0);
        const avg = sum / numericValues.length;
        const min = Math.min(...numericValues);
        const max = Math.max(...numericValues);
        
        answer = `I found ${values.length} records for ${targetColumn} in ${dateStr}:\n\n`;
        answer += `- Count: ${values.length} records\n`;
        answer += `- Average: ${avg.toFixed(2)}\n`;
        answer += `- Minimum: ${min.toFixed(2)}\n`;
        answer += `- Maximum: ${max.toFixed(2)}\n`;
        if (numericValues.length === values.length) {
          answer += `- Sum: ${sum.toFixed(2)}\n`;
        }
      } else {
        // Non-numeric values
        answer = `I found ${values.length} records for ${targetColumn} in ${dateStr}:\n\n`;
        const uniqueValues = Array.from(new Set(values.map(v => String(v))));
        answer += `Values: ${uniqueValues.slice(0, 10).join(', ')}${uniqueValues.length > 10 ? '...' : ''}`;
      }
    }
    
    return {
      answer,
    };
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

