import { BaseHandler, HandlerContext, HandlerResponse } from './baseHandler.js';
import { AnalysisIntent } from '../intentClassifier.js';
import { findMatchingColumn } from '../utils/columnMatcher.js';
import { ChartSpec, DataSummary } from '../../../shared/schema.js';
import { extractColumnsFromMessage } from '../../columnExtractor.js';

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
    
    // Check if this is an aggregation query with category filter - handle immediately
    if (this.isAggregationWithCategoryRequest(question)) {
      console.log('📊 Detected aggregation with category request in StatisticalHandler');
      return this.handleAggregationWithCategory(intent, context, question);
    }
    
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

  /**
   * Check if question is asking for aggregation with category filter
   */
  private isAggregationWithCategoryRequest(question: string): boolean {
    const lower = question.toLowerCase();
    // Pattern 1: "sum of all the value" (with optional "of column name [column]") "for [category] category"
    // Note: [\w\s']+ includes apostrophes for categories like "men's fashion"
    if (/\bsum\s+of\s+(?:all\s+the\s+)?(?:value|values?)(?:\s+of\s+(?:column\s+name\s+)?[\w\s']+)?\s+for\s+[\w\s']+(?:\s+category)?/i.test(lower)) {
      return true;
    }
    // Pattern 2: "sum of [column] for [category]"
    if (/\bsum\s+of\s+[\w\s']+\s+for\s+[\w\s']+(?:\s+category)?/i.test(lower)) {
      return true;
    }
    // Pattern 3: Standard aggregation patterns (include apostrophes)
    return /\b(aggregated?\s+(?:column\s+name\s+)?value|aggregate|total|sum)\s+(?:for|of|in)\s+(?:the\s+)?(?:column\s+)?(?:category\s+)?[\w\s']+/i.test(lower) ||
           /\b(?:what\s+is\s+)?(?:the\s+)?(?:aggregated?\s+(?:column\s+name\s+)?value|total|sum)\s+(?:for|of|in)\s+(?:the\s+)?(?:column\s+)?(?:category\s+)?[\w\s']+/i.test(lower) ||
           /\b(?:aggregated?\s+value|aggregate|total|sum)\s+(?:for|of|in)\s+(?:the\s+)?column\s+category\s+[\w\s']+/i.test(lower);
  }

  /**
   * Handle aggregation queries with category filters
   * STRICT IMPLEMENTATION: No fallbacks, explicit validation, dataset-aware resolution
   * Same implementation as GeneralHandler for consistency
   */
  private async handleAggregationWithCategory(
    intent: AnalysisIntent,
    context: HandlerContext,
    question: string
  ): Promise<HandlerResponse> {
    // DIAGNOSTIC LOGGING: Start
    console.log(`📊 [AGGREGATION] Processing query: "${question}"`);
    
    const allColumns = context.summary.columns.map(c => c.name);
    const numericColumns = context.summary.numericColumns || [];
    const data = context.data;
    
    // ============================================================================
    // STEP 1: HARD VALIDATION - Require numeric column, categorical column, filter value
    // ============================================================================
    
    if (numericColumns.length === 0) {
      const error = 'No numeric columns found in dataset. Cannot perform aggregation.';
      console.error(`❌ [AGGREGATION] ${error}`);
      return {
        answer: `Error: ${error} Available columns: ${allColumns.join(', ')}`,
        error,
        requiresClarification: true,
      };
    }
    
    if (data.length === 0) {
      const error = 'Dataset is empty. Cannot perform aggregation.';
      console.error(`❌ [AGGREGATION] ${error}`);
      return {
        answer: `Error: ${error}`,
        error,
        requiresClarification: true,
      };
    }
    
    // ============================================================================
    // STEP 2: EXTRACT CATEGORY VALUE FROM QUESTION
    // ============================================================================
    
    let categoryValue: string | null = null;
    
    // Pattern 0: "sum of all the value" (with optional "of column name [column]") "for X category"
    const sumForCategoryMatch = question.match(/\bsum\s+of\s+(?:all\s+the\s+)?(?:value|values?)(?:\s+of\s+(?:column\s+name\s+)?[\w\s']+)?\s+for\s+(.+?)(?:\s+category|\s*$)/i);
    if (sumForCategoryMatch && sumForCategoryMatch[1]) {
      categoryValue = sumForCategoryMatch[1].trim().replace(/[.,;:!?]+$/, '').trim();
      console.log(`   ✅ [AGGREGATION] Extracted category (pattern 0): "${categoryValue}"`);
    } else {
      // Pattern 1: "for the column category X"
      const columnCategoryMatch = question.match(/(?:for|of|in)\s+(?:the\s+)?column\s+category\s+(.+?)(?:\s*$|\s+(?:which|that|where|when|what|how|can|will|should|is|are|was|were))/i);
      if (columnCategoryMatch && columnCategoryMatch[1]) {
        categoryValue = columnCategoryMatch[1].trim().replace(/[.,;:!?]+$/, '').trim();
        console.log(`   ✅ [AGGREGATION] Extracted category (pattern 1): "${categoryValue}"`);
      } else {
        // Pattern 2: "for category X" or "for the category X"
        const categoryMatch = question.match(/(?:for|of|in)\s+(?:the\s+)?category\s+(.+?)(?:\s*$|\s+(?:which|that|where|when|what|how|can|will|should|is|are|was|were|category|column|in|for|of))/i);
        if (categoryMatch && categoryMatch[1]) {
          categoryValue = categoryMatch[1].trim().replace(/[.,;:!?]+$/, '').trim();
          console.log(`   ✅ [AGGREGATION] Extracted category (pattern 2): "${categoryValue}"`);
        } else {
          // Pattern 3: "for X" (fallback)
          const forMatch = question.match(/(?:for|of|in)\s+(?:the\s+)?(?:column\s+)?(?:category\s+)?(.+?)(?:\s*$|\s+(?:which|that|where|when|what|how|can|will|should|is|are|was|were|category|column|in|for|of|over|across|by))/i);
          if (forMatch && forMatch[1]) {
            categoryValue = forMatch[1].trim().replace(/[.,;:!?]+$/, '').trim();
            console.log(`   ✅ [AGGREGATION] Extracted category (pattern 3): "${categoryValue}"`);
          }
        }
      }
    }
    
    // HARD VALIDATION: Category value must be extracted
    if (!categoryValue || categoryValue.trim().length === 0) {
      const error = 'Could not extract category value from question';
      console.error(`❌ [AGGREGATION] ${error}`);
      return {
        answer: `Error: ${error}. Please specify a category, for example: "What is the sum of value for men's fashion category?"`,
        error,
        requiresClarification: true,
      };
    }
    
    // ============================================================================
    // STEP 3: FIND CATEGORY COLUMN (DATASET-AWARE)
    // ============================================================================
    
    let categoryColumn: string | null = null;
    const categoryColumnNames = ['category', 'Category', 'product_category', 'Product Category', 'product_category_name', 'cat'];
    
    // First try common category column names
    for (const colName of categoryColumnNames) {
      const matched = findMatchingColumn(colName, allColumns);
      if (matched) {
        categoryColumn = matched;
        break;
      }
    }
    
    // If not found, search dataset for columns containing the category value
    if (!categoryColumn) {
      const normalizedCategoryValue = categoryValue.toLowerCase().trim();
      const searchSampleSize = Math.min(data.length, 10000);
      const searchData = data.slice(0, searchSampleSize);
      
      const columnMatches: Array<{ column: string; score: number; exactMatches: number }> = [];
      
      for (const col of allColumns) {
        if (!numericColumns.includes(col) && !context.summary.dateColumns.includes(col)) {
          let exactMatches = 0;
          
          // Get unique values from this column
          const uniqueValues = new Set<string>();
          for (const row of searchData) {
            const val = String(row[col] || '').toLowerCase().trim();
            if (val) uniqueValues.add(val);
          }
          
          // Check for exact match in unique values
          for (const uniqueVal of uniqueValues) {
            if (uniqueVal === normalizedCategoryValue) {
              exactMatches++;
            }
          }
          
          if (exactMatches > 0) {
            columnMatches.push({ column: col, score: exactMatches * 100, exactMatches });
          }
        }
      }
      
      if (columnMatches.length > 0) {
        columnMatches.sort((a, b) => b.score - a.score);
        categoryColumn = columnMatches[0].column;
        console.log(`   ✅ [AGGREGATION] Found category column by dataset search: "${categoryColumn}"`);
      }
    }
    
    // HARD VALIDATION: Category column must be found
    if (!categoryColumn) {
      const error = `Could not find category column in dataset`;
      console.error(`❌ [AGGREGATION] ${error}. Available columns: ${allColumns.join(', ')}`);
      return {
        answer: `Error: ${error}. Available columns: ${allColumns.join(', ')}`,
        error,
        requiresClarification: true,
      };
    }
    
    // ============================================================================
    // STEP 4: DATASET-AWARE CATEGORY VALUE RESOLUTION
    // ============================================================================
    
    // Get all unique values from the category column (normalized)
    const uniqueCategoryValues = new Set<string>();
    for (const row of data) {
      const val = String(row[categoryColumn] || '').toLowerCase().trim();
      if (val) uniqueCategoryValues.add(val);
    }
    
    // Normalize the extracted category value
    const normalizedCategoryValue = categoryValue.toLowerCase().trim();
    
    // Find exact match in dataset values
    let resolvedCategoryValue: string | null = null;
    for (const datasetValue of uniqueCategoryValues) {
      if (datasetValue === normalizedCategoryValue) {
        // Find the original case version from dataset
        for (const row of data) {
          const val = String(row[categoryColumn] || '');
          if (val.toLowerCase().trim() === normalizedCategoryValue) {
            resolvedCategoryValue = val.trim();
            break;
          }
        }
        break;
      }
    }
    
    // HARD VALIDATION: Category value must exist in dataset
    if (!resolvedCategoryValue) {
      const availableCategories = Array.from(uniqueCategoryValues).slice(0, 20);
      const error = `No rows found for category = "${categoryValue}"`;
      console.error(`❌ [AGGREGATION] ${error}. Available categories: ${availableCategories.join(', ')}${uniqueCategoryValues.size > 20 ? '...' : ''}`);
      return {
        answer: `Error: ${error}. Available categories in "${categoryColumn}": ${availableCategories.join(', ')}${uniqueCategoryValues.size > 20 ? '...' : ''}`,
        error,
        requiresClarification: true,
      };
    }
    
    // ============================================================================
    // STEP 5: RESOLVE NUMERIC COLUMN TO AGGREGATE
    // ============================================================================
    
    let aggregateColumn: string | null = null;
    
    // Step 5.1: Check for explicit "column name value" pattern
    const columnNameValueMatch = question.match(/(?:column\s+name|column)\s+(\w+)/i);
    if (columnNameValueMatch && columnNameValueMatch[1]) {
      const explicitColumn = columnNameValueMatch[1].trim();
      const matchedColumn = findMatchingColumn(explicitColumn, allColumns);
      if (matchedColumn && numericColumns.includes(matchedColumn)) {
        aggregateColumn = matchedColumn;
        console.log(`   ✅ [AGGREGATION] Using explicit column from "column name ${explicitColumn}": "${aggregateColumn}"`);
      }
    }
    
    // Step 5.2: Extract mentioned columns
    if (!aggregateColumn) {
      const mentionedColumns = extractColumnsFromMessage(question, allColumns);
      for (const mentionedCol of mentionedColumns) {
        if (numericColumns.includes(mentionedCol)) {
          aggregateColumn = mentionedCol;
          console.log(`   ✅ [AGGREGATION] Using mentioned numeric column: "${aggregateColumn}"`);
          break;
        }
      }
    }
    
    // Step 5.3: If user says "sum of all the value", prioritize "value" column
    if (!aggregateColumn && /\bsum\s+of\s+(?:all\s+the\s+)?value/i.test(question)) {
      const valueColumn = findMatchingColumn('value', numericColumns);
      if (valueColumn) {
        aggregateColumn = valueColumn;
        console.log(`   ✅ [AGGREGATION] Using "value" column from pattern: "${aggregateColumn}"`);
      }
    }
    
    // Step 5.4: Fallback to preferred columns
    if (!aggregateColumn) {
      const preferredColumns = ['total', 'qty_ordered', 'price', 'amount', 'value', 'revenue', 'sales'];
      for (const prefCol of preferredColumns) {
        const matched = findMatchingColumn(prefCol, numericColumns);
        if (matched) {
          aggregateColumn = matched;
          console.log(`   ✅ [AGGREGATION] Using preferred column: "${aggregateColumn}"`);
          break;
        }
      }
    }
    
    // Step 5.5: Final fallback to first numeric column
    if (!aggregateColumn && numericColumns.length > 0) {
      aggregateColumn = numericColumns[0];
      console.log(`   ✅ [AGGREGATION] Using first numeric column: "${aggregateColumn}"`);
    }
    
    // HARD VALIDATION: Aggregate column must be found
    if (!aggregateColumn) {
      const error = 'Could not determine which numeric column to aggregate';
      console.error(`❌ [AGGREGATION] ${error}`);
      return {
        answer: `Error: ${error}. Available numeric columns: ${numericColumns.join(', ')}`,
        error,
        requiresClarification: true,
      };
    }
    
    // DIAGNOSTIC LOGGING: Resolved columns and filter
    console.log(`📊 [AGGREGATION] Resolved:`);
    console.log(`   - Numeric column: "${aggregateColumn}"`);
    console.log(`   - Category column: "${categoryColumn}"`);
    console.log(`   - Filter value: "${resolvedCategoryValue}"`);
    
    // ============================================================================
    // STEP 6: NORMALIZE AND FILTER ROWS
    // ============================================================================
    
    const normalizedFilterValue = resolvedCategoryValue.toLowerCase().trim();
    const filteredRows: Record<string, any>[] = [];
    
    for (const row of data) {
      const rowCategoryValue = String(row[categoryColumn] || '').toLowerCase().trim();
      if (rowCategoryValue === normalizedFilterValue) {
        filteredRows.push(row);
      }
    }
    
    // DIAGNOSTIC LOGGING: Filtered row count
    console.log(`📊 [AGGREGATION] Filtered rows: ${filteredRows.length} out of ${data.length}`);
    
    // HARD VALIDATION: Must have filtered rows
    if (filteredRows.length === 0) {
      const error = `No rows found for category = "${resolvedCategoryValue}"`;
      console.error(`❌ [AGGREGATION] ${error}`);
      return {
        answer: `Error: ${error}. Please check the category value.`,
        error,
        requiresClarification: true,
      };
    }
    
    // ============================================================================
    // STEP 7: EXPLICIT AGGREGATION EXECUTION
    // ============================================================================
    
    // Extract numeric values (normalized and validated)
    const numericValues: number[] = [];
    for (const row of filteredRows) {
      const val = row[aggregateColumn];
      if (val === null || val === undefined || val === '') continue;
      
      let numValue: number | null = null;
      if (typeof val === 'number') {
        numValue = isNaN(val) ? null : val;
      } else {
        const cleaned = String(val).replace(/[%,$]/g, '').trim();
        const parsed = Number(cleaned);
        numValue = isNaN(parsed) ? null : parsed;
      }
      
      if (numValue !== null) {
        numericValues.push(numValue);
      }
    }
    
    // HARD VALIDATION: Must have numeric values
    if (numericValues.length === 0) {
      const error = `No numeric values found in column "${aggregateColumn}" for category "${resolvedCategoryValue}"`;
      console.error(`❌ [AGGREGATION] ${error}`);
      return {
        answer: `Error: ${error}`,
        error,
        requiresClarification: true,
      };
    }
    
    // Execute aggregation operations
    const sum = numericValues.reduce((a, b) => a + b, 0);
    const count = numericValues.length;
    const avg = sum / count;
    const min = Math.min(...numericValues);
    const max = Math.max(...numericValues);
    
    // ============================================================================
    // STEP 8: RESPONSE CONTRACT - Must contain computed result
    // ============================================================================
    
    const answer = `The sum of "${aggregateColumn}" for category "${resolvedCategoryValue}" is ${sum.toFixed(2)}.\n\n` +
      `Aggregation details:\n` +
      `- Column aggregated: ${aggregateColumn}\n` +
      `- Category filter: ${categoryColumn} = "${resolvedCategoryValue}"\n` +
      `- Rows filtered: ${filteredRows.length}\n` +
      `- Sum: ${sum.toFixed(2)}\n` +
      `- Average: ${avg.toFixed(2)}\n` +
      `- Minimum: ${min.toFixed(2)}\n` +
      `- Maximum: ${max.toFixed(2)}\n` +
      `- Count: ${count} records`;
    
    console.log(`✅ [AGGREGATION] Success: Sum = ${sum.toFixed(2)}, Count = ${count}`);
    
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

