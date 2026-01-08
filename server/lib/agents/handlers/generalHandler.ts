import { BaseHandler, HandlerContext, HandlerResponse } from './baseHandler.js';
import { AnalysisIntent } from '../intentClassifier.js';
import { generateGeneralAnswer } from '../../dataAnalyzer.js';
import type { ChartSpec } from '../../../shared/schema.js';
import { findMatchingColumn } from '../utils/columnMatcher.js';
import { processChartData } from '../../chartGenerator.js';
import { generateChartInsights } from '../../insightGenerator.js';
import { calculateSmartDomainsForChart } from '../../axisScaling.js';

/**
 * General Handler
 * Handles general queries that don't fit specific categories
 * Uses the existing generateGeneralAnswer function for now
 */
export class GeneralHandler extends BaseHandler {
  canHandle(intent: AnalysisIntent): boolean {
    // General handler can handle chart, statistical, comparison, and custom types
    return ['chart', 'statistical', 'comparison', 'custom'].includes(intent.type);
  }

  async handle(intent: AnalysisIntent, context: HandlerContext): Promise<HandlerResponse> {
    // Validate data
    const validation = this.validateData(intent, context);
    if (!validation.valid && validation.errors.length > 0) {
      // For general queries, we might still proceed with warnings
      console.log('⚠️ Validation warnings:', validation.warnings);
      if (validation.errors.some(e => e.includes('not found'))) {
        return this.createErrorResponse(
          validation.errors.join(', '),
          intent,
          validation.suggestions
        );
      }
    }

    // Build question from intent
    let question = intent.customRequest || intent.originalQuestion || '';
    
    // Check if user explicitly requested charts
    const wantsCharts = this.isExplicitChartRequest(question);
    
    // Check if this is an aggregation query with category filter
    if (this.isAggregationWithCategoryRequest(question)) {
      console.log('📊 Detected aggregation with category request');
      return this.handleAggregationWithCategory(intent, context, question);
    }
    
    // Check if this is a specific value lookup (e.g., "What is the value of X on Y date?")
    if (this.isValueLookupRequest(question)) {
      console.log('🔍 Detected value lookup request');
      return this.handleValueLookup(intent, context, question);
    }
    
    // Check if this is a seasonal pattern question
    if (this.isSeasonalPatternRequest(question)) {
      console.log('📅 Detected seasonal pattern request, creating seasonal trend chart');
      return this.handleSeasonalPatterns(intent, context, question);
    }
    
    // Check if this is a trend over time request (should create a line chart)
    if (intent.type === 'chart' && (intent.chartType === 'line' || this.isTrendOverTimeRequest(question))) {
      console.log('📈 Detected trend over time request, creating line chart');
      return this.handleTrendOverTime(intent, context, question);
    }
    
    // Check if this is an advice question about models (should get simple response, no charts)
    const isAdviceQuestion = this.isAdviceQuestion(question);
    
    if (isAdviceQuestion) {
      console.log('💡 Detected advice question, providing simple conversational response');
      return this.handleAdviceQuestion(question, context);
    }
    
    // For general questions without explicit chart request, pass flag to generateGeneralAnswer
    // This will be handled in generateGeneralAnswer itself via the wantsCharts check

    // If intent has axisMapping with y2 (secondary Y-axis), handle it intelligently
    if (intent.axisMapping?.y2) {
      console.log('📊 Secondary Y-axis detected in intent:', intent.axisMapping);
      return this.handleSecondaryYAxis(intent, context);
    }
    
    // If intent has specific information, enhance the question
    if (intent.targetVariable) {
      question = question || `analyze ${intent.targetVariable}`;
    }
    
    if (intent.variables && intent.variables.length > 0) {
      question = question || `analyze ${intent.variables.join(' and ')}`;
    }

    if (!question) {
      question = 'Please analyze the data';
    }

    try {
      // Use existing generateGeneralAnswer function
      const result = await generateGeneralAnswer(
        context.data,
        question,
        context.chatHistory,
        context.summary,
        context.sessionId
      );

      return {
        answer: result.answer,
        charts: result.charts,
        insights: result.insights,
      };
    } catch (error) {
      console.error('General handler error:', error);
      return this.createErrorResponse(
        error instanceof Error ? error : new Error(String(error)),
        intent,
        this.findSimilarColumns(intent.targetVariable || '', context.summary)
      );
    }
  }

  /**
   * Check if user explicitly requested charts/visualizations
   */
  private isExplicitChartRequest(question: string): boolean {
    const lower = question.toLowerCase();
    const chartKeywords = [
      /\b(show|display|create|generate|make|draw|plot|graph)\s+(me\s+)?(a\s+)?(chart|graph|plot|visualization|visual|diagram|figure)/i,
      /\b(chart|graph|plot|visualization|visual)\s+(of|for|showing|with)/i,
      /\b(show|display|create|generate|make|draw)\s+(me\s+)?(a\s+)?(bar|line|scatter|pie|area)\s+(chart|graph|plot)/i,
      /\b(visualize|visualization|visual)\s+/i,
      /\b(can you|please)\s+(show|display|create|generate|make|draw)\s+(me\s+)?(a\s+)?(chart|graph|plot)/i,
    ];
    
    return chartKeywords.some(pattern => pattern.test(lower));
  }

  /**
   * Check if question is asking for aggregation with category filter
   * Examples: 
   * - "aggregated value for category X"
   * - "aggregated value for X"
   * - "total for category Y"
   * - "what is the aggregated column name value for the column category X"
   * - "aggregated value for the column category X"
   */
  private isAggregationWithCategoryRequest(question: string): boolean {
    const lower = question.toLowerCase();
    return /\b(aggregated?\s+(?:column\s+name\s+)?value|aggregate|total|sum)\s+(?:for|of|in)\s+(?:the\s+)?(?:column\s+)?(?:category\s+)?[\w\s]+/i.test(lower) ||
           /\b(?:what\s+is\s+)?(?:the\s+)?(?:aggregated?\s+(?:column\s+name\s+)?value|total|sum)\s+(?:for|of|in)\s+(?:the\s+)?(?:column\s+)?(?:category\s+)?[\w\s]+/i.test(lower) ||
           /\b(?:aggregated?\s+value|aggregate|total|sum)\s+(?:for|of|in)\s+(?:the\s+)?column\s+category\s+[\w\s]+/i.test(lower);
  }

  /**
   * Handle aggregation queries with category filters
   * Examples: 
   * - "what is the aggregated value for the category men's fashion"
   * - "what is the aggregated column name value for the column category men's fashion"
   */
  private async handleAggregationWithCategory(
    intent: AnalysisIntent,
    context: HandlerContext,
    question: string
  ): Promise<HandlerResponse> {
    const allColumns = context.summary.columns.map(c => c.name);
    const numericColumns = context.summary.numericColumns || [];
    const data = context.data;
    
    console.log(`📊 Processing aggregation with category query: "${question}"`);
    
    // Extract category value from question (e.g., "men's fashion")
    // Handle patterns like:
    // - "for the column category men's fashion"
    // - "for category men's fashion"
    // - "for men's fashion"
    let categoryValue: string | null = null;
    
    // Pattern 1: "for the column category X" (most specific)
    const columnCategoryMatch = question.match(/(?:for|of|in)\s+(?:the\s+)?column\s+category\s+(.+?)$/i);
    if (columnCategoryMatch && columnCategoryMatch[1]) {
      categoryValue = columnCategoryMatch[1].trim();
      console.log(`   ✅ Extracted category (pattern 1): "${categoryValue}"`);
    } else {
      // Pattern 2: "for category X" or "for the category X"
      const categoryMatch = question.match(/(?:for|of|in)\s+(?:the\s+)?category\s+(.+?)(?:\s*$|\s+category|\s+in|\s+for)/i);
      if (categoryMatch && categoryMatch[1]) {
        categoryValue = categoryMatch[1].trim();
        console.log(`   ✅ Extracted category (pattern 2): "${categoryValue}"`);
      } else {
        // Pattern 3: "for X" (fallback - extract everything after "for")
        const forMatch = question.match(/(?:for|of|in)\s+(?:the\s+)?(?:column\s+)?(?:category\s+)?(.+?)$/i);
        if (forMatch && forMatch[1]) {
          categoryValue = forMatch[1].trim();
          console.log(`   ✅ Extracted category (pattern 3): "${categoryValue}"`);
        }
      }
    }
    
    if (!categoryValue) {
      console.log(`   ❌ Could not extract category value from: "${question}"`);
      return {
        answer: 'I couldn\'t identify the category in your question. Please specify, for example: "What is the aggregated value for the category men\'s fashion?" or "What is the aggregated column name value for the column category men\'s fashion?"',
        requiresClarification: true,
      };
    }
    
    // Find category column (look for columns like "category", "Category", "product_category", etc.)
    let categoryColumn: string | null = null;
    const categoryColumnNames = ['category', 'Category', 'product_category', 'Product Category', 'product_category_name', 'cat'];
    for (const colName of categoryColumnNames) {
      const matched = findMatchingColumn(colName, allColumns);
      if (matched) {
        categoryColumn = matched;
        break;
      }
    }
    
    // If not found, try to find any non-numeric column that might contain categories
    if (!categoryColumn) {
      for (const col of allColumns) {
        if (!numericColumns.includes(col) && !context.summary.dateColumns.includes(col)) {
          // Check if this column contains the category value
          const sampleValues = data.slice(0, 100).map(row => String(row[col] || '').toLowerCase());
          if (sampleValues.some(val => val.includes(categoryValue.toLowerCase()) || categoryValue.toLowerCase().includes(val))) {
            categoryColumn = col;
            break;
          }
        }
      }
    }
    
    if (!categoryColumn) {
      return {
        answer: `I couldn't find a category column in your dataset. Available columns: ${allColumns.slice(0, 10).join(', ')}${allColumns.length > 10 ? '...' : ''}`,
        requiresClarification: true,
      };
    }
    
    // Determine which column to aggregate (default to total, qty_ordered, or first numeric column)
    let aggregateColumn: string | null = null;
    const preferredColumns = ['total', 'qty_ordered', 'price', 'amount', 'value', 'revenue', 'sales'];
    for (const prefCol of preferredColumns) {
      const matched = findMatchingColumn(prefCol, numericColumns);
      if (matched) {
        aggregateColumn = matched;
        break;
      }
    }
    
    if (!aggregateColumn && numericColumns.length > 0) {
      aggregateColumn = numericColumns[0];
    }
    
    if (!aggregateColumn) {
      return {
        answer: 'I couldn\'t find any numeric columns to aggregate. Please specify which column you want to aggregate.',
        requiresClarification: true,
      };
    }
    
    console.log(`📊 Aggregating ${aggregateColumn} for category "${categoryValue}" in column "${categoryColumn}"`);
    
    // Filter data by category
    const filteredData = data.filter(row => {
      const rowCategory = String(row[categoryColumn] || '').toLowerCase().trim();
      const targetCategory = categoryValue.toLowerCase().trim();
      return rowCategory === targetCategory || rowCategory.includes(targetCategory) || targetCategory.includes(rowCategory);
    });
    
    if (filteredData.length === 0) {
      return {
        answer: `I couldn't find any data for the category "${categoryValue}" in the "${categoryColumn}" column. Available categories might be different.`,
        requiresClarification: true,
      };
    }
    
    // Calculate aggregation (sum by default)
    const values = filteredData
      .map(row => {
        const val = row[aggregateColumn];
        if (val === null || val === undefined || val === '') return null;
        if (typeof val === 'number') return isNaN(val) ? null : val;
        const cleaned = String(val).replace(/[%,]/g, '').trim();
        const parsed = Number(cleaned);
        return isNaN(parsed) ? null : parsed;
      })
      .filter(v => v !== null) as number[];
    
    if (values.length === 0) {
      return {
        answer: `I found ${filteredData.length} records for category "${categoryValue}", but the ${aggregateColumn} column has no numeric values.`,
        requiresClarification: true,
      };
    }
    
    const sum = values.reduce((a, b) => a + b, 0);
    const avg = sum / values.length;
    const min = Math.min(...values);
    const max = Math.max(...values);
    const count = values.length;
    
    const answer = `The aggregated value for the category "${categoryValue}" is:\n\n` +
      `- Total (Sum): ${sum.toFixed(2)}\n` +
      `- Average: ${avg.toFixed(2)}\n` +
      `- Minimum: ${min.toFixed(2)}\n` +
      `- Maximum: ${max.toFixed(2)}\n` +
      `- Count: ${count} records`;
    
    return {
      answer,
    };
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
   * Check if question is asking for seasonal patterns
   */
  private isSeasonalPatternRequest(question: string): boolean {
    const lower = question.toLowerCase();
    return /\b(seasonal\s+patterns?|seasonal\s+trends?|seasonal\s+variations?|monthly\s+patterns?|yearly\s+patterns?|patterns?\s+over\s+time|are\s+there\s+patterns?)\b/i.test(lower);
  }

  /**
   * Handle seasonal pattern requests
   * Creates line charts showing patterns over time, grouped by month/season
   */
  private async handleSeasonalPatterns(
    intent: AnalysisIntent,
    context: HandlerContext,
    question: string
  ): Promise<HandlerResponse> {
    const allColumns = context.summary.columns.map(c => c.name);
    const numericColumns = context.summary.numericColumns || [];
    const dateColumns = context.summary.dateColumns || [];
    
    // Extract variables mentioned in the question
    const mentionedColumns: string[] = [];
    const questionLower = question.toLowerCase();
    
    // Find all numeric columns mentioned in the question
    for (const col of numericColumns) {
      const colLower = col.toLowerCase();
      if (questionLower.includes(colLower)) {
        mentionedColumns.push(col);
      }
    }
    
    // If intent has variables, use those
    const variablesToAnalyze = intent.variables && intent.variables.length > 0
      ? intent.variables.map(v => findMatchingColumn(v, numericColumns)).filter(Boolean) as string[]
      : mentionedColumns.length > 0
        ? mentionedColumns
        : numericColumns.slice(0, 2); // Default to first 2 numeric columns
    
    if (variablesToAnalyze.length === 0) {
      return {
        answer: 'I couldn\'t find any numeric columns to analyze for seasonal patterns. Please specify which columns you\'d like to analyze.',
        requiresClarification: true,
        suggestions: numericColumns.slice(0, 5),
      };
    }
    
    // Find date column for grouping
    const dateColumn = intent.axisMapping?.x
      ? findMatchingColumn(intent.axisMapping.x, allColumns)
      : dateColumns[0] || 
        findMatchingColumn('order_date', allColumns) ||
        findMatchingColumn('date', allColumns) ||
        findMatchingColumn('Month', allColumns) ||
        findMatchingColumn('Date', allColumns) ||
        findMatchingColumn('Time', allColumns);
    
    if (!dateColumn) {
      return {
        answer: 'I couldn\'t find a date column to analyze seasonal patterns. Please ensure your dataset has a date/time column (e.g., order_date, date, Month).',
        requiresClarification: true,
      };
    }
    
    console.log(`📅 Analyzing seasonal patterns for: ${variablesToAnalyze.join(', ')} grouped by ${dateColumn}`);
    
    // Create charts for each variable (or combined if only one)
    const charts: ChartSpec[] = [];
    
    if (variablesToAnalyze.length === 1) {
      // Single variable - create one chart
      const yColumn = variablesToAnalyze[0];
      const chartSpec: ChartSpec = {
        type: 'line',
        title: `Seasonal Patterns in ${yColumn}`,
        x: dateColumn,
        y: yColumn,
        xLabel: dateColumn,
        yLabel: yColumn,
        aggregate: 'mean', // Aggregate by month to show seasonal patterns
      };
      
      // Process chart data - this will automatically detect date column and aggregate by month
      const chartData = processChartData(context.data, chartSpec);
      
      if (chartData.length === 0) {
        return {
          answer: `No valid data points found for seasonal pattern analysis. Please check that columns "${dateColumn}" and "${yColumn}" contain valid data.`,
          requiresClarification: true,
        };
      }
      
      charts.push({
        ...chartSpec,
        data: chartData,
        keyInsight: `Seasonal patterns in ${yColumn} over time`,
      });
    } else {
      // Multiple variables - create dual-axis chart or separate charts
      // For seasonal patterns, create a dual-axis chart if 2 variables, otherwise separate
      if (variablesToAnalyze.length === 2) {
        const yColumn = variablesToAnalyze[0];
        const y2Column = variablesToAnalyze[1];
        
        const chartSpec: ChartSpec = {
          type: 'line',
          title: `Seasonal Patterns: ${yColumn} and ${y2Column}`,
          x: dateColumn,
          y: yColumn,
          y2: y2Column,
          xLabel: dateColumn,
          yLabel: yColumn,
          y2Label: y2Column,
          aggregate: 'mean',
        } as any;
        
        const chartData = processChartData(context.data, chartSpec);
        
        if (chartData.length > 0) {
          charts.push({
            ...chartSpec,
            data: chartData,
            keyInsight: `Seasonal patterns in ${yColumn} and ${y2Column} over time`,
          });
        }
      } else {
        // More than 2 variables - create separate charts
        for (const yColumn of variablesToAnalyze.slice(0, 3)) { // Limit to 3 charts
          const chartSpec: ChartSpec = {
            type: 'line',
            title: `Seasonal Patterns in ${yColumn}`,
            x: dateColumn,
            y: yColumn,
            xLabel: dateColumn,
            yLabel: yColumn,
            aggregate: 'mean',
          };
          
          const chartData = processChartData(context.data, chartSpec);
          
          if (chartData.length > 0) {
            charts.push({
              ...chartSpec,
              data: chartData,
              keyInsight: `Seasonal patterns in ${yColumn} over time`,
            });
          }
        }
      }
    }
    
    if (charts.length === 0) {
      return {
        answer: 'I couldn\'t create charts for the seasonal pattern analysis. Please check that the date column and numeric columns contain valid data.',
        requiresClarification: true,
      };
    }
    
    // Generate insights
    const insights = await generateChartInsights(charts[0], charts[0].data || [], context.summary, context.chatInsights);
    
    const answer = variablesToAnalyze.length === 1
      ? `I've analyzed seasonal patterns in ${variablesToAnalyze[0]}. The chart shows trends over time grouped by ${dateColumn}.`
      : variablesToAnalyze.length === 2
        ? `I've analyzed seasonal patterns in ${variablesToAnalyze.join(' and ')}. The chart shows both variables over time.`
        : `I've analyzed seasonal patterns in ${variablesToAnalyze.slice(0, 3).join(', ')}. The charts show trends over time.`;
    
    return {
      answer,
      charts,
      insights: insights.keyInsight ? [{ id: 1, text: insights.keyInsight }] : [],
    };
  }

  /**
   * Check if question is asking for trends over time
   */
  private isTrendOverTimeRequest(question: string): boolean {
    const lower = question.toLowerCase();
    return /\b(trends?\s+in|trends?\s+for|trend\s+line|over\s+time|analyze\s+trends?)\b/i.test(lower);
  }

  /**
   * Handle trend over time requests by creating a line chart
   * Supports dual-axis charts when y2 is specified or "with" pattern is detected
   */
  private async handleTrendOverTime(
    intent: AnalysisIntent,
    context: HandlerContext,
    question: string
  ): Promise<HandlerResponse> {
    const allColumns = context.summary.columns.map(c => c.name);
    const numericColumns = context.summary.numericColumns || [];
    
    // Check if this is a dual-axis request (has y2 in axisMapping or "with" pattern in question)
    const hasY2 = intent.axisMapping?.y2;
    const hasWithPattern = /\s+with\s+/i.test(question);
    
    // Extract target variable from question or intent
    let targetVariable = intent.targetVariable;
    let y2Variable = intent.axisMapping?.y2;
    
    // If "with" pattern detected, try to extract both variables
    if (hasWithPattern && !y2Variable) {
      const withMatch = question.match(/([a-zA-Z0-9_\s]+?)\s+with\s+([a-zA-Z0-9_\s]+?)(?:\s+in\s+one\s+(?:trend\s+)?(?:line|chart)|$)/i);
      if (withMatch && withMatch.length >= 3) {
        const var1 = withMatch[1].trim();
        const var2 = withMatch[2].trim();
        
        // Try to match both variables
        const matchedVar1 = findMatchingColumn(var1, numericColumns) || findMatchingColumn(var1, allColumns);
        const matchedVar2 = findMatchingColumn(var2, numericColumns) || findMatchingColumn(var2, allColumns);
        
        if (matchedVar1 && matchedVar2 && numericColumns.includes(matchedVar1) && numericColumns.includes(matchedVar2)) {
          targetVariable = matchedVar1;
          y2Variable = matchedVar2;
          console.log(`✅ Detected dual-axis pattern: "${var1}" with "${var2}" → y=${matchedVar1}, y2=${matchedVar2}`);
        }
      }
    }
    
    if (!targetVariable) {
      // Try to extract from question patterns like "trends in X", "X over time"
      const trendMatch = question.match(/\b(?:trends?\s+in|trends?\s+for|analyze\s+trends?\s+in)\s+([a-zA-Z0-9_\s]+?)(?:\s+over\s+time|$)/i);
      if (trendMatch && trendMatch[1]) {
        targetVariable = trendMatch[1].trim();
      } else {
        // Try "X over time" pattern
        const overTimeMatch = question.match(/([a-zA-Z0-9_\s]+?)\s+over\s+time/i);
        if (overTimeMatch && overTimeMatch[1]) {
          targetVariable = overTimeMatch[1].trim();
        }
      }
    }
    
    // Match target variable to actual column
    const yColumn = targetVariable 
      ? findMatchingColumn(targetVariable, numericColumns) || findMatchingColumn(targetVariable, allColumns)
      : null;
    
    if (!yColumn || !numericColumns.includes(yColumn)) {
      return {
        answer: `I couldn't find a numeric column matching "${targetVariable || 'the specified variable'}" for the trend line. Available numeric columns: ${numericColumns.slice(0, 10).join(', ')}${numericColumns.length > 10 ? '...' : ''}`,
        requiresClarification: true,
        suggestions: numericColumns.slice(0, 5),
      };
    }
    
    // Match y2 variable if specified
    let y2Column: string | null = null;
    if (y2Variable) {
      y2Column = findMatchingColumn(y2Variable, numericColumns) || findMatchingColumn(y2Variable, allColumns);
      if (!y2Column || !numericColumns.includes(y2Column)) {
        console.warn(`⚠️ Could not match y2 variable "${y2Variable}", creating single-series chart`);
        y2Column = null;
      }
    }
    
    // Find time/date column for X-axis
    const xColumn = intent.axisMapping?.x
      ? findMatchingColumn(intent.axisMapping.x, allColumns)
      : context.summary.dateColumns[0] || 
        findMatchingColumn('Month', allColumns) || 
        findMatchingColumn('Date', allColumns) ||
        findMatchingColumn('Time', allColumns) ||
        allColumns[0]; // Fallback to first column
    
    if (!xColumn) {
      return {
        answer: 'I couldn\'t find a time or date column for the X-axis. Please specify which column should represent time.',
        requiresClarification: true,
      };
    }
    
    // Create chart spec (dual-axis if y2Column exists)
    let chartSpec: ChartSpec;
    if (y2Column) {
      console.log(`📈 Creating dual-axis trend line chart: X=${xColumn}, Y=${yColumn}, Y2=${y2Column}`);
      chartSpec = {
        type: 'line',
        title: `${yColumn} and ${y2Column} Trends Over Time`,
        x: xColumn,
        y: yColumn,
        y2: y2Column,
        xLabel: xColumn,
        yLabel: yColumn,
        y2Label: y2Column,
        aggregate: 'none',
      } as any;
    } else {
      console.log(`📈 Creating trend line chart: X=${xColumn}, Y=${yColumn}`);
      chartSpec = {
        type: 'line',
        title: `Trend of ${yColumn} Over Time`,
        x: xColumn,
        y: yColumn,
        xLabel: xColumn,
        yLabel: yColumn,
        aggregate: 'none',
      };
    }
    
    const chartData = processChartData(context.data, chartSpec);
    
    if (chartData.length === 0) {
      return {
        answer: `No valid data points found for trend line. Please check that columns "${xColumn}" and "${yColumn}"${y2Column ? ` and "${y2Column}"` : ''} contain valid data.`,
        requiresClarification: true,
      };
    }
    
    // Calculate smart axis domains based on statistical measures
    const smartDomains = calculateSmartDomainsForChart(
      chartData,
      xColumn,
      yColumn,
      y2Column || undefined,
      {
        yOptions: { useIQR: true, paddingPercent: 5, includeOutliers: true },
        y2Options: y2Column ? { useIQR: true, paddingPercent: 5, includeOutliers: true } : undefined,
      }
    );
    
    const insights = await generateChartInsights(chartSpec, chartData, context.summary, context.chatInsights);
    
    const answer = y2Column
      ? `I've created a line chart with ${yColumn} on the left axis and ${y2Column} on the right axis, plotted over ${xColumn}.`
      : `I've created a trend line showing ${yColumn} over time (${xColumn}).`;
    
    return {
      answer,
      charts: [{
        ...chartSpec,
        data: chartData,
        ...smartDomains, // Add smart domains (xDomain, yDomain, y2Domain)
        keyInsight: insights.keyInsight,
      }],
      insights: [],
    };
  }

  /**
   * Check if question is asking for advice/suggestions rather than performing an action
   */
  private isAdviceQuestion(question: string): boolean {
    const lower = question.toLowerCase();
    const advicePatterns = [
      /how\s+can\s+we\s+improve/i,
      /how\s+to\s+improve/i,
      /what\s+should\s+we\s+do/i,
      /what\s+would\s+help/i,
      /suggestions?\s+for/i,
      /recommendations?\s+for/i,
      /advice\s+on/i,
      /how\s+do\s+we\s+improve/i,
      /what\s+can\s+we\s+do\s+to/i,
      /how\s+should\s+we/i,
    ];
    
    return advicePatterns.some(pattern => pattern.test(lower));
  }

  /**
   * Handle advice questions with simple conversational responses (no charts)
   */
  private async handleAdviceQuestion(
    question: string,
    context: HandlerContext
  ): Promise<HandlerResponse> {
    const { getModelForTask } = await import('../models.js');
    const { openai } = await import('../../openai.js');
    
    // Build context from recent chat history
    const recentHistory = context.chatHistory
      .slice(-5)
      .filter(msg => msg.content && msg.content.length < 1000)
      .map((msg) => `${msg.role}: ${msg.content}`)
      .join('\n');

    const historyContext = recentHistory ? `\n\nCONVERSATION HISTORY:\n${recentHistory}` : '';

    const prompt = `You are a helpful data analyst assistant. The user is asking for advice or suggestions about their data analysis or models.

User question: "${question}"
${historyContext}

Provide a helpful, conversational response with practical suggestions. Keep it concise (2-4 sentences). 
- If they're asking about improving a model, suggest things like: trying different features, feature engineering, different model types, hyperparameter tuning, or getting more data
- If they're asking about data analysis, suggest relevant approaches
- Be friendly and actionable

Do NOT generate charts or visualizations. Just provide text advice.

Respond naturally and conversationally.`;

    try {
      const model = getModelForTask('generation');
      const response = await openai.chat.completions.create({
        model,
        messages: [
          {
            role: 'system',
            content: 'You are a helpful data analyst assistant. Provide concise, actionable advice without generating charts.',
          },
          {
            role: 'user',
            content: prompt,
          },
        ],
        temperature: 0.7,
        max_tokens: 300,
      });

      const answer = response.choices[0].message.content?.trim() || 
        'I can help you improve your analysis. Could you provide more details about what you\'d like to improve?';

      return {
        answer,
        charts: [], // Explicitly no charts for advice questions
        insights: [],
      };
    } catch (error) {
      console.error('Error generating advice response:', error);
      return {
        answer: 'I can help you improve your analysis. Could you provide more details about what specific aspect you\'d like to improve?',
        charts: [],
        insights: [],
      };
    }
  }

  /**
   * Handle secondary Y-axis requests intelligently (AI-first, no regex)
   */
  private async handleSecondaryYAxis(
    intent: AnalysisIntent,
    context: HandlerContext
  ): Promise<HandlerResponse> {
    const { findMatchingColumn } = await import('../utils/columnMatcher.js');
    const { processChartData } = await import('../../chartGenerator.js');
    const { generateChartInsights } = await import('../../insightGenerator.js');

    const allColumns = context.summary.columns.map(c => c.name);
    const y2Variable = intent.axisMapping!.y2!;
    
    // Match the y2 variable to actual column name
    const y2Column = findMatchingColumn(y2Variable, allColumns);
    
    if (!y2Column) {
      return {
        answer: `I couldn't find a column matching "${y2Variable}" for the secondary Y-axis. Available columns: ${allColumns.slice(0, 5).join(', ')}${allColumns.length > 5 ? '...' : ''}`,
        requiresClarification: true,
        suggestions: allColumns.slice(0, 5),
      };
    }

    console.log('🔍 Looking for previous chart in chat history to add secondary Y-axis...');
    
    // Look for the most recent chart in chat history
    let previousChart: ChartSpec | null = null;
    for (let i = context.chatHistory.length - 1; i >= 0; i--) {
      const msg = context.chatHistory[i];
      if (msg.role === 'assistant' && msg.charts && msg.charts.length > 0) {
        // Find a line chart (most likely to have dual-axis)
        previousChart = msg.charts.find(c => c.type === 'line') || msg.charts[0];
        if (previousChart) {
          console.log('✅ Found previous chart:', previousChart.title);
          break;
        }
      }
    }
    
    // If we found a previous chart, add the secondary Y-axis to it
    if (previousChart && previousChart.type === 'line') {
      console.log('🔄 Adding secondary Y-axis to existing chart...');
      
      // Create updated chart spec with y2
      const updatedChart: ChartSpec = {
        ...previousChart,
        y2: y2Column,
        y2Label: y2Column,
        title: previousChart.title?.replace(/over.*$/i, '') || `${previousChart.y} and ${y2Column} Trends`,
      };
      
      // Process the data
      const chartData = processChartData(context.data, updatedChart);
      console.log(`✅ Dual-axis line data: ${chartData.length} points`);
      
      if (chartData.length === 0) {
        return {
          answer: `No valid data points found. Please check that column "${y2Column}" exists and contains numeric data.`,
          requiresClarification: true,
        };
      }
      
      // Calculate smart axis domains
      const smartDomains = calculateSmartDomainsForChart(
        chartData,
        updatedChart.x,
        updatedChart.y,
        y2Column,
        {
          yOptions: { useIQR: true, paddingPercent: 5, includeOutliers: true },
          y2Options: { useIQR: true, paddingPercent: 5, includeOutliers: true },
        }
      );
      
      const insights = await generateChartInsights(updatedChart, chartData, context.summary, context.chatInsights);
      
      return {
        answer: `I've added ${y2Column} on the secondary Y-axis. The chart now shows ${previousChart.y} on the left axis and ${y2Column} on the right axis.`,
        charts: [{
          ...updatedChart,
          data: chartData,
          ...smartDomains, // Add smart domains
          keyInsight: insights.keyInsight,
        }],
      };
    }
    
    // If no previous chart found, but we have y2, try to create a new dual-axis chart
    // Use intent's axisMapping or infer from context
    const primaryY = intent.axisMapping?.y 
      ? findMatchingColumn(intent.axisMapping.y, allColumns)
      : context.summary.numericColumns[0];
    
    const xAxis = intent.axisMapping?.x
      ? findMatchingColumn(intent.axisMapping.x, allColumns)
      : context.summary.dateColumns[0] || 
        findMatchingColumn('Month', allColumns) || 
        findMatchingColumn('Date', allColumns) ||
        allColumns[0];
    
    if (primaryY && y2Column && xAxis) {
      console.log('📊 Creating new dual-axis chart:', { x: xAxis, y: primaryY, y2: y2Column });
      
      const dualAxisSpec: ChartSpec = {
        type: 'line',
        title: `${primaryY} and ${y2Column} Trends Over Time`,
        x: xAxis,
        y: primaryY,
        y2: y2Column,
        xLabel: xAxis,
        yLabel: primaryY,
        y2Label: y2Column,
        aggregate: 'none',
      };
      
      const chartData = processChartData(context.data, dualAxisSpec);
      if (chartData.length > 0) {
        // Calculate smart axis domains
        const smartDomains = calculateSmartDomainsForChart(
          chartData,
          dualAxisSpec.x,
          dualAxisSpec.y,
          y2Column,
          {
            yOptions: { useIQR: true, paddingPercent: 5, includeOutliers: true },
            y2Options: { useIQR: true, paddingPercent: 5, includeOutliers: true },
          }
        );
        
        const insights = await generateChartInsights(dualAxisSpec, chartData, context.summary, context.chatInsights);
        return {
          answer: `I've created a line chart with ${primaryY} on the left axis and ${y2Column} on the right axis.`,
          charts: [{
            ...dualAxisSpec,
            data: chartData,
            ...smartDomains, // Add smart domains
            keyInsight: insights.keyInsight,
          }],
        };
      }
    }
    
    return {
      answer: `I detected a request to add ${y2Column} on the secondary Y-axis, but I couldn't find a previous chart to modify. Could you create a chart first, or specify which variable should be on the primary Y-axis?`,
      requiresClarification: true,
    };
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
}

