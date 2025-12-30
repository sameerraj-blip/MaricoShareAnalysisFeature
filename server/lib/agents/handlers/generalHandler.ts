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
}

