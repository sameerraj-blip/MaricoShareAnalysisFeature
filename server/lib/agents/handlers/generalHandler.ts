import { BaseHandler, HandlerContext, HandlerResponse } from './baseHandler.js';
import { AnalysisIntent } from '../intentClassifier.js';
import { generateGeneralAnswer } from '../../dataAnalyzer.js';
import type { ChartSpec } from '../../../shared/schema.js';

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

    // If intent has axisMapping with y2 (secondary Y-axis), handle it intelligently
    if (intent.axisMapping?.y2) {
      console.log('📊 Secondary Y-axis detected in intent:', intent.axisMapping);
      return this.handleSecondaryYAxis(intent, context);
    }

    // Build question from intent
    let question = intent.customRequest || intent.originalQuestion || '';
    
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
      
      const insights = await generateChartInsights(updatedChart, chartData, context.summary, context.chatInsights);
      
      return {
        answer: `I've added ${y2Column} on the secondary Y-axis. The chart now shows ${previousChart.y} on the left axis and ${y2Column} on the right axis.`,
        charts: [{
          ...updatedChart,
          data: chartData,
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
        const insights = await generateChartInsights(dualAxisSpec, chartData, context.summary, context.chatInsights);
        return {
          answer: `I've created a line chart with ${primaryY} on the left axis and ${y2Column} on the right axis.`,
          charts: [{
            ...dualAxisSpec,
            data: chartData,
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

