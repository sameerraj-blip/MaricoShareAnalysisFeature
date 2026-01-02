/**
 * Chat Response Service
 * Handles response validation, enrichment, and formatting
 */
import { chatResponseSchema, ThinkingStep } from "../../shared/schema.js";
import { processChartData } from "../../lib/chartGenerator.js";
import { generateChartInsights } from "../../lib/insightGenerator.js";
import { ChatDocument } from "../../models/chat.model.js";

/**
 * Enrich charts with data and insights
 * Memory-optimized for large datasets
 */
export async function enrichCharts(
  charts: any[],
  chatDocument: ChatDocument,
  chatLevelInsights?: any[]
): Promise<any[]> {
  if (!charts || !Array.isArray(charts)) {
    return [];
  }

  const MAX_CHART_DATA_POINTS = 50000; // Limit to prevent memory issues

  try {
    // Process charts sequentially to avoid memory spikes from parallel processing
    const enrichedCharts: any[] = [];
    
    for (const c of charts) {
      try {
        let dataForChart = c.data && Array.isArray(c.data)
          ? c.data
          : processChartData(chatDocument.rawData, c);
        
        // Limit data size for memory efficiency
        if (dataForChart.length > MAX_CHART_DATA_POINTS) {
          console.log(`⚠️ Chart "${c.title}" has ${dataForChart.length} data points, limiting to ${MAX_CHART_DATA_POINTS}`);
          if (c.type === 'line' || c.type === 'area') {
            const step = Math.ceil(dataForChart.length / MAX_CHART_DATA_POINTS);
            dataForChart = dataForChart.filter((_: any, idx: number) => idx % step === 0).slice(0, MAX_CHART_DATA_POINTS);
          } else {
            dataForChart = dataForChart.slice(0, MAX_CHART_DATA_POINTS);
          }
        }
        
        const insights = !('keyInsight' in c)
          ? await generateChartInsights(c, dataForChart, chatDocument.dataSummary, chatLevelInsights)
          : null;
        
        enrichedCharts.push({
          ...c,
          data: dataForChart,
          keyInsight: c.keyInsight ?? insights?.keyInsight,
        });
      } catch (chartError) {
        console.error(`Error enriching chart "${c.title}":`, chartError);
        // Include chart without enrichment rather than failing completely
        enrichedCharts.push(c);
      }
    }
    
    return enrichedCharts;
  } catch (e) {
    console.error('Final enrichment of chat charts failed:', e);
    return charts;
  }
}

/**
 * Derive insights from charts if missing
 */
export function deriveInsightsFromCharts(charts: any[]): { id: number; text: string }[] {
  if (!charts || !Array.isArray(charts) || charts.length === 0) {
    return [];
  }

  try {
    const derived = charts
      .map((c: any, idx: number) => {
        const text = c?.keyInsight || (c?.title ? `Insight: ${c.title}` : null);
        return text ? { id: idx + 1, text } : null;
      })
      .filter(Boolean) as { id: number; text: string }[];
    return derived;
  } catch {
    return [];
  }
}

/**
 * Validate and enrich chat response
 */
export function validateAndEnrichResponse(result: any, chatDocument: ChatDocument, chatLevelInsights?: any[]): any {
  // Validate response has answer
  if (!result || !result.answer || result.answer.trim().length === 0) {
    throw new Error('Empty answer from answerQuestion');
  }

  // Validate response schema
  let validated = chatResponseSchema.parse(result);

  // Ensure overall chat insights always present: derive from charts if missing
  if ((!validated.insights || validated.insights.length === 0) && Array.isArray(validated.charts) && validated.charts.length > 0) {
    const derived = deriveInsightsFromCharts(validated.charts);
    if (derived.length > 0) {
      validated = { ...validated, insights: derived } as any;
    }
  }

  return validated;
}

/**
 * Create error response
 */
export function createErrorResponse(error: Error | string): any {
  const errorMessage = error instanceof Error ? error.message : error;
  return {
    error: errorMessage,
    answer: `I'm sorry, I encountered an error: ${errorMessage}. Please try again or rephrase your question.`,
    charts: [],
    insights: [],
  };
}

