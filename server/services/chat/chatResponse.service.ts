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
 */
export async function enrichCharts(
  charts: any[],
  chatDocument: ChatDocument,
  chatLevelInsights?: any[]
): Promise<any[]> {
  if (!charts || !Array.isArray(charts)) {
    return [];
  }

  try {
    return await Promise.all(
      charts.map(async (c: any) => {
        const dataForChart = c.data && Array.isArray(c.data)
          ? c.data
          : processChartData(chatDocument.rawData, c);
        const insights = !('keyInsight' in c)
          ? await generateChartInsights(c, dataForChart, chatDocument.dataSummary, chatLevelInsights)
          : null;
        return {
          ...c,
          data: dataForChart,
          keyInsight: c.keyInsight ?? insights?.keyInsight,
        };
      })
    );
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

