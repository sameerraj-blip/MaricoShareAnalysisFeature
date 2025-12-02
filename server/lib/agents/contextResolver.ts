import { Message, ChartSpec } from '../../shared/schema.js';

/**
 * Resolved Reference
 */
export interface ResolvedReference {
  type: 'chart' | 'insight' | 'variable' | 'unknown';
  value: string;
  index: number; // Index in chat history
}

/**
 * Resolve contextual references in question
 * Replaces "that", "it", "the previous one" with explicit references
 */
export function resolveContextReferences(
  question: string,
  chatHistory: Message[]
): string {
  const questionLower = question.toLowerCase();
  
  // Patterns that indicate context references
  const contextPatterns = [
    /\bthat\b/gi,
    /\bit\b/gi,
    /\bthe\s+previous\s+one\b/gi,
    /\bthe\s+last\s+one\b/gi,
    /\bthe\s+above\b/gi,
    /\bthe\s+chart\b/gi,
    /\bthat\s+chart\b/gi,
  ];

  // Check if question contains context references
  const hasContextReference = contextPatterns.some(pattern => pattern.test(question));
  
  if (!hasContextReference || chatHistory.length === 0) {
    return question; // No resolution needed
  }

  // Find most recent chart or insight
  let resolvedQuestion = question;
  
  // Look for most recent chart
  for (let i = chatHistory.length - 1; i >= 0; i--) {
    const message = chatHistory[i];
    if (message.role === 'assistant' && message.charts && message.charts.length > 0) {
      const lastChart = message.charts[message.charts.length - 1];
      const chartRef = `the "${lastChart.title}" chart`;
      
      // Replace context references with explicit chart reference
      resolvedQuestion = resolvedQuestion.replace(/\bthat\s+chart\b/gi, chartRef);
      resolvedQuestion = resolvedQuestion.replace(/\bthe\s+chart\b/gi, chartRef);
      resolvedQuestion = resolvedQuestion.replace(/\bthat\b/gi, chartRef);
      resolvedQuestion = resolvedQuestion.replace(/\bit\b/gi, chartRef);
      resolvedQuestion = resolvedQuestion.replace(/\bthe\s+previous\s+one\b/gi, chartRef);
      resolvedQuestion = resolvedQuestion.replace(/\bthe\s+last\s+one\b/gi, chartRef);
      
      console.log(`✅ Resolved context reference: "${question}" → "${resolvedQuestion}"`);
      return resolvedQuestion;
    }
  }

  // Look for most recent insight
  for (let i = chatHistory.length - 1; i >= 0; i--) {
    const message = chatHistory[i];
    if (message.role === 'assistant' && message.insights && message.insights.length > 0) {
      const lastInsight = message.insights[message.insights.length - 1];
      const insightRef = `the "${lastInsight.text.substring(0, 50)}..." insight`;
      
      resolvedQuestion = resolvedQuestion.replace(/\bthat\b/gi, insightRef);
      resolvedQuestion = resolvedQuestion.replace(/\bit\b/gi, insightRef);
      
      console.log(`✅ Resolved context reference to insight: "${question}" → "${resolvedQuestion}"`);
      return resolvedQuestion;
    }
  }

  // If no chart or insight found, return original
  return question;
}

/**
 * Resolve a specific context reference
 */
export function resolveContextReference(
  reference: string,
  chatHistory: Message[]
): ResolvedReference | null {
  // Look for most recent chart
  for (let i = chatHistory.length - 1; i >= 0; i--) {
    const message = chatHistory[i];
    if (message.role === 'assistant' && message.charts && message.charts.length > 0) {
      const lastChart = message.charts[message.charts.length - 1];
      return {
        type: 'chart',
        value: lastChart.title,
        index: i,
      };
    }
  }

  // Look for most recent insight
  for (let i = chatHistory.length - 1; i >= 0; i--) {
    const message = chatHistory[i];
    if (message.role === 'assistant' && message.insights && message.insights.length > 0) {
      const lastInsight = message.insights[message.insights.length - 1];
      return {
        type: 'insight',
        value: lastInsight.text,
        index: i,
      };
    }
  }

  return null;
}

