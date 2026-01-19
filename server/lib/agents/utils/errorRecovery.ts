import { AnalysisIntent } from '../intentClassifier.js';
import { DataSummary } from '../../../shared/schema.js';
import { ChartSpec, Insight } from '../../../shared/schema.js';

/**
 * Error Response Interface
 */
export interface ErrorResponse {
  answer: string;
  charts?: ChartSpec[];
  insights?: Insight[];
  requiresClarification?: boolean;
  error?: string;
  suggestions?: string[];
}

/**
 * Create a helpful error response with suggestions
 */
export function createErrorResponse(
  error: Error | string,
  intent: AnalysisIntent,
  summary?: DataSummary,
  suggestions?: string[]
): ErrorResponse {
  const errorMessage = error instanceof Error ? error.message : error;
  
  // Generate helpful error message based on error type
  // For complex queries, don't show error - let the general handler process it
  const question = intent.customRequest || intent.originalQuestion || '';
  const isComplexQuery = question && (
    question.includes('above') || 
    question.includes('below') || 
    question.includes('average') || 
    question.includes('which months') || 
    question.includes('which categories') ||
    question.includes('month-over-month') ||
    question.includes('consistent growth')
  );
  
  if (isComplexQuery) {
    // For complex queries, return a message that indicates processing, not an error
    // The orchestrator will try the general handler
    return {
      answer: "Processing your complex query with multiple conditions. Analyzing the data to provide results.",
      requiresClarification: false,
      error: undefined,
      suggestions: [],
    };
  }
  
  let answer = "I encountered an issue processing your request. ";
  
  if (errorMessage.includes('column') || errorMessage.includes('Column')) {
    answer += "It looks like there might be an issue with the column names. ";
    if (summary && suggestions && suggestions.length > 0) {
      answer += `Did you mean: ${suggestions.slice(0, 3).join(', ')}?`;
    } else if (summary) {
      answer += `Available columns: ${summary.columns.map(c => c.name).slice(0, 5).join(', ')}${summary.columns.length > 5 ? '...' : ''}`;
    }
  } else if (errorMessage.includes('data') || errorMessage.includes('Data')) {
    answer += "There seems to be an issue with the data. ";
    if (suggestions && suggestions.length > 0) {
      answer += `Suggestions: ${suggestions.join(', ')}`;
    }
  } else if (intent.confidence < 0.5) {
    answer = "I'm not entirely sure what you're asking for. Could you rephrase your question? ";
    if (summary) {
      answer += `I can help you analyze: ${summary.numericColumns.slice(0, 5).join(', ')}${summary.numericColumns.length > 5 ? '...' : ''}`;
    }
  } else {
    answer += "Let me try a different approach. Could you rephrase your question?";
  }

  return {
    answer,
    requiresClarification: intent.confidence < 0.5,
    error: errorMessage,
    suggestions: suggestions || [],
  };
}

/**
 * Determine if we should retry with a different approach
 */
export function shouldRetry(error: Error | string, attempt: number, maxAttempts: number = 2): boolean {
  if (attempt >= maxAttempts) {
    return false;
  }

  const errorMessage = error instanceof Error ? error.message : String(error);
  
  // Retry on these errors
  const retryableErrors = [
    'timeout',
    'network',
    'rate limit',
    'temporary',
    'service unavailable',
  ];

  return retryableErrors.some(keyword => 
    errorMessage.toLowerCase().includes(keyword)
  );
}

/**
 * Get fallback suggestions based on intent type
 */
export function getFallbackSuggestions(
  intent: AnalysisIntent,
  summary: DataSummary
): string[] {
  const suggestions: string[] = [];

  if (intent.type === 'correlation' && summary.numericColumns.length > 0) {
    suggestions.push(`What affects ${summary.numericColumns[0]}?`);
    if (summary.numericColumns.length > 1) {
      suggestions.push(`Show correlations for ${summary.numericColumns[1]}`);
    }
  } else if (intent.type === 'chart' && summary.numericColumns.length > 0) {
    suggestions.push(`Show me a chart of ${summary.numericColumns[0]}`);
    if (summary.dateColumns.length > 0) {
      suggestions.push(`Show ${summary.numericColumns[0]} over time`);
    }
  } else {
    suggestions.push(`What affects ${summary.numericColumns[0] || 'the data'}?`);
    suggestions.push(`Show me trends in the data`);
    suggestions.push(`Analyze correlations`);
  }

  return suggestions;
}

