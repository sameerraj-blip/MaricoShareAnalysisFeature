import { AnalysisIntent } from '../intentClassifier.js';
import { ChartSpec, Insight, DataSummary, Message } from '../../../shared/schema.js';
import { createErrorResponse, ErrorResponse } from '../utils/errorRecovery.js';

/**
 * Handler Context
 * Contains all information needed for a handler to process a request
 */
export interface HandlerContext {
  data: Record<string, any>[];
  summary: DataSummary;
  context: RetrievedContext;
  chatHistory: Message[];
  sessionId: string;
  chatInsights?: Insight[];
}

/**
 * Retrieved Context from RAG
 */
export interface RetrievedContext {
  dataChunks: string[];
  pastQueries: string[];
  mentionedColumns: string[];
}

/**
 * Handler Response
 * Standardized response format from all handlers
 */
export interface HandlerResponse {
  answer: string;
  charts?: ChartSpec[];
  insights?: Insight[];
  requiresClarification?: boolean;
  error?: string;
  suggestions?: string[];
  table?: any;
  operationResult?: any;
}

/**
 * Validation Result
 */
export interface ValidationResult {
  valid: boolean;
  errors: string[];
  warnings: string[];
  suggestions: string[];
}

/**
 * Base Handler Class
 * All analysis handlers extend this base class
 */
export abstract class BaseHandler {
  /**
   * Check if this handler can handle the given intent
   */
  abstract canHandle(intent: AnalysisIntent): boolean;

  /**
   * Handle the intent and return a response
   */
  abstract handle(intent: AnalysisIntent, context: HandlerContext): Promise<HandlerResponse>;

  /**
   * Validate data before processing
   * Override in subclasses for specific validation
   */
  protected validateData(
    intent: AnalysisIntent,
    context: HandlerContext
  ): ValidationResult {
    const errors: string[] = [];
    const warnings: string[] = [];
    const suggestions: string[] = [];

    // Basic validation: check data exists
    if (!context.data || context.data.length === 0) {
      errors.push('No data available');
      return { valid: false, errors, warnings, suggestions };
    }

    // Check minimum row count (most analyses need at least 2 rows)
    if (context.data.length < 2) {
      warnings.push('Very small dataset (less than 2 rows)');
    }

    // Validate column references if intent has them
    // Use fuzzy matching instead of exact matching (handlers will do proper matching)
    // This validation is just a basic check - handlers do the actual intelligent matching
    if (intent.targetVariable) {
      const allColumnNames = context.summary.columns.map(c => c.name);
      const normalizedTarget = intent.targetVariable.toLowerCase().trim();
      const colExists = allColumnNames.some(
        col => col.toLowerCase().trim() === normalizedTarget ||
               col.toLowerCase().trim().includes(normalizedTarget) ||
               normalizedTarget.includes(col.toLowerCase().trim())
      );
      // Don't add error here - let handlers do intelligent matching
      // Only warn if completely unrelated
      if (!colExists && normalizedTarget.length >= 3) {
        // Only suggest if it's a reasonable length (not a typo)
        const similar = this.findSimilarColumns(
          intent.targetVariable,
          context.summary
        );
        if (similar.length === 0) {
          // No similar columns found - might be a real issue
          warnings.push(`Target variable "${intent.targetVariable}" may not match any columns`);
        }
      }
    }

    if (intent.variables && intent.variables.length > 0) {
      const allColumnNames = context.summary.columns.map(c => c.name);
      for (const variable of intent.variables) {
        const normalizedVar = variable.toLowerCase().trim();
        const colExists = allColumnNames.some(
          col => col.toLowerCase().trim() === normalizedVar ||
                 col.toLowerCase().trim().includes(normalizedVar) ||
                 normalizedVar.includes(col.toLowerCase().trim())
        );
        // Don't add error - let handlers do intelligent matching
        if (!colExists && normalizedVar.length >= 3) {
          warnings.push(`Variable "${variable}" may not match any columns`);
        }
      }
    }

    return {
      valid: errors.length === 0,
      errors,
      warnings,
      suggestions,
    };
  }

  /**
   * Find similar column names (fuzzy matching)
   */
  protected findSimilarColumns(
    searchName: string,
    summary: DataSummary,
    maxResults: number = 3
  ): string[] {
    const normalized = searchName.toLowerCase().replace(/[\s_-]/g, '');
    const matches: Array<{ name: string; score: number }> = [];

    for (const col of summary.columns) {
      const colNormalized = col.name.toLowerCase().replace(/[\s_-]/g, '');
      
      // Exact match
      if (colNormalized === normalized) {
        return [col.name];
      }

      // Contains match
      if (colNormalized.includes(normalized) || normalized.includes(colNormalized)) {
        const score = Math.min(
          normalized.length / colNormalized.length,
          colNormalized.length / normalized.length
        );
        matches.push({ name: col.name, score });
      }
    }

    // Sort by score and return top matches
    return matches
      .sort((a, b) => b.score - a.score)
      .slice(0, maxResults)
      .map(m => m.name);
  }

  /**
   * Create standardized error response
   */
  protected createErrorResponse(
    error: Error | string,
    intent: AnalysisIntent,
    suggestions?: string[]
  ): HandlerResponse {
    return createErrorResponse(error, intent, undefined, suggestions);
  }

  /**
   * Build conversational answer with context
   */
  protected buildAnswer(
    baseAnswer: string,
    intent: AnalysisIntent,
    context: HandlerContext
  ): string {
    let answer = baseAnswer;

    // Add context from RAG if available
    if (context.context.dataChunks.length > 0) {
      // Could enhance answer with retrieved context
    }

    // Add mentioned columns for reference
    if (context.context.mentionedColumns.length > 0) {
      // Could add column references
    }

    return answer;
  }
}

