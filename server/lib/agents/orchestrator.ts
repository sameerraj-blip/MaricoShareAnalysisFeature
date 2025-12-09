import { AnalysisIntent, classifyIntent } from './intentClassifier.js';
import { resolveContextReferences } from './contextResolver.js';
import { retrieveContext } from './contextRetriever.js';
import { BaseHandler, HandlerContext, HandlerResponse } from './handlers/baseHandler.js';
import { DataSummary, Message, ChartSpec, Insight, ThinkingStep } from '../../shared/schema.js';
import { createErrorResponse, getFallbackSuggestions } from './utils/errorRecovery.js';
import { askClarifyingQuestion } from './utils/clarification.js';
import { DataOpsHandler } from './handlers/dataOpsHandler.js';

export type ThinkingStepCallback = (step: ThinkingStep) => void;

/**
 * Agent Orchestrator
 * Main entry point for processing user queries
 * Routes to appropriate handlers and implements fallback chain
 */
export class AgentOrchestrator {
  private handlers: BaseHandler[] = [];
  
  /**
   * Get handler count (for initialization check)
   */
  getHandlerCount(): number {
    return this.handlers.length;
  }

  /**
   * Register a handler
   */
  registerHandler(handler: BaseHandler): void {
    this.handlers.push(handler);
  }

  /**
   * Track the current active step to ensure proper sequencing
   */
  private currentActiveStep: string | null = null;

  /**
   * Emit a thinking step if callback is provided
   * Automatically completes the previous active step when starting a new one
   */
  private emitThinkingStep(
    callback: ThinkingStepCallback | undefined,
    step: string,
    status: ThinkingStep['status'],
    details?: string
  ): void {
    if (!callback) return;

    // If we're starting a new active step, complete the previous one first
    if (status === 'active' && this.currentActiveStep && this.currentActiveStep !== step) {
      callback({
        step: this.currentActiveStep,
        status: 'completed',
        timestamp: Date.now(),
      });
    }

    // Update current active step
    if (status === 'active') {
      this.currentActiveStep = step;
    } else if (status === 'completed' && this.currentActiveStep === step) {
      this.currentActiveStep = null;
    }

    // Emit the current step
    callback({
      step,
      status,
      timestamp: Date.now(),
      details,
    });
  }

  /**
   * Process a user query
   * Implements the complete flow: intent classification → validation → routing → response
   */
  async processQuery(
    question: string,
    chatHistory: Message[],
    data: Record<string, any>[],
    summary: DataSummary,
    sessionId: string,
    chatInsights?: Insight[],
    onThinkingStep?: ThinkingStepCallback,
    mode?: string
  ): Promise<{ answer: string; charts?: ChartSpec[]; insights?: Insight[]; table?: any; operationResult?: any }> {
    try {
      console.log(`\n🔍 Processing query: "${question}" (mode: ${mode || 'analysis'})`);

      // Reset current active step at the start
      this.currentActiveStep = null;
      
      // ============================================
      // COMPLETE SEPARATE ROUTE FOR DATA OPS MODE
      // ============================================
      if (mode === 'dataOps') {
        console.log(`🔧 Data Ops Mode: Using separate route (bypassing analysis logic)`);
        
        // Step 1: Resolve context references (minimal, just for "that", "it", etc.)
        this.emitThinkingStep(onThinkingStep, "Understanding your question", "active");
        const enrichedQuestion = resolveContextReferences(question, chatHistory);
        this.emitThinkingStep(onThinkingStep, "Understanding your question", "completed");
        
        // Step 2: Extract operation from query (let DataOpsHandler use AI-based detection)
        this.emitThinkingStep(onThinkingStep, "Identifying data operation", "active");
        // Let DataOpsHandler's AI classifier handle all operation identification
        // This allows better context-aware detection (e.g., feature engineering vs add_column)
        let operation: string | undefined;
        
        // Create intent for DataOpsHandler
        // Note: DataOpsHandler checks for type 'dataOps', but AnalysisIntent schema doesn't include it
        // So we use 'custom' but ensure the handler can still process it via mode routing
        const intent: AnalysisIntent = {
          type: 'custom' as const,
          confidence: 1.0,
          customRequest: enrichedQuestion,
          operation: operation as any,
          requiresClarification: false,
        };
        
        this.emitThinkingStep(onThinkingStep, "Identifying data operation", "completed", operation ? `Operation: ${operation}` : 'Analyzing request');
        
        // Step 3: Get DataOpsHandler directly (no findHandler, no RAG, no analysis logic)
        const dataOpsHandler = this.handlers.find(h => h instanceof DataOpsHandler);
        
        if (!dataOpsHandler) {
          console.error('❌ DataOpsHandler not found!');
          return {
            answer: 'Data Operations handler is not available. Please contact support.',
          };
        }
        
        // Step 4: Build minimal context (no RAG retrieval needed for data ops)
        const handlerContext: HandlerContext = {
          data, // Will be replaced by handler with full dataset from blob
          summary,
          context: {
            dataChunks: [],
            pastQueries: [],
            mentionedColumns: [],
          }, // Empty context - data ops don't need RAG
          chatHistory,
          sessionId,
          chatInsights,
        };
        
        // Step 5: Execute DataOpsHandler directly
        this.emitThinkingStep(onThinkingStep, "Performing data operation", "active");
        
        try {
          const intentWithQuestion = { ...intent, originalQuestion: enrichedQuestion };
          const response = await dataOpsHandler.handle(intentWithQuestion, handlerContext);
          
          this.emitThinkingStep(onThinkingStep, "Performing data operation", "completed");
          
          // Handle response
          if (response.error) {
            console.log(`⚠️ DataOpsHandler returned error: ${response.error}`);
            this.emitThinkingStep(onThinkingStep, "Performing data operation", "error", response.error);
            return {
              answer: response.answer || `Error: ${response.error}`,
              table: response.table,
              operationResult: response.operationResult,
            };
          }
          
          if (response.requiresClarification) {
            this.emitThinkingStep(onThinkingStep, "Need more information", "active");
            return askClarifyingQuestion(intent, summary);
          }
          
          // Success - return response
          return {
            answer: response.answer,
            charts: response.charts,
            insights: response.insights,
            table: response.table,
            operationResult: response.operationResult,
          };
          
        } catch (error) {
          console.error('❌ DataOpsHandler error:', error);
          this.emitThinkingStep(onThinkingStep, "Performing data operation", "error", error instanceof Error ? error.message : String(error));
          return {
            answer: `An error occurred while performing the data operation: ${error instanceof Error ? error.message : String(error)}`,
          };
        }
      }
      
      // ============================================
      // MODELING MODE ROUTE
      // Routes through analysis flow, but intent classifier will detect ml_model
      // ============================================
      if (mode === 'modeling') {
        console.log(`🤖 Modeling Mode: Routing through analysis flow (will detect ml_model intent)`);
        // Continue to analysis flow - intent classifier will handle ml_model detection
      }
      
      // ============================================
      // ANALYSIS MODE ROUTE (existing logic)
      // Also handles modeling mode (routes through same flow)
      // ============================================
      
      this.emitThinkingStep(onThinkingStep, "Understanding your question", "active");

      // Step 1: Resolve context references ("that", "it", etc.)
      this.emitThinkingStep(onThinkingStep, "Understanding your question", "completed");
      this.emitThinkingStep(onThinkingStep, "Checking what you meant earlier", "active");
      const enrichedQuestion = resolveContextReferences(question, chatHistory);
      if (enrichedQuestion !== question) {
        console.log(`📝 Enriched question: "${enrichedQuestion}"`);
        this.emitThinkingStep(onThinkingStep, "Checking what you meant earlier", "completed", "Linked back to previous messages");
      } else {
        this.emitThinkingStep(onThinkingStep, "Checking what you meant earlier", "completed");
      }

      // Step 2: Classify intent (only for analysis mode)
      this.emitThinkingStep(onThinkingStep, "Figuring out the best way to answer", "active");
      const intent = await classifyIntent(enrichedQuestion, chatHistory, summary);
      console.log(`🎯 Intent: ${intent.type} (confidence: ${intent.confidence.toFixed(2)})`);
      this.emitThinkingStep(onThinkingStep, "Figuring out the best way to answer", "completed");

      // Step 3: Check if clarification needed
      // For correlation queries, try to proceed even with low confidence if we can extract target from question
      if (intent.type === 'conversational') {
        console.log(`💬 Conversational query detected, skipping clarification check`);
      } else if (intent.type === 'correlation') {
        // For correlation queries, check if we can extract target from question
        // If yes, proceed even with low confidence
        const question = intent.originalQuestion || intent.customRequest || '';
        const hasTargetInQuestion = /(?:what\s+(?:impacts?|affects?|influences?)|correlation\s+of)\s+[\w\s]+/i.test(question);
        
        if (hasTargetInQuestion && (intent.requiresClarification || intent.confidence < 0.5)) {
          console.log(`⚠️ Low confidence correlation query, but target may be extractable from question - proceeding to handler`);
          // Don't return early - let the handler try to extract and process
        } else if (intent.requiresClarification || intent.confidence < 0.5) {
          console.log(`❓ Low confidence (${intent.confidence.toFixed(2)}) or clarification required, asking for clarification`);
          this.emitThinkingStep(onThinkingStep, "Checking if I need more details", "active");
          return askClarifyingQuestion(intent, summary);
        }
      } else if (intent.requiresClarification || intent.confidence < 0.5) {
        console.log(`❓ Low confidence (${intent.confidence.toFixed(2)}) or clarification required, asking for clarification`);
        this.emitThinkingStep(onThinkingStep, "Checking if I need more details", "active");
        return askClarifyingQuestion(intent, summary);
      }

      // Step 4: Retrieve context (RAG) - only for analysis mode
      this.emitThinkingStep(onThinkingStep, "Looking through your data", "active");
      const context = await retrieveContext(
        enrichedQuestion,
        data,
        summary,
        chatHistory,
        sessionId
      );
      this.emitThinkingStep(onThinkingStep, "Looking through your data", "completed", context.dataChunks.length > 0 ? `Found ${context.dataChunks.length} useful pieces` : 'No extra data needed');

      // Step 5: Build handler context
      const handlerContext: HandlerContext = {
        data,
        summary,
        context,
        chatHistory,
        sessionId,
        chatInsights,
      };

      // Step 6: Route to appropriate handler
      this.emitThinkingStep(onThinkingStep, "Choosing the right analysis path", "active");
      const handler = this.findHandler(intent);
      
      if (!handler) {
        console.log(`⚠️ No handler found for intent type: ${intent.type}`);
        this.emitThinkingStep(onThinkingStep, "Choosing the right analysis path", "error", "Couldn't find a matching approach");
        return this.handleFallback(intent, handlerContext, onThinkingStep);
      }

      console.log(`✅ Routing to handler: ${handler.constructor.name}`);
      const handlerName = this.getFriendlyHandlerName(handler.constructor.name);
      this.emitThinkingStep(onThinkingStep, "Choosing the right analysis path", "completed", `Going with ${handlerName}`);

      // Step 7: Execute handler
      const handlerTask = this.getHandlerTaskDescription(intent.type);
      try {
        const intentWithQuestion = { ...intent, originalQuestion: enrichedQuestion };
        
        this.emitThinkingStep(onThinkingStep, handlerTask, "active");
        
        const response = await handler.handle(intentWithQuestion, handlerContext);
        
        this.emitThinkingStep(onThinkingStep, handlerTask, "completed");
        
        // Validate response
        if (response.error) {
          console.log(`⚠️ Handler returned error: ${response.error}`);
          this.emitThinkingStep(onThinkingStep, handlerTask, "error", response.error);
          return this.handleError(response.error, intent, handlerContext);
        }

        if (response.requiresClarification) {
          this.emitThinkingStep(onThinkingStep, "Checking if I need more details", "active");
          return askClarifyingQuestion(intent, summary);
        }

        // Validate answer exists
        if (!response.answer || response.answer.trim().length === 0) {
          console.error('❌ Handler returned empty answer');
          this.emitThinkingStep(onThinkingStep, handlerTask, "error", "Empty answer returned");
          throw new Error('Handler returned empty answer');
        }
        
        // Generate visualizations and insights thinking steps
        if (response.charts && response.charts.length > 0) {
          this.emitThinkingStep(onThinkingStep, "Turning results into visuals", "active");
          this.emitThinkingStep(onThinkingStep, "Turning results into visuals", "completed", `Created ${response.charts.length} chart${response.charts.length === 1 ? '' : 's'}`);
        }
        
        if (response.insights && response.insights.length > 0) {
          this.emitThinkingStep(onThinkingStep, "Summarizing the key points", "active");
          this.emitThinkingStep(onThinkingStep, "Summarizing the key points", "completed", `Captured ${response.insights.length} insight${response.insights.length === 1 ? '' : 's'}`);
        }
        
        this.emitThinkingStep(onThinkingStep, "Putting everything together", "active");
        this.emitThinkingStep(onThinkingStep, "Putting everything together", "completed");
        
        // Return successful response
        console.log(`✅ Handler returned answer (${response.answer.length} chars)`);
        return {
          answer: response.answer,
          charts: response.charts,
          insights: response.insights,
          table: response.table,
          operationResult: response.operationResult,
        };
      } catch (handlerError) {
        console.error(`❌ Handler execution failed:`, handlerError);
        const errorMsg = handlerError instanceof Error ? handlerError.message : String(handlerError);
        this.emitThinkingStep(onThinkingStep, handlerTask, "error", errorMsg);
        return this.recoverFromError(handlerError, enrichedQuestion, intent, handlerContext, onThinkingStep);
      }
    } catch (error) {
      console.error(`❌ Orchestrator error:`, error);
      const errorMsg = error instanceof Error ? error.message : String(error);
      this.emitThinkingStep(onThinkingStep, "Processing query...", "error", errorMsg);
      return this.recoverFromError(
        error,
        question,
        { type: 'custom', confidence: 0.3, customRequest: question },
        { data, summary, context: { dataChunks: [], pastQueries: [], mentionedColumns: [] }, chatHistory, sessionId },
        onThinkingStep
      );
    }
  }

  /**
   * Convert handler class name to a user-friendly label
   */
  private getFriendlyHandlerName(handlerName: string): string {
    const cleaned = handlerName
      .replace(/Handler$/, '')
      .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
      .trim();
    return cleaned.length > 0 ? cleaned : 'analysis';
  }

  /**
   * Get a user-friendly task description for the detected intent
   */
  private getHandlerTaskDescription(intentType: string): string {
    const taskMap: Record<string, string> = {
      'correlation': 'Exploring how different factors relate',
      'trend': 'Looking for patterns over time',
      'comparison': 'Comparing different segments',
      'aggregation': 'Summarizing the numbers',
      'filter': 'Focusing on the most relevant data',
      'chart': 'Designing the right visualization',
      'dataOps': 'Manipulating your data',
      'modelling': 'Building a predictive model',
      'general': 'Digging into the data for answers',
      'custom': 'Working through your request',
      'conversational': 'Crafting a reply',
    };
    return taskMap[intentType] || 'Analyzing your data for insights';
  }

  /**
   * Find appropriate handler for intent
   */
  private findHandler(intent: AnalysisIntent): BaseHandler | null {
    for (const handler of this.handlers) {
      if (handler.canHandle(intent)) {
        return handler;
      }
    }
    return null;
  }

  /**
   * Handle fallback when no specific handler found
   */
  private async handleFallback(
    intent: AnalysisIntent,
    context: HandlerContext,
    onThinkingStep?: ThinkingStepCallback
  ): Promise<{ answer: string; charts?: ChartSpec[]; insights?: Insight[]; table?: any; operationResult?: any }> {
    // Try to use general handler if available (it can handle custom types)
    const generalHandler = this.handlers.find(h => h.canHandle(intent));
    
    if (generalHandler) {
      try {
        const response = await generalHandler.handle(intent, context);
        return {
          answer: response.answer,
          charts: response.charts,
          insights: response.insights,
          table: response.table,
          operationResult: response.operationResult,
        };
      } catch (error) {
        // Continue to error handling
      }
    }

    // Return helpful error message
    const suggestions = getFallbackSuggestions(intent, context.summary);
    return {
      answer: `I'm not sure how to handle that request. Here are some things I can help with:\n\n${suggestions.map(s => `- ${s}`).join('\n')}`,
    };
  }

  /**
   * Handle errors with recovery
   */
  private handleError(
    error: string,
    intent: AnalysisIntent,
    context: HandlerContext
  ): { answer: string; charts?: ChartSpec[]; insights?: Insight[] } {
    const suggestions = getFallbackSuggestions(intent, context.summary);
    return createErrorResponse(new Error(error), intent, context.summary, suggestions);
  }

  /**
   * Recover from errors with fallback chain
   */
  private async recoverFromError(
    error: unknown,
    question: string,
    intent: AnalysisIntent,
    context: HandlerContext,
    onThinkingStep?: ThinkingStepCallback
  ): Promise<{ answer: string; charts?: ChartSpec[]; insights?: Insight[]; table?: any; operationResult?: any }> {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.log(`🔄 Error recovery: ${errorMessage}`);

    // Fallback Chain:
    // 1. Try general handler
    const generalHandler = this.handlers.find(h => h.canHandle(intent));
    if (generalHandler) {
      try {
        console.log(`🔄 Trying general handler as fallback...`);
        this.emitThinkingStep(onThinkingStep, "Trying fallback handler...", "active");
        const response = await generalHandler.handle(intent, context);
        if (!response.error) {
          this.emitThinkingStep(onThinkingStep, "Trying fallback handler...", "completed");
          return {
            answer: response.answer,
            charts: response.charts,
            insights: response.insights,
            table: response.table,
            operationResult: response.operationResult,
          };
        }
      } catch (fallbackError) {
        console.log(`⚠️ General handler also failed`);
        this.emitThinkingStep(onThinkingStep, "Trying fallback handler...", "error");
      }
    }

    // 2. Ask clarifying question (but not for conversational queries)
    if (intent.confidence < 0.5 && intent.type !== 'conversational') {
      return askClarifyingQuestion(intent, context.summary);
    }

    // 3. Return helpful error with suggestions (or simple response for conversational)
    if (intent.type === 'conversational') {
      // For conversational queries, just return a friendly message
      const userMessage = intent.customRequest || question || '';
      const questionLower = userMessage.toLowerCase();
      if (questionLower.match(/\b(hi|hello|hey)\b/)) {
        return { answer: "Hi there! 👋 I'm here to help you explore your data. What would you like to know?" };
      }
      return { answer: "I'm here to help! What would you like to know about your data?" };
    }
    
    const suggestions = getFallbackSuggestions(intent, context.summary);
    return createErrorResponse(
      error instanceof Error ? error : new Error(errorMessage),
      intent,
      context.summary,
      suggestions
    );
  }
}

// Singleton instance
let orchestratorInstance: AgentOrchestrator | null = null;

/**
 * Get or create orchestrator instance
 */
export function getOrchestrator(): AgentOrchestrator {
  if (!orchestratorInstance) {
    orchestratorInstance = new AgentOrchestrator();
  }
  return orchestratorInstance;
}

