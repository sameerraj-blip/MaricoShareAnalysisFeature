import { AnalysisIntent, classifyIntent } from './intentClassifier.js';
import { resolveContextReferences } from './contextResolver.js';
import { retrieveContext } from './contextRetriever.js';
import { BaseHandler, HandlerContext, HandlerResponse } from './handlers/baseHandler.js';
import { DataSummary, Message, ChartSpec, Insight, ThinkingStep } from '../../../shared/schema.js';
import { createErrorResponse, getFallbackSuggestions } from './utils/errorRecovery.js';
import { askClarifyingQuestion } from './utils/clarification.js';

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
    onThinkingStep?: ThinkingStepCallback
  ): Promise<{ answer: string; charts?: ChartSpec[]; insights?: Insight[] }> {
    try {
      console.log(`\n🔍 Processing query: "${question}"`);

      // Reset current active step at the start
      this.currentActiveStep = null;
      
      this.emitThinkingStep(onThinkingStep, "Understanding your question", "active");

      // Step 1: Resolve context references ("that", "it", etc.)
      // Complete "Understanding your question" before starting the next step
      this.emitThinkingStep(onThinkingStep, "Understanding your question", "completed");
      this.emitThinkingStep(onThinkingStep, "Checking what you meant earlier", "active");
      const enrichedQuestion = resolveContextReferences(question, chatHistory);
      if (enrichedQuestion !== question) {
        console.log(`📝 Enriched question: "${enrichedQuestion}"`);
        this.emitThinkingStep(onThinkingStep, "Checking what you meant earlier", "completed", "Linked back to previous messages");
      } else {
        this.emitThinkingStep(onThinkingStep, "Checking what you meant earlier", "completed");
      }

      // Step 2: Classify intent
      this.emitThinkingStep(onThinkingStep, "Figuring out the best way to answer", "active");
      const intent = await classifyIntent(enrichedQuestion, chatHistory, summary);
      console.log(`🎯 Intent: ${intent.type} (confidence: ${intent.confidence.toFixed(2)})`);
      this.emitThinkingStep(onThinkingStep, "Figuring out the best way to answer", "completed");

      // Step 3: Check if clarification needed
      // For conversational queries, skip clarification - they're usually simple greetings
      // Only ask for clarification if explicitly required AND not conversational
      if (intent.type === 'conversational') {
        console.log(`💬 Conversational query detected, skipping clarification check`);
      } else if (intent.requiresClarification || intent.confidence < 0.5) {
        console.log(`❓ Low confidence (${intent.confidence.toFixed(2)}) or clarification required, asking for clarification`);
        this.emitThinkingStep(onThinkingStep, "Checking if I need more details", "active");
        return askClarifyingQuestion(intent, summary);
      }

      // Step 4: Retrieve context (RAG)
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
        // Fallback to general handler or return error
        return this.handleFallback(intent, handlerContext, onThinkingStep);
      }

      console.log(`✅ Routing to handler: ${handler.constructor.name}`);
      const handlerName = this.getFriendlyHandlerName(handler.constructor.name);
      this.emitThinkingStep(onThinkingStep, "Choosing the right analysis path", "completed", `Going with ${handlerName}`);

      // Step 7: Execute handler
      // Emit handler-specific thinking step (declare outside try so it's accessible in catch)
      const handlerTask = this.getHandlerTaskDescription(intent.type);
      try {
        // Add original question to intent for handlers that need it
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
  ): Promise<{ answer: string; charts?: ChartSpec[]; insights?: Insight[] }> {
    // Try to use general handler if available (it can handle custom types)
    const generalHandler = this.handlers.find(h => h.canHandle(intent));
    
    if (generalHandler) {
      try {
        const response = await generalHandler.handle(intent, context);
        return {
          answer: response.answer,
          charts: response.charts,
          insights: response.insights,
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
  ): Promise<{ answer: string; charts?: ChartSpec[]; insights?: Insight[] }> {
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

