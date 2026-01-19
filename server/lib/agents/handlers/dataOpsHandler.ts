import { BaseHandler, HandlerContext, HandlerResponse } from './baseHandler.js';
import { AnalysisIntent } from '../intentClassifier.js';
import {
  DataOpsIntent,
  parseDataOpsIntent,
  executeDataOperation,
} from '../../dataOps/dataOpsOrchestrator.js';
import { getChatBySessionIdEfficient } from '../../../models/chat.model.js';

/**
 * DataOps Handler
 * Bridges the agent orchestrator with the newer Data Ops orchestrator.
 */
export class DataOpsHandler extends BaseHandler {
  canHandle(intent: AnalysisIntent): boolean {
    // Handle dataOps type OR custom type when routed via dataOps mode
    // The orchestrator routes to this handler when mode is 'dataOps', 
    // so we accept custom intents as well (they'll be parsed by parseDataOpsIntent)
    return intent.type === 'dataOps' || intent.type === 'custom';
  }

  async handle(intent: AnalysisIntent, context: HandlerContext): Promise<HandlerResponse> {
    const requestText = intent.originalQuestion || intent.customRequest || '';
    if (!requestText.trim()) {
      return {
        answer: 'Please let me know what data operation you would like me to perform.',
        requiresClarification: true,
      };
    }

    const sessionDoc = await getChatBySessionIdEfficient(context.sessionId);
    if (!sessionDoc) {
      return {
        answer: 'I could not find this session. Please re-upload your dataset and try again.',
        error: 'Session not found',
      };
    }

    const dataset =
      Array.isArray(sessionDoc.rawData) && sessionDoc.rawData.length > 0
        ? sessionDoc.rawData
        : context.data;

    let dataOpsIntent: DataOpsIntent;
    if (intent.operation) {
      dataOpsIntent = {
        operation: intent.operation as DataOpsIntent['operation'],
        column: intent.column,
        targetType: intent.targetType as any,
        limit: intent.limit,
        requiresClarification: false,
      };
      console.log(`📋 Using operation from intent: ${dataOpsIntent.operation}`);
    } else {
      console.log(`🔍 Parsing data ops intent for: "${requestText}"`);
      dataOpsIntent = await parseDataOpsIntent(
        requestText,
        context.chatHistory,
        context.summary,
        sessionDoc
      );
      console.log(`📋 Parsed intent:`, {
        operation: dataOpsIntent.operation,
        groupByColumn: dataOpsIntent.groupByColumn,
        aggColumns: dataOpsIntent.aggColumns,
        requiresClarification: dataOpsIntent.requiresClarification,
        clarificationMessage: dataOpsIntent.clarificationMessage,
      });
    }

    if (dataOpsIntent.requiresClarification) {
      console.log(`⚠️ Intent requires clarification: ${dataOpsIntent.clarificationMessage}`);
      return {
        answer: dataOpsIntent.clarificationMessage || 'Could you clarify which part of the data to work with?',
        requiresClarification: true,
      };
    }

    // If operation is "unknown" and requiresClarification is false, this is a general analysis question
    // Route it to the general analysis handler instead of executing as a data operation
    if (dataOpsIntent.operation === 'unknown' && !dataOpsIntent.requiresClarification) {
      console.log(`📊 Detected general analysis question, routing to general analysis handler: "${requestText}"`);
      // Return a response that signals this should be handled by general analysis
      // The orchestrator will try the next handler (GeneralHandler) if this handler returns null or a special signal
      return {
        answer: '', // Empty answer signals to try next handler
        shouldTryNextHandler: true, // Signal to orchestrator
      };
    }

    try {
      // Get chat history from context or session document
      const chatHistory = context.chatHistory || sessionDoc?.messages || [];
      
      const result = await executeDataOperation(
        dataOpsIntent,
        dataset,
        context.sessionId,
        sessionDoc,
        requestText,
        chatHistory
      );

      console.log(`📊 DataOpsHandler returning: answer length=${result.answer.length}, preview rows=${result.preview?.length || 0}, saved=${result.saved}`);
      if (result.preview && result.preview.length > 0) {
        console.log(`📊 Preview sample:`, JSON.stringify(result.preview[0], null, 2));
      }
      
      return {
        answer: result.answer,
        table: result.preview || result.data?.slice(0, 50) || [], // Fallback to data if preview not set
        operationResult: {
          summary: result.summary,
          saved: result.saved,
        },
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return {
        answer: `I couldn't complete that data operation: ${message}`,
        error: message,
      };
    }
  }
}
