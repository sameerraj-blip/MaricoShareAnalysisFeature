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
    return intent.type === 'dataOps';
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
    } else {
      dataOpsIntent = await parseDataOpsIntent(
        requestText,
        context.chatHistory,
        context.summary,
        sessionDoc
      );
    }

    if (dataOpsIntent.requiresClarification) {
      return {
        answer: dataOpsIntent.clarificationMessage || 'Could you clarify which part of the data to work with?',
        requiresClarification: true,
      };
    }

    try {
      const result = await executeDataOperation(
        dataOpsIntent,
        dataset,
        context.sessionId,
        sessionDoc,
        requestText
      );

      return {
        answer: result.answer,
        table: result.preview,
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
