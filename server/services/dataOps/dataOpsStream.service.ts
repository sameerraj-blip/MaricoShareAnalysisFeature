/**
 * Data Ops Stream Service
 * Handles streaming data operations with SSE
 */
import { Message } from "../../shared/schema.js";
import { parseDataOpsIntent, executeDataOperation } from "../../lib/dataOps/dataOpsOrchestrator.js";
import { 
  getChatBySessionIdForUser, 
  updateChatDocument, 
  addMessagesBySessionId,
  ChatDocument 
} from "../../models/chat.model.js";
import { sendSSE, setSSEHeaders } from "../../utils/sse.helper.js";
import { loadLatestData } from "../../utils/dataLoader.js";
import { Response } from "express";

export interface ProcessStreamDataOpsParams {
  sessionId: string;
  message: string;
  chatHistory?: Message[];
  dataOpsMode?: boolean;
  username: string;
  res: Response;
}

/**
 * Load data for operation using shared data loader
 * This ensures consistency with analysis - both use the same data source
 */
async function loadDataForOperation(chatDocument: ChatDocument): Promise<Record<string, any>[]> {
  // Use the shared data loader to ensure we get the latest data
  // This ensures data operations work on the same data that analysis uses
  return await loadLatestData(chatDocument);
}

/**
 * Process a streaming data operations request
 */
export async function processStreamDataOperation(params: ProcessStreamDataOpsParams): Promise<void> {
  const { sessionId, message, chatHistory, dataOpsMode, username, res } = params;

  // Set SSE headers
  setSSEHeaders(res);

  try {
    // Get chat document - handle CosmosDB initialization errors gracefully
    let chatDocument: ChatDocument | null = null;
    try {
      chatDocument = await getChatBySessionIdForUser(sessionId, username);
    } catch (error: any) {
      // If CosmosDB isn't initialized, we can't get the document
      if (error?.message?.includes('CosmosDB container not initialized')) {
        console.warn('⚠️ CosmosDB not initialized, cannot proceed without session document.');
        sendSSE(res, 'error', { message: 'Database is initializing. Please wait a moment and try again.' });
        res.end();
        return;
      }
      // Re-throw other errors
      throw error;
    }
    
    if (!chatDocument) {
      sendSSE(res, 'error', { message: 'Session not found. Please upload a file first.' });
      res.end();
      return;
    }

    // Update dataOpsMode if provided
    if (dataOpsMode !== undefined && chatDocument.dataOpsMode !== dataOpsMode) {
      chatDocument.dataOpsMode = dataOpsMode;
      await updateChatDocument(chatDocument);
    }

    // Send thinking step
    sendSSE(res, 'thinking', {
      step: 'Processing data operation',
      status: 'active',
      timestamp: Date.now(),
    });

    // Load data
    const fullData = await loadDataForOperation(chatDocument);
    console.log(`✅ Using ${fullData.length} rows for data operation`);

    // Parse intent
    sendSSE(res, 'thinking', {
      step: 'Understanding your request',
      status: 'active',
      timestamp: Date.now(),
    });

    const intent = await parseDataOpsIntent(message, chatHistory || [], chatDocument.dataSummary, chatDocument);

    sendSSE(res, 'thinking', {
      step: 'Understanding your request',
      status: 'completed',
      timestamp: Date.now(),
    });

    // Get full chat history - always use database messages as source of truth
    // Merge frontend chatHistory with database messages to ensure we have the latest
    let fullChatHistory: Message[] = [];
    try {
      // Always prefer database messages as they're the source of truth
      const dbMessages = chatDocument.messages || [];
      const frontendMessages = chatHistory || [];
      
      // Merge: use database messages if available, otherwise use frontend
      // Database messages are more complete as they include the latest assistant responses
      if (dbMessages.length > 0) {
        fullChatHistory = dbMessages;
        console.log(`📚 Using ${dbMessages.length} messages from database for context`);
      } else if (frontendMessages.length > 0) {
        fullChatHistory = frontendMessages;
        console.log(`📱 Using ${frontendMessages.length} messages from frontend for context`);
      }
    } catch (error) {
      console.warn('⚠️ Could not access chat history, using frontend history:', error);
      fullChatHistory = chatHistory || [];
    }

    // Execute operation
    sendSSE(res, 'thinking', {
      step: 'Executing data operation',
      status: 'active',
      timestamp: Date.now(),
    });

    const result = await executeDataOperation(intent, fullData, sessionId, chatDocument, message, fullChatHistory);

    sendSSE(res, 'thinking', {
      step: 'Executing data operation',
      status: 'completed',
      timestamp: Date.now(),
    });

    // Save messages
    try {
      await addMessagesBySessionId(sessionId, [
        {
          role: 'user',
          content: message,
          timestamp: Date.now(),
          userEmail: username.toLowerCase(),
        },
        {
          role: 'assistant',
          content: result.answer,
          timestamp: Date.now(),
        },
      ]);
    } catch (error) {
      console.error("⚠️ Failed to save messages:", error);
    }

    // Send response
    sendSSE(res, 'response', {
      answer: result.answer,
      preview: result.preview,
      summary: result.summary,
      saved: result.saved,
    });

    sendSSE(res, 'done', {});
    res.end();
    console.log('✅ Data Ops stream completed successfully');
  } catch (error) {
    console.error('Data Ops stream error:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to process data operation';
    sendSSE(res, 'error', { message: errorMessage });
    res.end();
  }
}

