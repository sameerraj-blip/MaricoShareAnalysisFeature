/**
 * Data Ops Service
 * Main business logic for data operations
 */
import { Message } from "../../shared/schema.js";
import { parseDataOpsIntent, executeDataOperation, DataOpsIntent } from "../../lib/dataOps/dataOpsOrchestrator.js";
import { 
  getChatBySessionIdForUser, 
  updateChatDocument, 
  addMessagesBySessionId,
  ChatDocument 
} from "../../models/chat.model.js";
import { loadLatestData } from "../../utils/dataLoader.js";
import queryCache from "../../lib/cache.js";

export interface ProcessDataOpsParams {
  sessionId: string;
  message: string;
  dataOpsMode?: boolean;
  username: string;
}

export interface ProcessDataOpsResult {
  answer: string;
  preview?: any;
  summary?: any;
  saved?: boolean;
}

async function loadDataForOperation(
  chatDocument: ChatDocument,
  requiredColumns?: string[]
): Promise<Record<string, any>[]> {
  // Use the shared data loader to ensure we get the latest data
  // This ensures data operations work on the same data that analysis uses
  // For large datasets, we can filter columns to reduce memory usage
  return await loadLatestData(chatDocument, requiredColumns);
}

/**
 * Process a data operations request
 */
export async function processDataOperation(params: ProcessDataOpsParams): Promise<ProcessDataOpsResult> {
  const { sessionId, message, dataOpsMode, username } = params;

  // Get chat document - handle CosmosDB initialization errors gracefully
  let chatDocument: ChatDocument | null = null;
  try {
    chatDocument = await getChatBySessionIdForUser(sessionId, username);
  } catch (error: any) {
    // If CosmosDB isn't initialized, we can't get the document
    if (error?.message?.includes('CosmosDB container not initialized')) {
      console.warn('⚠️ CosmosDB not initialized, proceeding without session document. Context may be limited.');
      // We'll continue without chatDocument, but this means we won't have data or dataSummary
      // This is a limitation - we need CosmosDB to be initialized to work properly
      throw new Error('Database is initializing. Please wait a moment and try again.');
    }
    // Re-throw other errors
    throw error;
  }
  
  if (!chatDocument) {
    throw new Error('Session not found. Please upload a file first.');
  }

  // Update dataOpsMode if provided
  if (dataOpsMode !== undefined && chatDocument.dataOpsMode !== dataOpsMode) {
    chatDocument.dataOpsMode = dataOpsMode;
    await updateChatDocument(chatDocument);
  }

  // Load data
  const fullData = await loadDataForOperation(chatDocument);
  console.log(`✅ Using ${fullData.length} rows for data operation`);

  // Fetch last 15 messages from Cosmos DB for context
  const allMessages = chatDocument.messages || [];
  const chatHistory = allMessages.slice(-15);
  console.log(`📚 Using ${chatHistory.length} messages from database for context`);

  // Parse intent
  const intent = await parseDataOpsIntent(message, chatHistory, chatDocument.dataSummary, chatDocument);

  // Use last 15 messages for full chat history
  const fullChatHistory = chatHistory;

  // Execute operation
  const result = await executeDataOperation(intent, fullData, sessionId, chatDocument, message, fullChatHistory);

  // Invalidate cache when data is modified (saved operations)
  if (result.saved) {
    queryCache.invalidateSession(sessionId);
    console.log(`🗑️ Cache invalidated for session ${sessionId} due to data modification`);
  }

  // Save messages
  // Use consistent timestamps to prevent duplicates
  const assistantMessageTimestamp = Date.now();
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
        timestamp: assistantMessageTimestamp,
      },
    ]);
  } catch (error) {
    console.error("⚠️ Failed to save messages:", error);
  }

  return {
    answer: result.answer,
    preview: result.preview,
    summary: result.summary,
    saved: result.saved,
  };
}

