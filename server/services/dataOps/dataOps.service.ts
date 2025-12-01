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

export interface ProcessDataOpsParams {
  sessionId: string;
  message: string;
  chatHistory?: Message[];
  dataOpsMode?: boolean;
  username: string;
}

export interface ProcessDataOpsResult {
  answer: string;
  preview?: any;
  summary?: any;
  saved?: boolean;
}

async function loadDataForOperation(chatDocument: ChatDocument): Promise<Record<string, any>[]> {
  // Use the shared data loader to ensure we get the latest data
  // This ensures data operations work on the same data that analysis uses
  return await loadLatestData(chatDocument);
}

/**
 * Process a data operations request
 */
export async function processDataOperation(params: ProcessDataOpsParams): Promise<ProcessDataOpsResult> {
  const { sessionId, message, chatHistory, dataOpsMode, username } = params;

  // Get chat document - handle CosmosDB initialization errors gracefully
  let chatDocument: ChatDocument | null = null;
  try {
    chatDocument = await getChatBySessionIdForUser(sessionId, username);
  } catch (error: any) {
    // If CosmosDB isn't initialized, we can't get the document
    // But we can still try to proceed if we have chatHistory with previous model info
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

  // Parse intent
  const intent = await parseDataOpsIntent(message, chatHistory || [], chatDocument.dataSummary, chatDocument);

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
  const result = await executeDataOperation(intent, fullData, sessionId, chatDocument, message, fullChatHistory);

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

  return {
    answer: result.answer,
    preview: result.preview,
    summary: result.summary,
    saved: result.saved,
  };
}

