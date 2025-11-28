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

/**
 * Load data from various sources (rawData, blob, sampleRows)
 */
// Use shared data loader to ensure consistency with analysis
import { loadLatestData } from "../../utils/dataLoader.js";

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

  // Get chat document
  const chatDocument = await getChatBySessionIdForUser(sessionId, username);
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

  // Execute operation
  const result = await executeDataOperation(intent, fullData, sessionId, chatDocument, message);

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

