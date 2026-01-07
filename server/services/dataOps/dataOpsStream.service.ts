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
  dataOpsMode?: boolean;
  username: string;
  res: Response;
}

/**
 * Load data for operation using shared data loader
 * This ensures consistency with analysis - both use the same data source
 */
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
 * Process a streaming data operations request
 */
export async function processStreamDataOperation(params: ProcessStreamDataOpsParams): Promise<void> {
  const { sessionId, message, dataOpsMode, username, res } = params;

  // Set SSE headers
  setSSEHeaders(res);

  // Track if client disconnected
  let clientDisconnected = false;

  // Handle client disconnect/abort
  const checkConnection = (): boolean => {
    if (res.writableEnded || res.destroyed || !res.writable) {
      clientDisconnected = true;
      return false;
    }
    return true;
  };

  // Set up connection close handlers
  res.on('close', () => {
    clientDisconnected = true;
    console.log('🚫 Client disconnected from data ops stream');
  });

  res.on('error', (error: any) => {
    // ECONNRESET, EPIPE, ECONNABORTED are expected when client disconnects
    if (error.code !== 'ECONNRESET' && error.code !== 'EPIPE' && error.code !== 'ECONNABORTED') {
      console.error('SSE connection error:', error);
    }
    clientDisconnected = true;
  });

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

    // Check connection before sending
    if (!checkConnection()) {
      return;
    }

    // Send thinking step
    if (!sendSSE(res, 'thinking', {
      step: 'Processing data operation',
      status: 'active',
      timestamp: Date.now(),
    })) {
      return; // Client disconnected
    }

    // Load data
    let fullData: Record<string, any>[];
    try {
      fullData = await loadDataForOperation(chatDocument);
    } catch (error: any) {
      console.error('❌ Failed to load data:', error);
      const errorMessage = error?.message || 'Failed to load data';
      sendSSE(res, 'error', { 
        message: errorMessage.includes('No data found') 
          ? 'No data found. Please ensure your file was uploaded correctly and try uploading again.' 
          : errorMessage 
      });
      res.end();
      return;
    }
    
    if (!fullData || fullData.length === 0) {
      sendSSE(res, 'error', { message: 'No data found. Please ensure your file was uploaded correctly and try again.' });
      res.end();
      return;
    }
    
    console.log(`✅ Using ${fullData.length} rows for data operation`);

    // Check connection before parsing
    if (!checkConnection()) {
      return;
    }

    // Parse intent
    if (!sendSSE(res, 'thinking', {
      step: 'Understanding your request',
      status: 'active',
      timestamp: Date.now(),
    })) {
      return; // Client disconnected
    }

    // Fetch last 15 messages from Cosmos DB for context
    const allMessages = chatDocument.messages || [];
    const chatHistory = allMessages.slice(-15);
    console.log(`📚 Using ${chatHistory.length} messages from database for context`);

    const intent = await parseDataOpsIntent(message, chatHistory, chatDocument.dataSummary, chatDocument);

    // Check connection after parsing
    if (!checkConnection()) {
      return;
    }

    if (!sendSSE(res, 'thinking', {
      step: 'Understanding your request',
      status: 'completed',
      timestamp: Date.now(),
    })) {
      return; // Client disconnected
    }

    // Use last 15 messages for full chat history
    const fullChatHistory = chatHistory;

    // Check connection before executing
    if (!checkConnection()) {
      return;
    }

    // Execute operation
    if (!sendSSE(res, 'thinking', {
      step: 'Executing data operation',
      status: 'active',
      timestamp: Date.now(),
    })) {
      return; // Client disconnected
    }

    const result = await executeDataOperation(intent, fullData, sessionId, chatDocument, message, fullChatHistory);

    // Check connection after execution
    if (!checkConnection()) {
      return;
    }

    if (!sendSSE(res, 'thinking', {
      step: 'Executing data operation',
      status: 'completed',
      timestamp: Date.now(),
    })) {
      return; // Client disconnected
    }

    // Check connection before saving messages
    if (!checkConnection()) {
      console.log('🚫 Client disconnected, skipping message save');
      return;
    }

    // Save messages only if client is still connected
    // Use consistent timestamp to prevent duplicates
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

    // Check connection before sending response
    if (!checkConnection()) {
      return;
    }

    // Send response
    if (!sendSSE(res, 'response', {
      answer: result.answer,
      preview: result.preview,
      summary: result.summary,
      saved: result.saved,
    })) {
      return; // Client disconnected
    }

    if (!sendSSE(res, 'done', {})) {
      return; // Client disconnected
    }

    if (!res.writableEnded && !res.destroyed) {
    res.end();
    }
    console.log('✅ Data Ops stream completed successfully');
  } catch (error) {
    console.error('Data Ops stream error:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to process data operation';
    sendSSE(res, 'error', { message: errorMessage });
    res.end();
  }
}

