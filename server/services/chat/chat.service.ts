/**
 * Chat Service
 * Main business logic for chat operations
 */
import { Message } from "../../shared/schema.js";
import { answerQuestion } from "../../lib/dataAnalyzer.js";
import { generateAISuggestions } from "../../lib/suggestionGenerator.js";
import { 
  getChatBySessionIdForUser, 
  addMessagesBySessionId, 
  updateMessageAndTruncate,
  ChatDocument 
} from "../../models/chat.model.js";
import { enrichCharts, validateAndEnrichResponse } from "./chatResponse.service.js";
import { loadLatestData, loadDataForColumns } from "../../utils/dataLoader.js";
import { extractRequiredColumns, extractColumnsFromHistory } from "../../lib/agents/utils/columnExtractor.js";
import { classifyIntent } from "../../lib/agents/intentClassifier.js";
import { parseUserQuery } from "../../lib/queryParser.js";
import queryCache from "../../lib/cache.js";

export interface ProcessChatMessageParams {
  sessionId: string;
  message: string;
  targetTimestamp?: number;
  username: string;
}

export interface ProcessChatMessageResult {
  answer: string;
  charts?: any[];
  insights?: any[];
  suggestions?: string[];
}

/**
 * Process a chat message and generate response
 */
export async function processChatMessage(params: ProcessChatMessageParams): Promise<ProcessChatMessageResult> {
  const { sessionId, message, targetTimestamp, username } = params;

  // Get chat document FIRST (with full history) so processing uses complete context
  console.log('🔍 Fetching chat document for sessionId:', sessionId);
  const chatDocument = await getChatBySessionIdForUser(sessionId, username);

  if (!chatDocument) {
    throw new Error('Session not found. Please upload a file first.');
  }

  console.log('✅ Chat document found, loading latest data...');
  
  // Fetch last 15 messages from Cosmos DB for context
  // For edited messages, use full history from database for processing
  // This ensures context-aware processing works correctly
  const allMessages = chatDocument.messages || [];
  const processingChatHistory = targetTimestamp 
    ? allMessages // Use full history from database for edits
    : allMessages.slice(-15); // Use last 15 messages for new messages

  // Extract required columns for optimized loading
  let requiredColumns: string[] = [];
  try {
    const intent = await classifyIntent(message, processingChatHistory || [], chatDocument.dataSummary);
    let parsedQuery: any = null;
    try {
      parsedQuery = await parseUserQuery(message, chatDocument.dataSummary, processingChatHistory || []);
    } catch (error) {
      // Query parsing is optional
    }
    
    const historyColumns = extractColumnsFromHistory(processingChatHistory || [], chatDocument.dataSummary);
    requiredColumns = extractRequiredColumns(
      message,
      intent,
      parsedQuery,
      null,
      chatDocument.dataSummary
    );
    requiredColumns = Array.from(new Set([...requiredColumns, ...historyColumns]));
    console.log(`📊 Extracted ${requiredColumns.length} required columns for optimized loading`);
  } catch (error) {
    console.warn('⚠️ Failed to extract required columns, loading all data:', error);
  }
  
  // Check cache before loading data
  // BUT: Skip cache for aggregation queries with category filters (data operations)
  const isAggregationWithCategory = /\b(aggregated?\s+(?:column\s+name\s+)?value|aggregate|total|sum)\s+(?:for|of|in)\s+(?:the\s+)?(?:column\s+)?(?:category\s+)?[\w\s]+/i.test(message) ||
                                     /\b(?:what\s+is\s+)?(?:the\s+)?(?:aggregated?\s+(?:column\s+name\s+)?value|total|sum)\s+(?:for|of|in)\s+(?:the\s+)?(?:column\s+)?(?:category\s+)?[\w\s]+/i.test(message) ||
                                     /\b(?:aggregated?\s+value|aggregate|total|sum)\s+(?:for|of|in)\s+(?:the\s+)?column\s+category\s+[\w\s]+/i.test(message);

  let cachedResult: ProcessChatMessageResult | null = null;
  if (!isAggregationWithCategory) {
    cachedResult = queryCache.get<ProcessChatMessageResult>(
      sessionId,
      message,
      requiredColumns
    );
    if (cachedResult) {
      console.log(`✅ Returning cached result`);
      return cachedResult;
    }
  } else {
    console.log(`🔄 Skipping cache for aggregation query (data operation)`);
  }
  
  // Load the latest data (including any modifications from data operations)
  // Use column-specific loading for large datasets
  const latestData = requiredColumns.length > 0
    ? await loadDataForColumns(chatDocument, requiredColumns)
    : await loadLatestData(chatDocument);
  console.log(`✅ Loaded ${latestData.length} rows of data for analysis`);
  
  // Get chat-level insights from the document
  const chatLevelInsights = chatDocument.insights && Array.isArray(chatDocument.insights) && chatDocument.insights.length > 0
    ? chatDocument.insights
    : undefined;

  // Answer the question using the latest data
  // Include permanent context if available
  const answerResult = await answerQuestion(
    latestData,
    message,
    processingChatHistory,
    chatDocument.dataSummary,
    sessionId,
    chatLevelInsights,
    undefined, // onThinkingStep
    undefined, // mode
    chatDocument.permanentContext // permanent context
  );

  // Enrich charts with data and insights
  if (answerResult.charts && Array.isArray(answerResult.charts)) {
    answerResult.charts = await enrichCharts(answerResult.charts, chatDocument, chatLevelInsights);
  }

  // Validate and enrich response
  const validated = validateAndEnrichResponse(answerResult, chatDocument, chatLevelInsights);

  // If targetTimestamp is provided, this is an edit operation
  // Truncate history AFTER processing (so processing had full context)
  // Only do this if we're actually editing (message exists), not for new messages
  if (targetTimestamp) {
    // Check if this message actually exists before trying to edit
    const existingMessage = chatDocument.messages?.find(
      (msg) => msg.timestamp === targetTimestamp && msg.role === 'user'
    );
    
    if (existingMessage) {
      console.log('✏️ Editing message with targetTimestamp:', targetTimestamp);
      try {
        await updateMessageAndTruncate(sessionId, targetTimestamp, message);
        console.log('✅ Message updated and messages truncated in database');
      } catch (truncateError) {
        console.error('⚠️ Failed to update message and truncate:', truncateError);
        // Continue with the chat request even if truncation fails
      }
    } else {
      // This is a new message, not an edit - ignore targetTimestamp
      console.log(`ℹ️ targetTimestamp ${targetTimestamp} provided but message not found. Treating as new message.`);
    }
  }

  // Generate AI suggestions
  let suggestions: string[] = [];
  try {
    const updatedChatHistory = [
      ...processingChatHistory,
      { role: 'user' as const, content: message, timestamp: targetTimestamp || Date.now() },
      { role: 'assistant' as const, content: validated.answer, timestamp: Date.now() }
    ];
    suggestions = await generateAISuggestions(
      updatedChatHistory,
      chatDocument.dataSummary,
      validated.answer
    );
  } catch (error) {
    console.error('Failed to generate suggestions:', error);
  }

  // Save messages to database
  // Use targetTimestamp for the user message to match the frontend's timestamp
  // This prevents duplicate messages when the SSE polling picks up the saved messages
  // IMPORTANT: 
  // - Full chart data (with data arrays) must be passed to addMessagesBySessionId so it can:
  //   1. Save large charts to blob storage
  //   2. Store full charts in top-level session.charts array
  // - Only message-level charts should be stripped of data to prevent CosmosDB size limit
  // - addMessagesBySessionId will handle saving charts to blob and stripping data from message charts
  // - addMessagesBySessionId will also check for duplicates before adding
  const assistantMessageTimestamp = Date.now();
  try {
    const userEmail = username?.toLowerCase();
    const userMessageTimestamp = targetTimestamp || Date.now();
    
    // Pass FULL charts with data to addMessagesBySessionId
    // It will:
    // 1. Save large charts to blob storage
    // 2. Store charts in top-level session.charts (with data for small charts, without data for large ones)
    // 3. Strip data from message-level charts to prevent CosmosDB size issues
    // 4. Check for duplicates before adding
    await addMessagesBySessionId(sessionId, [
      {
        role: 'user',
        content: message,
        timestamp: userMessageTimestamp,
        userEmail: userEmail,
      },
      {
        role: 'assistant',
        content: validated.answer,
        charts: validated.charts || [], // Pass FULL charts with data - addMessagesBySessionId will handle blob storage
        insights: validated.insights,
        preview: validated.preview || undefined, // Save preview data for data operations
        summary: validated.summary || undefined, // Save summary data for data operations
        timestamp: assistantMessageTimestamp,
      },
    ]);
    console.log(`✅ Messages saved to chat: ${chatDocument.id}`);
  } catch (cosmosError) {
    console.error("⚠️ Failed to save messages to CosmosDB:", cosmosError);
    // Continue without failing the chat - CosmosDB is optional
  }

  const result = {
    answer: validated.answer,
    charts: validated.charts,
    insights: validated.insights,
    suggestions,
  };
  
  // Cache the result
  queryCache.set(sessionId, message, requiredColumns, result);
  
  return result;
}

