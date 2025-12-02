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
import { loadLatestData } from "../../utils/dataLoader.js";

export interface ProcessChatMessageParams {
  sessionId: string;
  message: string;
  chatHistory?: Message[];
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
  const { sessionId, message, chatHistory, targetTimestamp, username } = params;

  // If targetTimestamp is provided, this is an edit operation
  if (targetTimestamp) {
    console.log('✏️ Editing message with targetTimestamp:', targetTimestamp);
    try {
      await updateMessageAndTruncate(sessionId, targetTimestamp, message);
      console.log('✅ Message updated and messages truncated in database');
    } catch (truncateError) {
      console.error('⚠️ Failed to update message and truncate:', truncateError);
      // Continue with the chat request even if truncation fails
    }
  }

  // Get chat document
  console.log('🔍 Fetching chat document for sessionId:', sessionId);
  const chatDocument = await getChatBySessionIdForUser(sessionId, username);

  if (!chatDocument) {
    throw new Error('Session not found. Please upload a file first.');
  }

  console.log('✅ Chat document found, loading latest data...');
  
  // Load the latest data (including any modifications from data operations)
  // This ensures that data operations performed by any user are reflected in analysis
  const latestData = await loadLatestData(chatDocument);
  console.log(`✅ Loaded ${latestData.length} rows of data for analysis`);
  
  // Get chat-level insights from the document
  const chatLevelInsights = chatDocument.insights && Array.isArray(chatDocument.insights) && chatDocument.insights.length > 0
    ? chatDocument.insights
    : undefined;

  // Answer the question using the latest data
  const result = await answerQuestion(
    latestData,
    message,
    chatHistory || [],
    chatDocument.dataSummary,
    sessionId,
    chatLevelInsights
  );

  // Enrich charts with data and insights
  if (result.charts && Array.isArray(result.charts)) {
    result.charts = await enrichCharts(result.charts, chatDocument, chatLevelInsights);
  }

  // Validate and enrich response
  const validated = validateAndEnrichResponse(result, chatDocument, chatLevelInsights);

  // Generate AI suggestions
  let suggestions: string[] = [];
  try {
    const updatedChatHistory = [
      ...(chatHistory || []),
      { role: 'user' as const, content: message, timestamp: Date.now() },
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
  try {
    const userEmail = username?.toLowerCase();
    await addMessagesBySessionId(sessionId, [
      {
        role: 'user',
        content: message,
        timestamp: Date.now(),
        userEmail: userEmail,
      },
      {
        role: 'assistant',
        content: validated.answer,
        charts: validated.charts,
        insights: validated.insights,
        timestamp: Date.now(),
      },
    ]);
    console.log(`✅ Messages saved to chat: ${chatDocument.id}`);
  } catch (cosmosError) {
    console.error("⚠️ Failed to save messages to CosmosDB:", cosmosError);
    // Continue without failing the chat - CosmosDB is optional
  }

  return {
    answer: validated.answer,
    charts: validated.charts,
    insights: validated.insights,
    suggestions,
  };
}

