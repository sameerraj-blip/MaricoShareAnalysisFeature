/**
 * Chat Stream Service
 * Handles streaming chat operations with SSE
 */
import { Message, ThinkingStep } from "../../shared/schema.js";
import { answerQuestion } from "../../lib/dataAnalyzer.js";
import { generateAISuggestions } from "../../lib/suggestionGenerator.js";
import { 
  getChatBySessionIdForUser, 
  addMessagesBySessionId, 
  updateMessageAndTruncate,
  getChatBySessionIdEfficient,
  ChatDocument 
} from "../../models/chat.model.js";
import { loadChartsFromBlob } from "../../lib/blobStorage.js";
import { enrichCharts, validateAndEnrichResponse } from "./chatResponse.service.js";
import { sendSSE, setSSEHeaders } from "../../utils/sse.helper.js";
import { loadLatestData } from "../../utils/dataLoader.js";
import { classifyMode } from "../../lib/agents/modeClassifier.js";
import { Response } from "express";

export interface ProcessStreamChatParams {
  sessionId: string;
  message: string;
  chatHistory?: Message[];
  targetTimestamp?: number;
  username: string;
  res: Response;
  mode?: 'general' | 'analysis' | 'dataOps' | 'modeling'; // Optional mode override
}

/**
 * Process a streaming chat message
 */
export async function processStreamChat(params: ProcessStreamChatParams): Promise<void> {
  const { sessionId, message, chatHistory, targetTimestamp, username, res, mode } = params;

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
    console.log('🚫 Client disconnected from chat stream');
  });

  res.on('error', (error: any) => {
    // ECONNRESET, EPIPE, ECONNABORTED are expected when client disconnects
    if (error.code !== 'ECONNRESET' && error.code !== 'EPIPE' && error.code !== 'ECONNABORTED') {
      console.error('SSE connection error:', error);
    }
    clientDisconnected = true;
  });

  try {
    // Get chat document FIRST (with full history) so processing uses complete context
    console.log('🔍 Fetching chat document for sessionId:', sessionId);
    let chatDocument: ChatDocument | null = null;
    
    try {
      chatDocument = await getChatBySessionIdForUser(sessionId, username);
    } catch (dbError: any) {
      // Handle CosmosDB connection errors gracefully
      const errorMessage = dbError instanceof Error ? dbError.message : String(dbError);
      if (errorMessage.includes('ECONNREFUSED') || errorMessage.includes('ETIMEDOUT') || dbError.code === 'ECONNREFUSED') {
        console.error('❌ CosmosDB connection error, attempting to continue with blob storage data...');
        sendSSE(res, 'error', { 
          message: 'Database connection issue. Please try again in a moment. If the problem persists, check your network connection.' 
        });
        res.end();
        return;
      }
      // Re-throw non-connection errors
      throw dbError;
    }

    if (!chatDocument) {
      sendSSE(res, 'error', { message: 'Session not found. Please upload a file first.' });
      res.end();
      return;
    }

    console.log('✅ Chat document found, loading latest data...');
    
    // Load the latest data (including any modifications from data operations)
    // This ensures that data operations performed by any user are reflected in analysis
    const latestData = await loadLatestData(chatDocument);
    console.log(`✅ Loaded ${latestData.length} rows of data for analysis`);
    
    // Get chat-level insights
    const chatLevelInsights = chatDocument.insights && Array.isArray(chatDocument.insights) && chatDocument.insights.length > 0
      ? chatDocument.insights
      : undefined;

    // Track thinking steps
    const thinkingSteps: ThinkingStep[] = [];
    
    // Create callback to emit thinking steps
    const onThinkingStep = (step: ThinkingStep) => {
      thinkingSteps.push(step);
      sendSSE(res, 'thinking', step);
    };

    // Check connection before processing
    if (!checkConnection()) {
      return;
    }

    // For edited messages, use full history from database for mode detection (same as new messages)
    // This ensures context-aware mode detection works correctly
    const modeDetectionChatHistory = targetTimestamp 
      ? (chatDocument.messages || []) // Use full history from database for edits
      : (chatHistory || []); // Use provided history for new messages

    // Determine mode: use provided mode (user override) or auto-detect
    // Treat 'general' the same as no mode (auto-detect)
    const shouldAutoDetect = !mode || mode === 'general';
    let detectedMode: 'analysis' | 'dataOps' | 'modeling' = mode && mode !== 'general' ? mode : 'analysis';
    
    if (shouldAutoDetect) {
      // Auto-detect mode using AI classifier
      try {
        onThinkingStep({
          step: 'Detecting query type',
          status: 'active',
          timestamp: Date.now(),
        });
        
        const modeClassification = await classifyMode(
          message,
          modeDetectionChatHistory,
          chatDocument.dataSummary
        );
        
        detectedMode = modeClassification.mode;
        
        onThinkingStep({
          step: 'Detecting query type',
          status: 'completed',
          timestamp: Date.now(),
          details: `Detected: ${detectedMode} (confidence: ${(modeClassification.confidence * 100).toFixed(0)}%)`,
        });
        
        console.log(`🎯 Auto-detected mode: ${detectedMode} (confidence: ${modeClassification.confidence.toFixed(2)})`);
      } catch (error) {
        console.error('⚠️ Mode classification failed, defaulting to analysis:', error);
        onThinkingStep({
          step: 'Detecting query type',
          status: 'completed',
          timestamp: Date.now(),
          details: 'Using default: analysis',
        });
        detectedMode = 'analysis';
      }
    } else {
      console.log(`🎯 Using user-specified mode: ${mode}`);
    }

    // For edited messages, use full history from database for processing (same as new messages)
    // This ensures context-aware processing works correctly
    const processingChatHistory = targetTimestamp 
      ? (chatDocument.messages || []) // Use full history from database for edits
      : (chatHistory || []); // Use provided history for new messages

    // Answer the question with streaming using the latest data
    const result = await answerQuestion(
      latestData,
      message,
      processingChatHistory,
      chatDocument.dataSummary,
      sessionId,
      chatLevelInsights,
      onThinkingStep,
      detectedMode
    );

    // Check connection after processing
    if (!checkConnection()) {
      return;
    }

    // Enrich charts
    if (result.charts && Array.isArray(result.charts)) {
      result.charts = await enrichCharts(result.charts, chatDocument, chatLevelInsights);
    }

    // Check connection after enriching charts
    if (!checkConnection()) {
      return;
    }

    // Validate and enrich response
    const validated = validateAndEnrichResponse(result, chatDocument, chatLevelInsights);

    // Transform data operations response format for frontend compatibility
    // Frontend expects 'preview' and 'summary', but orchestrator returns 'table' and 'operationResult'
    const transformedResponse: any = { ...validated };
    if (result.table && Array.isArray(result.table)) {
      transformedResponse.preview = result.table;
      console.log(`📊 Transformed table to preview: ${result.table.length} rows`);
    }
    if (result.operationResult) {
      if (result.operationResult.summary && Array.isArray(result.operationResult.summary)) {
        transformedResponse.summary = result.operationResult.summary;
        console.log(`📋 Transformed operationResult.summary to summary: ${result.operationResult.summary.length} items`);
      }
    }

    // Check connection before generating suggestions
    if (!checkConnection()) {
      return;
    }

    // Generate AI suggestions
    let suggestions: string[] = [];
    try {
      const updatedChatHistory = [
        ...(chatHistory || []),
        { role: 'user' as const, content: message, timestamp: Date.now() },
        { role: 'assistant' as const, content: transformedResponse.answer, timestamp: Date.now() }
      ];
      suggestions = await generateAISuggestions(
        updatedChatHistory,
        chatDocument.dataSummary,
        transformedResponse.answer
      );
    } catch (error) {
      console.error('Failed to generate suggestions:', error);
    }

    // Check connection before saving messages
    if (!checkConnection()) {
      console.log('🚫 Client disconnected, skipping message save');
      return;
    }

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
          // Continue - don't fail the entire request
        }
      } else {
        // This is a new message, not an edit - ignore targetTimestamp
        console.log(`ℹ️ targetTimestamp ${targetTimestamp} provided but message not found. Treating as new message.`);
      }
    }

    // Save messages only if client is still connected
    // Use targetTimestamp for the user message to match the frontend's timestamp
    // This prevents duplicate messages when the SSE polling picks up the saved messages
    // IMPORTANT: Pass FULL charts with data to addMessagesBySessionId
    // It will handle saving large charts to blob and stripping data from message charts
    try {
      const userEmail = username?.toLowerCase();
      const userMessageTimestamp = targetTimestamp || Date.now();
      
      // Pass FULL charts with data - addMessagesBySessionId will:
      // 1. Save large charts to blob storage
      // 2. Store charts in top-level session.charts
      // 3. Strip data from message-level charts to prevent CosmosDB size issues
      await addMessagesBySessionId(sessionId, [
        {
          role: 'user',
          content: message,
          timestamp: userMessageTimestamp,
          userEmail: userEmail,
        },
        {
          role: 'assistant',
          content: transformedResponse.answer,
          charts: transformedResponse.charts || [], // Pass FULL charts with data
          insights: transformedResponse.insights,
          timestamp: Date.now(),
        },
      ]);
      console.log(`✅ Messages saved to chat: ${chatDocument.id}`);
    } catch (cosmosError) {
      console.error("⚠️ Failed to save messages to CosmosDB:", cosmosError);
    }

    // Check connection before sending response
    if (!checkConnection()) {
      return;
    }

    // Send final response with preview/summary for data operations
    if (!sendSSE(res, 'response', {
      ...transformedResponse,
      suggestions,
    })) {
      return; // Client disconnected
    }

    if (!sendSSE(res, 'done', {})) {
      return; // Client disconnected
    }

    if (!res.writableEnded && !res.destroyed) {
    res.end();
    }
    console.log('✅ Stream completed successfully');
  } catch (error) {
    console.error('Chat stream error:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to process message';
    if (checkConnection()) {
    sendSSE(res, 'error', { message: errorMessage });
    }
    if (!res.writableEnded && !res.destroyed) {
    res.end();
    }
  }
}

/**
 * Stream chat messages for a session
 */
export async function streamChatMessages(sessionId: string, username: string, req: Request, res: Response): Promise<void> {
  setSSEHeaders(res);

  try {
    // Normalize username for consistent comparison
    const normalizedUsername = username.trim().toLowerCase();
    
    // Verify user has access to this session
    let chatDocument: ChatDocument | null = null;
    try {
      chatDocument = await getChatBySessionIdForUser(sessionId, normalizedUsername);
    } catch (accessError: any) {
      // Handle authorization errors
      if (accessError?.statusCode === 403) {
        console.warn(`⚠️ Unauthorized SSE access attempt: ${username} tried to access session ${sessionId}`);
        sendSSE(res, 'error', { message: 'Unauthorized to access this session' });
        res.end();
        return;
      }
      
      // Handle CosmosDB connection errors
      const errorMessage = accessError instanceof Error ? accessError.message : String(accessError);
      if (errorMessage.includes('ECONNREFUSED') || errorMessage.includes('ETIMEDOUT') || accessError.code === 'ECONNREFUSED') {
        console.error('❌ CosmosDB connection error in streamChatMessages:', errorMessage.substring(0, 100));
        sendSSE(res, 'error', { 
          message: 'Database connection issue. Please try again in a moment.' 
        });
        res.end();
        return;
      }
      
      // Re-throw if it's not an authorization or connection error
      throw accessError;
    }
    
    if (!chatDocument) {
      sendSSE(res, 'error', { message: 'Session not found' });
      res.end();
      return;
    }

    // Helper function to enrich messages with chart data
    const enrichMessagesWithCharts = async (messages: any[], chartsWithData: any[]): Promise<any[]> => {
      // Build lookup map: chart title+type -> full chart with data
      const chartLookup = new Map<string, any>();
      chartsWithData.forEach(chart => {
        if (chart.title && chart.type) {
          const key = `${chart.type}::${chart.title}`;
          chartLookup.set(key, chart);
        }
      });

      // Enrich message charts with data from top-level charts
      return messages.map(msg => {
        if (!msg.charts || msg.charts.length === 0) {
          return msg;
        }

        const enrichedCharts = msg.charts.map((chart: any) => {
          const key = `${chart.type}::${chart.title}`;
          const fullChart = chartLookup.get(key);
          
          if (fullChart && fullChart.data) {
            return {
              ...chart,
              data: fullChart.data,
              trendLine: fullChart.trendLine,
              xDomain: fullChart.xDomain,
              yDomain: fullChart.yDomain,
            };
          }
          
          return chart;
        });

        return {
          ...msg,
          charts: enrichedCharts,
        };
      });
    };

    // Load charts from blob if needed
    let chartsWithData = chatDocument.charts || [];
    if (chatDocument.chartReferences && chatDocument.chartReferences.length > 0) {
      try {
        const chartsFromBlob = await loadChartsFromBlob(chatDocument.chartReferences);
        if (chartsFromBlob.length > 0) {
          chartsWithData = chartsFromBlob;
        }
      } catch (blobError) {
        console.error('⚠️ Failed to load charts from blob in SSE:', blobError);
      }
    }

    // Also include charts from CosmosDB that might have data
    (chatDocument.charts || []).forEach(chart => {
      if (chart.data && chart.title && chart.type) {
        const key = `${chart.type}::${chart.title}`;
        if (!chartsWithData.find(c => `${c.type}::${c.title}` === key)) {
          chartsWithData.push(chart);
        }
      }
    });

    let lastMessageCount = chatDocument.messages?.length || 0;

    // Function to fetch and send new messages
    const sendMessageUpdate = async () => {
      // Check if connection is still open
      if (res.writableEnded || res.destroyed || !res.writable) {
        return false;
      }

      try {
        const currentChat = await getChatBySessionIdEfficient(sessionId);
        if (!currentChat) {
          return true; // Connection still valid, just no chat found
        }

        // Reload charts if needed
        let currentChartsWithData = currentChat.charts || [];
        if (currentChat.chartReferences && currentChat.chartReferences.length > 0) {
          try {
            const chartsFromBlob = await loadChartsFromBlob(currentChat.chartReferences);
            if (chartsFromBlob.length > 0) {
              currentChartsWithData = chartsFromBlob;
            }
          } catch (blobError) {
            // Continue with CosmosDB charts
          }
        }

        const currentMessageCount = currentChat.messages?.length || 0;
        
        // Only send update if message count changed
        if (currentMessageCount !== lastMessageCount) {
          const newMessages = currentChat.messages?.slice(lastMessageCount) || [];
          lastMessageCount = currentMessageCount;
          
          // Enrich new messages with chart data
          const enrichedMessages = await enrichMessagesWithCharts(newMessages, currentChartsWithData);
          
          const sent = sendSSE(res, 'messages', {
            messages: enrichedMessages,
            totalCount: currentMessageCount,
          });
          
          if (!sent) {
            return false; // Connection closed
          }
        }
        return true;
      } catch (error) {
        // Only try to send error if connection is still open
        if (!res.writableEnded && !res.destroyed && res.writable) {
          console.error('Error fetching chat messages for SSE:', error);
          sendSSE(res, 'error', { 
            message: error instanceof Error ? error.message : 'Failed to fetch messages.' 
          });
        }
        return false;
      }
    };

    // Enrich initial messages with chart data
    const enrichedInitialMessages = await enrichMessagesWithCharts(
      chatDocument.messages || [],
      chartsWithData
    );

    // Send initial message count
    sendSSE(res, 'init', {
      messageCount: lastMessageCount,
      messages: enrichedInitialMessages,
    });

    // Set up polling to check for new messages every 2 seconds
    const checkInterval = setInterval(async () => {
      // Check if connection is still open
      if (res.writableEnded || res.destroyed || !res.writable) {
        clearInterval(checkInterval);
        return;
      }

      const stillConnected = await sendMessageUpdate();
      if (!stillConnected) {
        clearInterval(checkInterval);
        try {
          res.end();
        } catch (e) {
          // Ignore errors when ending already closed connection
        }
      }
    }, 2000); // Check every 2 seconds

    // Clean up on client disconnect
    req.on('close', () => {
      clearInterval(checkInterval);
      try {
        if (!res.writableEnded && !res.destroyed) {
          res.end();
        }
      } catch (e) {
        // Ignore errors when ending already closed connection
      }
    });

    // Handle errors - only log unexpected errors
    req.on('error', (error: any) => {
      // ECONNRESET is expected when clients disconnect normally
      if (error.code !== 'ECONNRESET' && error.code !== 'EPIPE' && error.code !== 'ECONNABORTED') {
        console.error('SSE connection error:', error);
      }
      clearInterval(checkInterval);
      try {
        if (!res.writableEnded && !res.destroyed) {
          res.end();
        }
      } catch (e) {
        // Ignore errors when ending already closed connection
      }
    });

  } catch (error) {
    console.error("streamChatMessages error:", error);
    const message = error instanceof Error ? error.message : "Failed to stream chat messages.";
    sendSSE(res, 'error', { message });
    res.end();
  }
}

