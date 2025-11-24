import { Request, Response } from "express";
import { answerQuestion } from "../lib/dataAnalyzer.js";
import { processChartData } from "../lib/chartGenerator.js";
import { generateChartInsights } from "../lib/insightGenerator.js";
import { chatResponseSchema, ThinkingStep } from "../../shared/schema.js";
import { getChatBySessionIdForUser, addMessagesBySessionId, getChatBySessionIdEfficient } from "../lib/cosmosDB.js";
import { generateAISuggestions } from '../lib/suggestionGenerator.js';

export const chatWithAI = async (req: Request, res: Response) => {
  try {
    console.log('📨 chatWithAI() called');
    const { sessionId, message, chatHistory } = req.body;
    const username = (req.body.username as string) || (req.headers['x-user-email'] as string);

    console.log('📥 Request body:', { sessionId, message: message?.substring(0, 50), chatHistoryLength: chatHistory?.length });

    if (!sessionId || !message) {
      console.log('❌ Missing required fields');
      return res.status(400).json({ error: 'Missing required fields' });
    }

    if (!username) {
      return res.status(401).json({ error: 'Missing authenticated user email' });
    }

    // Get chat document from CosmosDB by session ID
    console.log('🔍 Fetching chat document for sessionId:', sessionId);
    const chatDocument = await getChatBySessionIdForUser(sessionId, username);

    if (!chatDocument) {
      console.log('❌ Chat document not found');
      return res.status(404).json({ error: 'Session not found. Please upload a file first.' });
    }

    console.log('✅ Chat document found, calling answerQuestion()');
    // Get chat-level insights from the document to inform chart insights
    const chatLevelInsights = chatDocument.insights && Array.isArray(chatDocument.insights) && chatDocument.insights.length > 0
      ? chatDocument.insights
      : undefined;
    
    // Answer the question using data from CosmosDB
    const result = await answerQuestion(
      chatDocument.rawData, // Use the actual data stored in CosmosDB
      message,
      chatHistory || [],
      chatDocument.dataSummary,
      sessionId, // Pass sessionId for RAG
      chatLevelInsights // Pass chat insights to inform chart insights
    );

    // Ensure every chart has per-chart keyInsight and recommendation before validation
    // Use chat-level insights to inform chart insights (prefer result insights, fallback to document insights)
    const finalChatInsights = result.insights && Array.isArray(result.insights) && result.insights.length > 0
      ? result.insights
      : chatLevelInsights;
    
    if (result.charts && Array.isArray(result.charts)) {
      try {
        result.charts = await Promise.all(
          result.charts.map(async (c: any) => {
            const dataForChart = c.data && Array.isArray(c.data)
              ? c.data
              : processChartData(chatDocument.rawData, c);
            const insights = !('keyInsight' in c)
              ? await generateChartInsights(c, dataForChart, chatDocument.dataSummary, chatLevelInsights)
              : null;
            return {
              ...c,
              data: dataForChart,
              keyInsight: c.keyInsight ?? insights?.keyInsight,
            };
          })
        );
      } catch (e) {
        console.error('Final enrichment of chat charts failed:', e);
      }
    }

    // Validate response has answer
    if (!result || !result.answer || result.answer.trim().length === 0) {
      console.error('❌ Empty answer from answerQuestion:', result);
      return res.status(500).json({
        error: 'Failed to generate response. Please try again.',
        answer: "I'm sorry, I couldn't generate a response. Please try rephrasing your question.",
      });
    }
    
    console.log('✅ Answer generated:', result.answer.substring(0, 100));
    console.log('📤 Response being sent:', {
      answerLength: result.answer?.length,
      hasCharts: !!result.charts,
      chartsCount: result.charts?.length || 0,
      hasInsights: !!result.insights,
      insightsCount: result.insights?.length || 0,
    });
    
    // Validate response
    let validated = chatResponseSchema.parse(result);
    console.log('✅ Response validated successfully');

    // Ensure overall chat insights always present: derive from charts if missing
    if ((!validated.insights || validated.insights.length === 0) && Array.isArray(validated.charts) && validated.charts.length > 0) {
      try {
        const derived = validated.charts
          .map((c: any, idx: number) => {
            const text = c?.keyInsight || (c?.title ? `Insight: ${c.title}` : null);
            return text ? { id: idx + 1, text } : null;
          })
          .filter(Boolean) as { id: number; text: string }[];
        if (derived.length > 0) {
          validated = { ...validated, insights: derived } as any;
        }
      } catch {}
    }

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

    // Add suggestions to validated response
    validated = { ...validated, suggestions } as any;

    // Save messages to CosmosDB (by sessionId to avoid partition mismatches)
    try {
      const userEmail = username?.toLowerCase();
      await addMessagesBySessionId(sessionId, [
        {
          role: 'user',
          content: message,
          timestamp: Date.now(),
          userEmail: userEmail, // Add user email for shared analyses
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

    console.log('📨 Sending response to client:', {
      answerLength: validated.answer.length,
      chartsCount: validated.charts?.length || 0,
      insightsCount: validated.insights?.length || 0,
      suggestionsCount: suggestions.length,
    });
    res.json(validated);
    console.log('✅ Response sent successfully');
  } catch (error) {
    console.error('Chat error:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to process message';
    // Always return a valid response with an answer field
    res.status(500).json({
      error: errorMessage,
      answer: `I'm sorry, I encountered an error: ${errorMessage}. Please try again or rephrase your question.`,
      charts: [],
      insights: [],
    });
  }
};

/**
 * SSE helper function to send events
 * Safely handles client disconnections
 */
function sendSSE(res: Response, event: string, data: any): boolean {
  // Check if connection is still writable
  if (res.writableEnded || res.destroyed || !res.writable) {
    return false;
  }

  try {
    const message = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
    res.write(message);
    // Force flush the response (if supported by the platform)
    if (typeof (res as any).flush === 'function') {
      (res as any).flush();
    }
    console.log(`📤 SSE sent: ${event}`, data);
    return true;
  } catch (error: any) {
    // Ignore errors from client disconnections (ECONNRESET, EPIPE are expected)
    if (error.code === 'ECONNRESET' || error.code === 'EPIPE' || error.code === 'ECONNABORTED') {
      // Client disconnected - this is normal, don't log as error
      return false;
    }
    // Log unexpected errors
    console.error('Error sending SSE event:', error);
    return false;
  }
}

/**
 * Streaming chat endpoint using Server-Sent Events (SSE)
 */
export const chatWithAIStream = async (req: Request, res: Response) => {
  // Set SSE headers
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no'); // Disable buffering for nginx

  try {
    console.log('📨 chatWithAIStream() called');
    const { sessionId, message, chatHistory } = req.body;
    const username = (req.body.username as string) || (req.headers['x-user-email'] as string);

    console.log('📥 Request body:', { sessionId, message: message?.substring(0, 50), chatHistoryLength: chatHistory?.length });

    if (!sessionId || !message) {
      console.log('❌ Missing required fields');
      sendSSE(res, 'error', { message: 'Missing required fields' });
      res.end();
      return;
    }

    if (!username) {
      sendSSE(res, 'error', { message: 'Missing authenticated user email' });
      res.end();
      return;
    }

    // Get chat document from CosmosDB by session ID
    console.log('🔍 Fetching chat document for sessionId:', sessionId);
    const chatDocument = await getChatBySessionIdForUser(sessionId, username);

    if (!chatDocument) {
      console.log('❌ Chat document not found');
      sendSSE(res, 'error', { message: 'Session not found. Please upload a file first.' });
      res.end();
      return;
    }

    console.log('✅ Chat document found, calling answerQuestion()');
    // Get chat-level insights from the document to inform chart insights
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
    
    // Answer the question using data from CosmosDB with streaming
    const result = await answerQuestion(
      chatDocument.rawData, // Use the actual data stored in CosmosDB
      message,
      chatHistory || [],
      chatDocument.dataSummary,
      sessionId, // Pass sessionId for RAG
      chatLevelInsights, // Pass chat insights to inform chart insights
      onThinkingStep // Pass thinking step callback
    );

    // Ensure every chart has per-chart keyInsight and recommendation before validation
    // Use chat-level insights to inform chart insights (prefer result insights, fallback to document insights)
    const finalChatInsights = result.insights && Array.isArray(result.insights) && result.insights.length > 0
      ? result.insights
      : chatLevelInsights;
    
    if (result.charts && Array.isArray(result.charts)) {
      try {
        result.charts = await Promise.all(
          result.charts.map(async (c: any) => {
            const dataForChart = c.data && Array.isArray(c.data)
              ? c.data
              : processChartData(chatDocument.rawData, c);
            const insights = !('keyInsight' in c)
              ? await generateChartInsights(c, dataForChart, chatDocument.dataSummary, chatLevelInsights)
              : null;
            return {
              ...c,
              data: dataForChart,
              keyInsight: c.keyInsight ?? insights?.keyInsight,
            };
          })
        );
      } catch (e) {
        console.error('Final enrichment of chat charts failed:', e);
      }
    }

    // Validate response has answer
    if (!result || !result.answer || result.answer.trim().length === 0) {
      console.error('❌ Empty answer from answerQuestion:', result);
      sendSSE(res, 'error', { message: 'Failed to generate response. Please try again.' });
      res.end();
      return;
    }
    
    console.log('✅ Answer generated:', result.answer.substring(0, 100));
    console.log('📤 Response being sent:', {
      answerLength: result.answer?.length,
      hasCharts: !!result.charts,
      chartsCount: result.charts?.length || 0,
      hasInsights: !!result.insights,
      insightsCount: result.insights?.length || 0,
    });
    
    // Validate response
    let validated = chatResponseSchema.parse(result);
    console.log('✅ Response validated successfully');

    // Ensure overall chat insights always present: derive from charts if missing
    if ((!validated.insights || validated.insights.length === 0) && Array.isArray(validated.charts) && validated.charts.length > 0) {
      try {
        const derived = validated.charts
          .map((c: any, idx: number) => {
            const text = c?.keyInsight || (c?.title ? `Insight: ${c.title}` : null);
            return text ? { id: idx + 1, text } : null;
          })
          .filter(Boolean) as { id: number; text: string }[];
        if (derived.length > 0) {
          validated = { ...validated, insights: derived } as any;
        }
      } catch {}
    }

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

    // Add suggestions to validated response
    validated = { ...validated, suggestions } as any;

    // Save messages to CosmosDB (by sessionId to avoid partition mismatches)
    try {
      const userEmail = username?.toLowerCase();
      await addMessagesBySessionId(sessionId, [
        {
          role: 'user',
          content: message,
          timestamp: Date.now(),
          userEmail: userEmail, // Add user email for shared analyses
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

    // Send final response
    sendSSE(res, 'response', validated);
    sendSSE(res, 'done', {});
    res.end();
    console.log('✅ Stream completed successfully');
  } catch (error) {
    console.error('Chat stream error:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to process message';
    sendSSE(res, 'error', { message: errorMessage });
    res.end();
  }
};

/**
 * Streaming chat messages endpoint using Server-Sent Events (SSE)
 * Provides real-time updates for chat messages in a session
 */
export const streamChatMessagesController = async (req: Request, res: Response) => {
  // Set SSE headers
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');

  try {
    const { sessionId } = req.params;
    // EventSource doesn't support custom headers, so get email from query parameter
    const queryEmail = req.query.username;
    const headerEmail = req.headers['x-user-email'];
    
    let username: string | undefined;
    if (typeof queryEmail === "string" && queryEmail.trim().length > 0) {
      username = queryEmail.trim().toLowerCase();
    } else if (typeof headerEmail === "string" && headerEmail.trim().length > 0) {
      username = headerEmail.trim().toLowerCase();
    }

    if (!sessionId) {
      sendSSE(res, 'error', { message: 'Session ID is required' });
      res.end();
      return;
    }

    if (!username) {
      sendSSE(res, 'error', { message: 'Missing authenticated user email' });
      res.end();
      return;
    }

    // Verify user has access to this session
    const chatDocument = await getChatBySessionIdForUser(sessionId, username);
    if (!chatDocument) {
      sendSSE(res, 'error', { message: 'Session not found or unauthorized' });
      res.end();
      return;
    }

    let lastMessageCount = chatDocument.messages?.length || 0;

    // Function to fetch and send new messages
    const sendMessageUpdate = async () => {
      // Check if connection is still open before attempting to send
      if (res.writableEnded || res.destroyed || !res.writable) {
        return false;
      }

      try {
        const currentChat = await getChatBySessionIdEfficient(sessionId);
        if (!currentChat) {
          return true; // Connection still valid, just no chat found
        }

        const currentMessageCount = currentChat.messages?.length || 0;
        
        // Only send update if message count changed
        if (currentMessageCount !== lastMessageCount) {
          const newMessages = currentChat.messages?.slice(lastMessageCount) || [];
          lastMessageCount = currentMessageCount;
          
          const sent = sendSSE(res, 'messages', {
            messages: newMessages,
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

    // Send initial message count
    sendSSE(res, 'init', {
      messageCount: lastMessageCount,
      messages: chatDocument.messages || [],
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
    console.error("streamChatMessagesController error:", error);
    const message = error instanceof Error ? error.message : "Failed to stream chat messages.";
    sendSSE(res, 'error', { message });
    res.end();
  }
};
