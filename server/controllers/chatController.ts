/**
 * Chat Controller
 * Thin controller layer for chat endpoints - delegates to services
 */
import { Request, Response } from "express";
import type { Request as ExpressRequest } from "express";
import { processChatMessage } from "../services/chat/chat.service.js";
import { createErrorResponse } from "../services/chat/chatResponse.service.js";
import { processStreamChat, streamChatMessages } from "../services/chat/chatStream.service.js";
import { requireUsername, extractUsername } from "../utils/auth.helper.js";
import { sendError, sendValidationError, sendNotFound } from "../utils/responseFormatter.js";

/**
 * Non-streaming chat endpoint
 */
export const chatWithAI = async (req: Request, res: Response) => {
  try {
    console.log('📨 chatWithAI() called');
    const { sessionId, message, chatHistory, targetTimestamp } = req.body;
    const username = requireUsername(req);

    console.log('📥 Request body:', { sessionId, message: message?.substring(0, 50), chatHistoryLength: chatHistory?.length, targetTimestamp });

    // Validate required fields
    if (!sessionId || !message) {
      console.log('❌ Missing required fields');
      return sendValidationError(res, 'Missing required fields');
    }

    // Process chat message
    const result = await processChatMessage({
      sessionId,
      message,
      chatHistory,
      targetTimestamp,
      username,
    });

    console.log('📨 Sending response to client:', {
      answerLength: result.answer.length,
      chartsCount: result.charts?.length || 0,
      insightsCount: result.insights?.length || 0,
      suggestionsCount: result.suggestions?.length || 0,
    });

    res.json(result);
    console.log('✅ Response sent successfully');
  } catch (error) {
    console.error('Chat error:', error);
    const errorResponse = createErrorResponse(error as Error);
    res.status(500).json(errorResponse);
  }
};

/**
 * Streaming chat endpoint using Server-Sent Events (SSE)
 */
export const chatWithAIStream = async (req: Request, res: Response) => {
  try {
    console.log('📨 chatWithAIStream() called');
    const { sessionId, message, chatHistory, targetTimestamp, mode } = req.body;
    const username = requireUsername(req);

    console.log('📥 Request body:', { sessionId, message: message?.substring(0, 50), chatHistoryLength: chatHistory?.length, targetTimestamp, mode });

    // Validate required fields
    if (!sessionId || !message) {
      console.log('❌ Missing required fields');
      return;
    }

    // Validate mode if provided (treat 'general' as undefined for auto-detection)
    const validMode = mode && ['general', 'analysis', 'dataOps', 'modeling'].includes(mode) 
      ? (mode === 'general' ? undefined : mode)
      : undefined;

    // Process streaming chat
    await processStreamChat({
      sessionId,
      message,
      chatHistory,
      targetTimestamp,
      username,
      res,
      mode: validMode,
    });
  } catch (error) {
    console.error('Chat stream error:', error);
    // Error handling is done in the service
  }
};

/**
 * Streaming chat messages endpoint using Server-Sent Events (SSE)
 * Provides real-time updates for chat messages in a session
 */
export const streamChatMessagesController = async (req: Request, res: Response) => {
  try {
    const { sessionId } = req.params;
    
    // Extract username from query or header
    const queryEmail = req.query.username;
    const headerEmail = req.headers['x-user-email'];
    
    let username: string | undefined;
    if (typeof queryEmail === "string" && queryEmail.trim().length > 0) {
      username = queryEmail.trim().toLowerCase();
    } else if (typeof headerEmail === "string" && headerEmail.trim().length > 0) {
      username = headerEmail.trim().toLowerCase();
    }

    if (!sessionId) {
      return;
    }

    if (!username) {
      return;
    }

    // Stream messages
    await streamChatMessages(sessionId, username, req as ExpressRequest, res);
  } catch (error) {
    console.error("streamChatMessagesController error:", error);
    // Error handling is done in the service
  }
};
