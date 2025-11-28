/**
 * Data Ops Controller
 * Thin controller layer for data operations endpoints - delegates to services
 */
import { Request, Response } from "express";
import { processDataOperation } from "../services/dataOps/dataOps.service.js";
import { processStreamDataOperation } from "../services/dataOps/dataOpsStream.service.js";
import { requireUsername } from "../utils/auth.helper.js";
import { sendError, sendValidationError, sendNotFound } from "../utils/responseFormatter.js";

/**
 * Non-streaming Data Ops chat endpoint
 */
export const dataOpsChatWithAI = async (req: Request, res: Response) => {
  try {
    console.log('📨 dataOpsChatWithAI() called');
    const { sessionId, message, chatHistory, dataOpsMode } = req.body;
    const username = requireUsername(req);

    // Validate required fields
    if (!sessionId || !message) {
      return sendValidationError(res, 'Missing required fields');
    }

    // Process data operation
    const result = await processDataOperation({
      sessionId,
      message,
      chatHistory,
      dataOpsMode,
      username,
    });

    res.json(result);
  } catch (error) {
    console.error('Data Ops chat error:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to process data operation';
    sendError(res, errorMessage);
  }
};

/**
 * Streaming Data Ops chat endpoint using Server-Sent Events (SSE)
 */
export const dataOpsChatWithAIStream = async (req: Request, res: Response) => {
  try {
    console.log('📨 dataOpsChatWithAIStream() called');
    const { sessionId, message, chatHistory, dataOpsMode } = req.body;
    const username = requireUsername(req);

    // Validate required fields
    if (!sessionId || !message) {
      return;
    }
    
    // Process streaming data operation
    await processStreamDataOperation({
      sessionId,
      message,
      chatHistory,
      dataOpsMode,
      username,
      res,
    });
  } catch (error) {
    console.error('Data Ops stream error:', error);
    // Error handling is done in the service
  }
};
