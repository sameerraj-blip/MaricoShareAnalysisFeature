import { Request, Response } from "express";
import { 
  getUserChats, 
  getChatDocument, 
  getChatBySessionIdForUser,
} from "../models/chat.model.js";

// Get all analysis sessions for a user
export const getUserAnalysisSessions = async (req: Request, res: Response) => {
  try {
    const username = req.params.username || req.headers['x-user-email'] || req.query.username;
    
    if (!username) {
      return res.status(400).json({ error: 'Username is required' });
    }

    const chats = await getUserChats(username as string);
    
    // Return summary information for each chat (without full raw data)
    const sessions = chats.map(chat => ({
      id: chat.id,
      fileName: chat.fileName,
      uploadedAt: chat.uploadedAt,
      createdAt: chat.createdAt,
      lastUpdatedAt: chat.lastUpdatedAt,
      collaborators: chat.collaborators || [chat.username],
      dataSummary: chat.dataSummary,
      chartsCount: chat.charts.length,
      insightsCount: chat.insights?.length || 0,
      messagesCount: chat.messages.length,
      blobInfo: chat.blobInfo,
      analysisMetadata: chat.analysisMetadata,
      sessionId: chat.sessionId
    }));

    res.json({
      sessions,
      totalCount: sessions.length
    });
  } catch (error) {
    console.error('Error getting user analysis sessions:', error);
    const statusCode = (error as any)?.statusCode || 500;
    res.status(statusCode).json({
      error: error instanceof Error ? error.message : 'Failed to retrieve analysis sessions'
    });
  }
};

// Get complete analysis data for a specific chat
export const getAnalysisData = async (req: Request, res: Response) => {
  try {
    const { chatId } = req.params;
    const username = req.query.username || req.headers['x-user-email'];
    
    if (!username) {
      return res.status(400).json({ error: 'Username is required' });
    }

    const chatDocument = await getChatDocument(chatId, username as string);
    
    if (!chatDocument) {
      return res.status(404).json({ error: 'Analysis data not found' });
    }

    // Return complete analysis data
    res.json({
      id: chatDocument.id,
      fileName: chatDocument.fileName,
      uploadedAt: chatDocument.uploadedAt,
      createdAt: chatDocument.createdAt,
      lastUpdatedAt: chatDocument.lastUpdatedAt,
      collaborators: chatDocument.collaborators || [chatDocument.username],
      dataSummary: chatDocument.dataSummary,
      rawData: chatDocument.rawData,
      sampleRows: chatDocument.sampleRows,
      // Preview can be generated from rawData.slice(0, 50) on the frontend
      columnStatistics: chatDocument.columnStatistics,
      charts: chatDocument.charts,
      insights: chatDocument.insights || [],
      messages: chatDocument.messages,
      blobInfo: chatDocument.blobInfo,
      analysisMetadata: chatDocument.analysisMetadata,
      sessionId: chatDocument.sessionId
    });
  } catch (error) {
    console.error('Error getting analysis data:', error);
    const statusCode = (error as any)?.statusCode || 500;
    res.status(statusCode).json({
      error: error instanceof Error ? error.message : 'Failed to retrieve analysis data'
    });
  }
};

// Get analysis data by session ID
export const getAnalysisDataBySession = async (req: Request, res: Response) => {
  try {
    const { sessionId } = req.params;
    const username = req.query.username || req.headers['x-user-email'];
    
    if (!username) {
      return res.status(401).json({ error: 'Username is required' });
    }

    const chatDocument = await getChatBySessionIdForUser(sessionId, username as string);
    
    if (!chatDocument) {
      return res.status(404).json({ error: 'Analysis data not found for this session' });
    }

    // Return complete analysis data
    res.json({
      id: chatDocument.id,
      fileName: chatDocument.fileName,
      uploadedAt: chatDocument.uploadedAt,
      createdAt: chatDocument.createdAt,
      lastUpdatedAt: chatDocument.lastUpdatedAt,
      collaborators: chatDocument.collaborators || [chatDocument.username],
      dataSummary: chatDocument.dataSummary,
      rawData: chatDocument.rawData,
      sampleRows: chatDocument.sampleRows,
      // Preview can be generated from rawData.slice(0, 50) on the frontend
      columnStatistics: chatDocument.columnStatistics,
      charts: chatDocument.charts,
      insights: chatDocument.insights || [],
      messages: chatDocument.messages,
      blobInfo: chatDocument.blobInfo,
      analysisMetadata: chatDocument.analysisMetadata,
      sessionId: chatDocument.sessionId
    });
  } catch (error) {
    console.error('Error getting analysis data by session:', error);
    const statusCode = (error as any)?.statusCode || 500;
    res.status(statusCode).json({
      error: error instanceof Error ? error.message : 'Failed to retrieve analysis data'
    });
  }
};

// Get column statistics for a specific analysis
export const getColumnStatistics = async (req: Request, res: Response) => {
  try {
    const { chatId } = req.params;
    const username = req.query.username || req.headers['x-user-email'];
    
    if (!username) {
      return res.status(400).json({ error: 'Username is required' });
    }

    const chatDocument = await getChatDocument(chatId, username as string);
    
    if (!chatDocument) {
      return res.status(404).json({ error: 'Analysis data not found' });
    }

    res.json({
      chatId: chatDocument.id,
      fileName: chatDocument.fileName,
      columnStatistics: chatDocument.columnStatistics,
      numericColumns: chatDocument.dataSummary.numericColumns,
      totalNumericColumns: Object.keys(chatDocument.columnStatistics).length
    });
  } catch (error) {
    console.error('Error getting column statistics:', error);
    const statusCode = (error as any)?.statusCode || 500;
    res.status(statusCode).json({
      error: error instanceof Error ? error.message : 'Failed to retrieve column statistics'
    });
  }
};

// Get raw data for a specific analysis (with pagination)
export const getRawData = async (req: Request, res: Response) => {
  try {
    const { chatId } = req.params;
    const username = req.query.username || req.headers['x-user-email'];
    const page = parseInt(req.query.page as string) || 1;
    const limit = parseInt(req.query.limit as string) || 100;
    
    if (!username) {
      return res.status(400).json({ error: 'Username is required' });
    }

    const chatDocument = await getChatDocument(chatId, username as string);
    
    if (!chatDocument) {
      return res.status(404).json({ error: 'Analysis data not found' });
    }

    const startIndex = (page - 1) * limit;
    const endIndex = startIndex + limit;
    const paginatedData = chatDocument.rawData.slice(startIndex, endIndex);

    res.json({
      chatId: chatDocument.id,
      fileName: chatDocument.fileName,
      data: paginatedData,
      pagination: {
        page,
        limit,
        totalRows: chatDocument.rawData.length,
        totalPages: Math.ceil(chatDocument.rawData.length / limit),
        hasNextPage: endIndex < chatDocument.rawData.length,
        hasPrevPage: page > 1
      }
    });
  } catch (error) {
    console.error('Error getting raw data:', error);
    const statusCode = (error as any)?.statusCode || 500;
    res.status(statusCode).json({
      error: error instanceof Error ? error.message : 'Failed to retrieve raw data'
    });
  }
};
