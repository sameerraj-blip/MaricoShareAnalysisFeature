import { Request, Response } from "express";
import { 
  getAllSessions, 
  getAllSessionsPaginated, 
  getSessionsWithFilters, 
  getSessionStatistics,
  getChatBySessionIdEfficient,
  deleteSessionBySessionId,
  ChatDocument 
} from "../lib/cosmosDB.js";

// Get all sessions
export const getAllSessionsEndpoint = async (req: Request, res: Response) => {
  try {
    // Get username from headers or query parameters
    const username = req.headers['x-user-email'] || req.query.username;
    
    if (!username) {
      return res.status(400).json({ 
        error: 'Username is required. Please ensure you are logged in.' 
      });
    }

    const sessions = await getAllSessions(username as string);
    
    // Return simplified session list for better performance
    const sessionList = sessions.map(session => ({
      id: session.id,
      username: session.username,
      fileName: session.fileName,
      uploadedAt: session.uploadedAt,
      createdAt: session.createdAt,
      lastUpdatedAt: session.lastUpdatedAt,
      messageCount: session.messages.length,
      chartCount: session.charts.length,
      sessionId: session.sessionId,
    }));

    res.json({ 
      sessions: sessionList, 
      count: sessionList.length,
      message: `Retrieved ${sessionList.length} sessions for user: ${username}`
    });
  } catch (error) {
    console.error('Get all sessions error:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to fetch all sessions';
    
    // Check if it's a CosmosDB initialization error
    if (errorMessage.includes('not initialized')) {
      return res.status(503).json({
        error: 'Database is initializing. Please try again in a moment.',
        retryAfter: 2
      });
    }
    
    res.status(500).json({
      error: errorMessage,
    });
  }
};

// Get sessions with pagination
export const getSessionsPaginatedEndpoint = async (req: Request, res: Response) => {
  try {
    const pageSize = parseInt(req.query.pageSize as string) || 10;
    const continuationToken = req.query.continuationToken as string;
    
    // Get username from headers or query parameters
    const username = req.headers['x-user-email'] || req.query.username;
    
    if (!username) {
      return res.status(400).json({ 
        error: 'Username is required. Please ensure you are logged in.' 
      });
    }

    const result = await getAllSessionsPaginated(pageSize, continuationToken, username as string);
    
    // Return simplified session list
    const sessionList = result.sessions.map(session => ({
      id: session.id,
      username: session.username,
      fileName: session.fileName,
      uploadedAt: session.uploadedAt,
      createdAt: session.createdAt,
      lastUpdatedAt: session.lastUpdatedAt,
      messageCount: session.messages.length,
      chartCount: session.charts.length,
      sessionId: session.sessionId,
    }));

    res.json({
      sessions: sessionList,
      count: sessionList.length,
      continuationToken: result.continuationToken,
      hasMoreResults: result.hasMoreResults,
      pageSize,
      message: `Retrieved ${sessionList.length} sessions (page size: ${pageSize}) for user: ${username}`
    });
  } catch (error) {
    console.error('Get paginated sessions error:', error);
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Failed to fetch paginated sessions',
    });
  }
};

// Get sessions with filters
export const getSessionsFilteredEndpoint = async (req: Request, res: Response) => {
  try {
    const {
      username,
      fileName,
      dateFrom,
      dateTo,
      limit,
      orderBy,
      orderDirection
    } = req.query;

    const options: {
      username?: string;
      fileName?: string;
      dateFrom?: number;
      dateTo?: number;
      limit?: number;
      orderBy?: 'createdAt' | 'lastUpdatedAt' | 'uploadedAt';
      orderDirection?: 'ASC' | 'DESC';
    } = {};

    if (username) options.username = username as string;
    if (fileName) options.fileName = fileName as string;
    if (dateFrom) options.dateFrom = parseInt(dateFrom as string);
    if (dateTo) options.dateTo = parseInt(dateTo as string);
    if (limit) options.limit = parseInt(limit as string);
    if (orderBy) options.orderBy = orderBy as 'createdAt' | 'lastUpdatedAt' | 'uploadedAt';
    if (orderDirection) options.orderDirection = orderDirection as 'ASC' | 'DESC';

    const sessions = await getSessionsWithFilters(options);
    
    // Return simplified session list
    const sessionList = sessions.map(session => ({
      id: session.id,
      username: session.username,
      fileName: session.fileName,
      uploadedAt: session.uploadedAt,
      createdAt: session.createdAt,
      lastUpdatedAt: session.lastUpdatedAt,
      messageCount: session.messages.length,
      chartCount: session.charts.length,
      sessionId: session.sessionId,
    }));

    res.json({
      sessions: sessionList,
      count: sessionList.length,
      filters: options,
      message: `Retrieved ${sessionList.length} sessions with filters`
    });
  } catch (error) {
    console.error('Get filtered sessions error:', error);
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Failed to fetch filtered sessions',
    });
  }
};

// Get session statistics
export const getSessionStatisticsEndpoint = async (req: Request, res: Response) => {
  try {
    const stats = await getSessionStatistics();
    
    res.json({
      statistics: stats,
      message: `Generated statistics for ${stats.totalSessions} sessions`
    });
  } catch (error) {
    console.error('Get session statistics error:', error);
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Failed to fetch session statistics',
    });
  }
};

// Get detailed session by session ID (efficient)
export const getSessionDetailsEndpoint = async (req: Request, res: Response) => {
  try {
    const { sessionId } = req.params;
    
    if (!sessionId) {
      return res.status(400).json({ error: 'Session ID is required' });
    }

    // Get session directly from CosmosDB by session ID
    const session = await getChatBySessionIdEfficient(sessionId);
    
    if (!session) {
      return res.status(404).json({ error: 'Session not found' });
    }

    res.json({
      session,
      message: `Retrieved session details for ${sessionId}`
    });
  } catch (error) {
    console.error('Get session details error:', error);
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Failed to fetch session details',
    });
  }
};

// Get sessions by user
export const getSessionsByUserEndpoint = async (req: Request, res: Response) => {
  try {
    const { username } = req.params;
    
    if (!username) {
      return res.status(400).json({ error: 'Username is required' });
    }

    const sessions = await getSessionsWithFilters({ username });
    
    // Return simplified session list
    const sessionList = sessions.map(session => ({
      id: session.id,
      username: session.username,
      fileName: session.fileName,
      uploadedAt: session.uploadedAt,
      createdAt: session.createdAt,
      lastUpdatedAt: session.lastUpdatedAt,
      messageCount: session.messages.length,
      chartCount: session.charts.length,
      sessionId: session.sessionId,
    }));

    res.json({
      sessions: sessionList,
      count: sessionList.length,
      username,
      message: `Retrieved ${sessionList.length} sessions for user ${username}`
    });
  } catch (error) {
    console.error('Get sessions by user error:', error);
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Failed to fetch sessions by user',
    });
  }
};

// Delete session by session ID
export const deleteSessionEndpoint = async (req: Request, res: Response) => {
  try {
    const { sessionId } = req.params;
    
    if (!sessionId) {
      return res.status(400).json({ error: 'Session ID is required' });
    }

    // Get username from headers or query parameters
    const username = req.headers['x-user-email'] || req.query.username;
    
    if (!username) {
      return res.status(400).json({ 
        error: 'Username is required. Please ensure you are logged in.' 
      });
    }

    // Delete the session
    await deleteSessionBySessionId(sessionId, username as string);
    
    res.json({
      success: true,
      message: `Session ${sessionId} deleted successfully`,
      sessionId
    });
  } catch (error) {
    console.error('Delete session error:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to delete session';
    
    // Check if it's a "not found" error
    if (errorMessage.includes('not found') || errorMessage.includes('Session not found')) {
      return res.status(404).json({
        error: errorMessage
      });
    }
    
    // Check if it's a CosmosDB initialization error
    if (errorMessage.includes('not initialized')) {
      return res.status(503).json({
        error: 'Database is initializing. Please try again in a moment.',
        retryAfter: 2
      });
    }
    
    res.status(500).json({
      error: errorMessage
    });
  }
};
