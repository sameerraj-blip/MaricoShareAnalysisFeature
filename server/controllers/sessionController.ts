import { Request, Response } from "express";
import { 
  getAllSessions, 
  getAllSessionsPaginated, 
  getSessionsWithFilters, 
  getSessionStatistics,
  getChatBySessionIdForUser,
  deleteSessionBySessionId,
  updateSessionFileName,
  ChatDocument 
} from "../models/chat.model.js";
import { loadChartsFromBlob } from "../lib/blobStorage.js";

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
      collaborators: session.collaborators || [session.username],
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
      collaborators: session.collaborators || [session.username],
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
      collaborators: session.collaborators || [session.username],
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
    const requesterEmail = req.headers['x-user-email'] || req.query.username;
    
    if (!sessionId) {
      return res.status(400).json({ error: 'Session ID is required' });
    }

    if (!requesterEmail) {
      return res.status(401).json({ error: 'Missing authenticated user email' });
    }

    // Normalize email for consistent comparison
    const normalizedRequesterEmail = (requesterEmail as string).trim().toLowerCase();

    // Get session directly from CosmosDB by session ID with access check
    try {
      const session = await getChatBySessionIdForUser(sessionId, normalizedRequesterEmail);
      
      if (!session) {
        return res.status(404).json({ error: 'Session not found' });
      }

      // Load charts from blob storage if they're stored there
      let chartsWithData = session.charts || [];
      if (session.chartReferences && session.chartReferences.length > 0) {
        try {
          const chartsFromBlob = await loadChartsFromBlob(session.chartReferences);
          // Merge charts from blob with charts in CosmosDB (charts in CosmosDB may have metadata only)
          // Use charts from blob if available, otherwise use charts from CosmosDB
          if (chartsFromBlob.length > 0) {
            chartsWithData = chartsFromBlob;
            console.log(`✅ Loaded ${chartsFromBlob.length} charts from blob storage`);
          }
        } catch (blobError) {
          console.error('⚠️ Failed to load charts from blob, using charts from CosmosDB:', blobError);
          // Continue with charts from CosmosDB (may not have data arrays)
        }
      }

      // Build a lookup map: chart title+type -> full chart with data
      // This allows us to enrich message charts with data from top-level charts
      const chartLookup = new Map<string, any>();
      chartsWithData.forEach(chart => {
        if (chart.title && chart.type) {
          const key = `${chart.type}::${chart.title}`;
          chartLookup.set(key, chart);
        }
      });

      // Also check charts in CosmosDB that might have data (for small charts not in blob)
      (session.charts || []).forEach(chart => {
        if (chart.title && chart.type && chart.data) {
          const key = `${chart.type}::${chart.title}`;
          if (!chartLookup.has(key)) {
            chartLookup.set(key, chart);
          }
        }
      });

      // Enrich message charts with data from top-level charts
      const enrichedMessages = (session.messages || []).map(msg => {
        if (!msg.charts || msg.charts.length === 0) {
          return msg;
        }

        const enrichedCharts = msg.charts.map(chart => {
          const key = `${chart.type}::${chart.title}`;
          const fullChart = chartLookup.get(key);
          
          if (fullChart && fullChart.data) {
            // Merge metadata from message chart with data from top-level chart
            return {
              ...chart,
              data: fullChart.data,
              trendLine: fullChart.trendLine,
              xDomain: fullChart.xDomain,
              yDomain: fullChart.yDomain,
            };
          }
          
          // If no match found, return chart as-is (might have data already or be a small chart)
          return chart;
        });

        return {
          ...msg,
          charts: enrichedCharts,
        };
      });

      console.log(`✅ Enriched ${enrichedMessages.length} messages with chart data`);

      // Return session with charts loaded from blob and messages enriched with chart data
      const sessionWithCharts = {
        ...session,
        charts: chartsWithData,
        messages: enrichedMessages,
      };

      res.json({
        session: sessionWithCharts,
        message: `Retrieved session details for ${sessionId}`
      });
    } catch (accessError: any) {
      // Handle authorization errors separately
      if (accessError?.statusCode === 403) {
        console.warn(`⚠️ Unauthorized access attempt: ${requesterEmail} tried to access session ${sessionId}`);
        return res.status(403).json({ 
          error: 'Unauthorized to access this session',
          message: 'You do not have permission to access this session'
        });
      }
      // Re-throw if it's not an authorization error
      throw accessError;
    }
  } catch (error) {
    console.error('Get session details error:', error);
    const statusCode = (error as any)?.statusCode || 500;
    res.status(statusCode).json({
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
      collaborators: session.collaborators || [session.username],
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

// Update session fileName by session ID
export const updateSessionNameEndpoint = async (req: Request, res: Response) => {
  try {
    const { sessionId } = req.params;
    const { fileName } = req.body;
    
    if (!sessionId) {
      return res.status(400).json({ error: 'Session ID is required' });
    }
    
    if (!fileName || typeof fileName !== 'string' || fileName.trim().length === 0) {
      return res.status(400).json({ error: 'File name is required' });
    }

    // Get username from headers or query parameters
    const username = req.headers['x-user-email'] || req.query.username;
    
    if (!username) {
      return res.status(400).json({ 
        error: 'Username is required. Please ensure you are logged in.' 
      });
    }

    // Update the session fileName
    const updatedSession = await updateSessionFileName(sessionId, username as string, fileName.trim());
    
    res.json({
      success: true,
      message: `Session name updated successfully`,
      session: {
        id: updatedSession.id,
        sessionId: updatedSession.sessionId,
        fileName: updatedSession.fileName,
        lastUpdatedAt: updatedSession.lastUpdatedAt,
      }
    });
  } catch (error) {
    console.error('Update session name error:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to update session name';
    
    // Check if it's a "not found" error
    if (errorMessage.includes('not found') || errorMessage.includes('Session not found')) {
      return res.status(404).json({
        error: errorMessage
      });
    }
    
    // Check if it's an unauthorized error
    if (errorMessage.includes('Unauthorized')) {
      return res.status(403).json({
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
