import { Request, Response } from "express";
import {
  sharedAnalysesResponseSchema,
  AnalysisSessionSummary,
} from "../../shared/schema.js";
import {
  acceptSharedAnalysisInvite,
  createSharedAnalysisInvite,
  declineSharedAnalysisInvite,
  getSharedAnalysisInviteById,
  listSharedAnalysesForOwner,
  listSharedAnalysesForUser,
  type ChatDocument,
} from "../lib/cosmosDB.js";

const getUserEmailFromRequest = (req: Request): string | undefined => {
  const headerEmail = req.headers["x-user-email"];
  if (typeof headerEmail === "string" && headerEmail.trim().length > 0) {
    return headerEmail.toLowerCase();
  }
  const queryEmail = req.query.username;
  if (typeof queryEmail === "string" && queryEmail.trim().length > 0) {
    return queryEmail.toLowerCase();
  }
  return undefined;
};

const toSessionSummary = (chatDocument: ChatDocument): AnalysisSessionSummary => ({
  id: chatDocument.id,
  fileName: chatDocument.fileName,
  uploadedAt: chatDocument.uploadedAt,
  createdAt: chatDocument.createdAt,
  lastUpdatedAt: chatDocument.lastUpdatedAt,
  collaborators: chatDocument.collaborators || [chatDocument.username],
  dataSummary: chatDocument.dataSummary,
  chartsCount: chatDocument.charts?.length || 0,
  insightsCount: chatDocument.insights?.length || 0,
  messagesCount: chatDocument.messages?.length || 0,
  blobInfo: chatDocument.blobInfo,
  analysisMetadata: chatDocument.analysisMetadata,
  sessionId: chatDocument.sessionId,
});

const sanitizeEmail = (value: string) => value.trim().toLowerCase();

// SSE helper function (similar to chatController)
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

export const shareAnalysisController = async (req: Request, res: Response) => {
  try {
    const ownerEmail = getUserEmailFromRequest(req);
    if (!ownerEmail) {
      return res.status(401).json({ error: "Missing authenticated user email." });
    }

    const { sessionId, targetEmail, note } = req.body || {};
    if (!sessionId || !targetEmail) {
      return res.status(400).json({ error: "sessionId and targetEmail are required." });
    }

    const invite = await createSharedAnalysisInvite({
      ownerEmail,
      targetEmail: sanitizeEmail(targetEmail),
      sourceSessionId: sessionId,
      note,
    });

    res.status(201).json({ invite });
  } catch (error) {
    console.error("shareAnalysisController error:", error);
    const message = error instanceof Error ? error.message : "Failed to share analysis.";
    const statusCode = (error as any)?.statusCode || 500;
    res.status(statusCode).json({ error: message });
  }
};

export const getIncomingSharedAnalysesController = async (req: Request, res: Response) => {
  try {
    const userEmail = getUserEmailFromRequest(req);
    if (!userEmail) {
      return res.status(401).json({ error: "Missing authenticated user email." });
    }

    const invitations = await listSharedAnalysesForUser(userEmail);
    const responsePayload = {
      pending: invitations.filter((invite) => invite.status === "pending"),
      accepted: invitations.filter((invite) => invite.status === "accepted"),
    };

    // Validate payload before sending (throws if invalid)
    sharedAnalysesResponseSchema.parse(responsePayload);
    res.json(responsePayload);
  } catch (error) {
    console.error("getIncomingSharedAnalysesController error:", error);
    const message = error instanceof Error ? error.message : "Failed to load shared analyses.";
    const statusCode = (error as any)?.statusCode || 500;
    res.status(statusCode).json({ error: message });
  }
};

export const getSentSharedAnalysesController = async (req: Request, res: Response) => {
  try {
    const userEmail = getUserEmailFromRequest(req);
    if (!userEmail) {
      return res.status(401).json({ error: "Missing authenticated user email." });
    }

    const invitations = await listSharedAnalysesForOwner(userEmail);
    res.json({ invitations });
  } catch (error) {
    console.error("getSentSharedAnalysesController error:", error);
    const message = error instanceof Error ? error.message : "Failed to load sent shared analyses.";
    const statusCode = (error as any)?.statusCode || 500;
    res.status(statusCode).json({ error: message });
  }
};

export const acceptSharedAnalysisController = async (req: Request, res: Response) => {
  try {
    const userEmail = getUserEmailFromRequest(req);
    if (!userEmail) {
      return res.status(401).json({ error: "Missing authenticated user email." });
    }

    const { inviteId } = req.params;
    if (!inviteId) {
      return res.status(400).json({ error: "inviteId is required." });
    }

    const { invite, newSession } = await acceptSharedAnalysisInvite(inviteId, userEmail);
    const summary = toSessionSummary(newSession);

    res.json({
      invite,
      acceptedSession: summary,
    });
  } catch (error) {
    console.error("acceptSharedAnalysisController error:", error);
    const message = error instanceof Error ? error.message : "Failed to accept shared analysis.";
    const statusCode = (error as any)?.statusCode || 500;
    res.status(statusCode).json({ error: message });
  }
};

export const declineSharedAnalysisController = async (req: Request, res: Response) => {
  try {
    const userEmail = getUserEmailFromRequest(req);
    if (!userEmail) {
      return res.status(401).json({ error: "Missing authenticated user email." });
    }

    const { inviteId } = req.params;
    if (!inviteId) {
      return res.status(400).json({ error: "inviteId is required." });
    }

    const invite = await declineSharedAnalysisInvite(inviteId, userEmail);
    res.json({ invite });
  } catch (error) {
    console.error("declineSharedAnalysisController error:", error);
    const message = error instanceof Error ? error.message : "Failed to decline shared analysis.";
    const statusCode = (error as any)?.statusCode || 500;
    res.status(statusCode).json({ error: message });
  }
};

export const getSharedAnalysisInviteController = async (req: Request, res: Response) => {
  try {
    const userEmail = getUserEmailFromRequest(req);
    if (!userEmail) {
      return res.status(401).json({ error: "Missing authenticated user email." });
    }

    const { inviteId } = req.params;
    if (!inviteId) {
      return res.status(400).json({ error: "inviteId is required." });
    }

    const invite = await getSharedAnalysisInviteById(inviteId, userEmail);
    if (!invite) {
      return res.status(404).json({ error: "Shared analysis invite not found." });
    }

    res.json({ invite });
  } catch (error) {
    console.error("getSharedAnalysisInviteController error:", error);
    const message = error instanceof Error ? error.message : "Failed to load shared analysis invite.";
    const statusCode = (error as any)?.statusCode || 500;
    res.status(statusCode).json({ error: message });
  }
};

/**
 * Streaming shared analyses endpoint using Server-Sent Events (SSE)
 * Provides real-time updates for incoming shared analysis invites
 */
export const streamIncomingSharedAnalysesController = async (req: Request, res: Response) => {
  // Set SSE headers
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no'); // Disable buffering for nginx

  try {
    const userEmail = getUserEmailFromRequest(req);
    if (!userEmail) {
      sendSSE(res, 'error', { message: 'Missing authenticated user email.' });
      res.end();
      return;
    }


    // Function to fetch and send shared analyses
    const sendSharedAnalyses = async () => {
      // Check if connection is still open before attempting to send
      if (res.writableEnded || res.destroyed || !res.writable) {
        return false;
      }

      try {
        const invitations = await listSharedAnalysesForUser(userEmail);
        const responsePayload = {
          pending: invitations.filter((invite) => invite.status === "pending"),
          accepted: invitations.filter((invite) => invite.status === "accepted"),
        };

        // Validate payload before sending
        sharedAnalysesResponseSchema.parse(responsePayload);
        const sent = sendSSE(res, 'update', responsePayload);
        
        if (!sent) {
          return false; // Connection closed
        }
        return true;
      } catch (error) {
        // Only try to send error if connection is still open
        if (!res.writableEnded && !res.destroyed && res.writable) {
          console.error('Error fetching shared analyses for SSE:', error);
          sendSSE(res, 'error', { 
            message: error instanceof Error ? error.message : 'Failed to fetch shared analyses.' 
          });
        }
        return false;
      }
    };

    // Send initial data immediately
    await sendSharedAnalyses();

    // Set up polling to check for new invites every 3 seconds
    const checkInterval = setInterval(async () => {
      // Check if connection is still open
      if (res.writableEnded || res.destroyed || !res.writable) {
        clearInterval(checkInterval);
        return;
      }

      const stillConnected = await sendSharedAnalyses();
      if (!stillConnected) {
        clearInterval(checkInterval);
        try {
          res.end();
        } catch (e) {
          // Ignore errors when ending already closed connection
        }
      }
    }, 3000); // Check every 3 seconds

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
    console.error("streamIncomingSharedAnalysesController error:", error);
    const message = error instanceof Error ? error.message : "Failed to stream shared analyses.";
    sendSSE(res, 'error', { message });
    res.end();
  }
};

