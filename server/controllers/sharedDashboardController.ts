import { Request, Response } from "express";
import {
  sharedDashboardsResponseSchema,
} from "../shared/schema.js";
import {
  acceptSharedDashboardInvite,
  createSharedDashboardInvite,
  declineSharedDashboardInvite,
  getSharedDashboardInviteById,
  listSharedDashboardsForOwner,
  listSharedDashboardsForUser,
} from "../models/sharedDashboard.model.js";
import type { Dashboard } from "../models/dashboard.model.js";

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

const sanitizeEmail = (value: string) => value.trim().toLowerCase();

// SSE helper function (similar to sharedAnalysisController)
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

export const shareDashboardController = async (req: Request, res: Response) => {
  try {
    const ownerEmail = getUserEmailFromRequest(req);
    if (!ownerEmail) {
      return res.status(401).json({ error: "Missing authenticated user email." });
    }

    const { dashboardId, targetEmail, permission, note } = req.body || {};
    if (!dashboardId || !targetEmail || !permission) {
      return res.status(400).json({ error: "dashboardId, targetEmail, and permission are required." });
    }

    if (permission !== "view" && permission !== "edit") {
      return res.status(400).json({ error: "permission must be either 'view' or 'edit'." });
    }

    const invite = await createSharedDashboardInvite({
      ownerEmail,
      targetEmail: sanitizeEmail(targetEmail),
      sourceDashboardId: dashboardId,
      permission,
      note,
    });

    res.status(201).json({ invite });
  } catch (error) {
    console.error("shareDashboardController error:", error);
    const message = error instanceof Error ? error.message : "Failed to share dashboard.";
    const statusCode = (error as any)?.statusCode || 500;
    res.status(statusCode).json({ error: message });
  }
};

export const getIncomingSharedDashboardsController = async (req: Request, res: Response) => {
  try {
    const userEmail = getUserEmailFromRequest(req);
    if (!userEmail) {
      return res.status(401).json({ error: "Missing authenticated user email." });
    }

    const invitations = await listSharedDashboardsForUser(userEmail);
    const responsePayload = {
      pending: invitations.filter((invite) => invite.status === "pending"),
      accepted: invitations.filter((invite) => invite.status === "accepted"),
    };

    // Validate payload before sending (throws if invalid)
    sharedDashboardsResponseSchema.parse(responsePayload);
    res.json(responsePayload);
  } catch (error) {
    console.error("getIncomingSharedDashboardsController error:", error);
    const message = error instanceof Error ? error.message : "Failed to load shared dashboards.";
    const statusCode = (error as any)?.statusCode || 500;
    res.status(statusCode).json({ error: message });
  }
};

export const getSentSharedDashboardsController = async (req: Request, res: Response) => {
  try {
    const userEmail = getUserEmailFromRequest(req);
    if (!userEmail) {
      return res.status(401).json({ error: "Missing authenticated user email." });
    }

    const invitations = await listSharedDashboardsForOwner(userEmail);
    res.json({ invitations });
  } catch (error) {
    console.error("getSentSharedDashboardsController error:", error);
    const message = error instanceof Error ? error.message : "Failed to load sent shared dashboards.";
    const statusCode = (error as any)?.statusCode || 500;
    res.status(statusCode).json({ error: message });
  }
};

export const acceptSharedDashboardController = async (req: Request, res: Response) => {
  try {
    const userEmail = getUserEmailFromRequest(req);
    if (!userEmail) {
      return res.status(401).json({ error: "Missing authenticated user email." });
    }

    const { inviteId } = req.params;
    if (!inviteId) {
      return res.status(400).json({ error: "inviteId is required." });
    }

    const { invite, dashboard } = await acceptSharedDashboardInvite(inviteId, userEmail);

    res.json({
      invite,
      dashboard,
    });
  } catch (error) {
    console.error("acceptSharedDashboardController error:", error);
    const message = error instanceof Error ? error.message : "Failed to accept shared dashboard.";
    const statusCode = (error as any)?.statusCode || 500;
    res.status(statusCode).json({ error: message });
  }
};

export const declineSharedDashboardController = async (req: Request, res: Response) => {
  try {
    const userEmail = getUserEmailFromRequest(req);
    if (!userEmail) {
      return res.status(401).json({ error: "Missing authenticated user email." });
    }

    const { inviteId } = req.params;
    if (!inviteId) {
      return res.status(400).json({ error: "inviteId is required." });
    }

    const invite = await declineSharedDashboardInvite(inviteId, userEmail);
    res.json({ invite });
  } catch (error) {
    console.error("declineSharedDashboardController error:", error);
    const message = error instanceof Error ? error.message : "Failed to decline shared dashboard.";
    const statusCode = (error as any)?.statusCode || 500;
    res.status(statusCode).json({ error: message });
  }
};

export const getSharedDashboardInviteController = async (req: Request, res: Response) => {
  try {
    const userEmail = getUserEmailFromRequest(req);
    if (!userEmail) {
      return res.status(401).json({ error: "Missing authenticated user email." });
    }

    const { inviteId } = req.params;
    if (!inviteId) {
      return res.status(400).json({ error: "inviteId is required." });
    }

    const invite = await getSharedDashboardInviteById(inviteId, userEmail);
    if (!invite) {
      return res.status(404).json({ error: "Shared dashboard invite not found." });
    }

    res.json({ invite });
  } catch (error) {
    console.error("getSharedDashboardInviteController error:", error);
    const message = error instanceof Error ? error.message : "Failed to load shared dashboard invite.";
    const statusCode = (error as any)?.statusCode || 500;
    res.status(statusCode).json({ error: message });
  }
};

/**
 * Streaming shared dashboards endpoint using Server-Sent Events (SSE)
 * Provides real-time updates for incoming shared dashboard invites
 */
export const streamIncomingSharedDashboardsController = async (req: Request, res: Response) => {
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

    // Function to fetch and send shared dashboards
    const sendSharedDashboards = async () => {
      // Check if connection is still open before attempting to send
      if (res.writableEnded || res.destroyed || !res.writable) {
        return false;
      }

      try {
        const invitations = await listSharedDashboardsForUser(userEmail);
        const responsePayload = {
          pending: invitations.filter((invite) => invite.status === "pending"),
          accepted: invitations.filter((invite) => invite.status === "accepted"),
        };

        // Validate payload before sending
        sharedDashboardsResponseSchema.parse(responsePayload);
        const sent = sendSSE(res, 'update', responsePayload);
        
        if (!sent) {
          return false; // Connection closed
        }
        return true;
      } catch (error) {
        // Only try to send error if connection is still open
        if (!res.writableEnded && !res.destroyed && res.writable) {
          console.error('Error fetching shared dashboards for SSE:', error);
          sendSSE(res, 'error', { 
            message: error instanceof Error ? error.message : 'Failed to fetch shared dashboards.' 
          });
        }
        return false;
      }
    };

    // Send initial data immediately
    await sendSharedDashboards();

    // Set up polling to check for new invites every 3 seconds
    const checkInterval = setInterval(async () => {
      // Check if connection is still open
      if (res.writableEnded || res.destroyed || !res.writable) {
        clearInterval(checkInterval);
        return;
      }

      const stillConnected = await sendSharedDashboards();
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
    console.error("streamIncomingSharedDashboardsController error:", error);
    const message = error instanceof Error ? error.message : "Failed to stream shared dashboards.";
    sendSSE(res, 'error', { message });
    res.end();
  }
};
