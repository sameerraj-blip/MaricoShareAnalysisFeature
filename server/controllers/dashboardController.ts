import { Request, Response } from "express";
import {
  addChartToDashboardRequestSchema,
  createDashboardRequestSchema,
  removeChartFromDashboardRequestSchema,
} from "../shared/schema.js";
import {
  addChartToDashboard,
  addSheetToDashboard,
  createDashboard,
  deleteDashboard,
  getDashboardById,
  getUserDashboards,
  removeChartFromDashboard,
  removeSheetFromDashboard,
  renameSheet,
  renameDashboard,
  updateChartInsightOrRecommendation,
} from "../models/dashboard.model.js";

export const createDashboardController = async (req: Request, res: Response) => {
  try {
    const username = (req.body.username || req.headers['x-user-email'] || 'anonymous@example.com') as string;
    const parsed = createDashboardRequestSchema.parse(req.body);
    const dashboard = await createDashboard(username, parsed.name, parsed.charts || []);
    res.status(201).json(dashboard);
  } catch (error: any) {
    // Check if it's a duplicate name error
    if (error?.message?.includes('already exists')) {
      res.status(409).json({ error: error.message });
    } else {
      res.status(400).json({ error: error?.message || 'Failed to create dashboard' });
    }
  }
};

export const listDashboardsController = async (req: Request, res: Response) => {
  try {
    const username = (req.query.username || req.headers['x-user-email'] || 'anonymous@example.com') as string;
    const dashboards = await getUserDashboards(username);
    
    // Also get shared dashboards that the user has accepted
    const { listSharedDashboardsForUser } = await import("../models/sharedDashboard.model.js");
    const { waitForDashboardsContainer } = await import("../models/database.config.js");
    const normalizedUsername = username.toLowerCase();
    const sharedInvites = await listSharedDashboardsForUser(normalizedUsername);
    
    // Get accepted shared dashboards
    const acceptedInvites = sharedInvites.filter(invite => invite.status === "accepted");
    const sharedDashboards = await Promise.all(
      acceptedInvites.map(async (invite) => {
        try {
          // Get dashboard using owner's username (partition key)
          const dashboardsContainer = await waitForDashboardsContainer();
          const { resource } = await dashboardsContainer.item(invite.sourceDashboardId, invite.ownerEmail).read();
          const dashboard = resource as unknown as typeof dashboards[0];
          
          if (dashboard) {
            // Add permission and shared flag to the dashboard
            return {
              ...dashboard,
              isShared: true,
              sharedPermission: invite.permission,
              sharedBy: invite.ownerEmail,
            };
          }
          return null;
        } catch (error) {
          console.error(`Failed to fetch shared dashboard ${invite.sourceDashboardId}:`, error);
          return null;
        }
      })
    );
    
    // Filter out null values and merge with owned dashboards
    const validSharedDashboards = sharedDashboards.filter((d): d is NonNullable<typeof d> => d !== null);
    const allDashboards = [...dashboards, ...validSharedDashboards];
    
    res.json({ dashboards: allDashboards });
  } catch (error: any) {
    res.status(500).json({ error: error?.message || 'Failed to fetch dashboards' });
  }
};

export const getDashboardController = async (req: Request, res: Response) => {
  try {
    const username = (req.query.username || req.headers['x-user-email'] || 'anonymous@example.com') as string;
    const { dashboardId } = req.params as { dashboardId: string };
    const dashboard = await getDashboardById(dashboardId, username);
    if (!dashboard) {
      return res.status(404).json({ error: 'Dashboard not found' });
    }
    res.json(dashboard);
  } catch (error: any) {
    res.status(500).json({ error: error?.message || 'Failed to fetch dashboard' });
  }
};

export const deleteDashboardController = async (req: Request, res: Response) => {
  try {
    const username = (req.body.username || req.headers['x-user-email'] || 'anonymous@example.com') as string;
    const { dashboardId } = req.params as { dashboardId: string };
    const existing = await getDashboardById(dashboardId, username);
    if (!existing) return res.status(404).json({ error: 'Dashboard not found' });
    await deleteDashboard(dashboardId, username);
    res.json({ success: true });
  } catch (error: any) {
    res.status(500).json({ error: error?.message || 'Failed to delete dashboard' });
  }
};

export const addChartToDashboardController = async (req: Request, res: Response) => {
  try {
    const username = (req.body.username || req.headers['x-user-email'] || 'anonymous@example.com') as string;
    const { dashboardId } = req.params as { dashboardId: string };
    const parsed = addChartToDashboardRequestSchema.parse(req.body);
    
    console.log(`[addChartToDashboard] Attempting to add chart to dashboard ${dashboardId} for user ${username}`);
    
    const updated = await addChartToDashboard(dashboardId, username, parsed.chart, parsed.sheetId);
    res.json(updated);
  } catch (error: any) {
    console.error(`[addChartToDashboard] Error:`, error);
    res.status(400).json({ error: error?.message || 'Failed to add chart' });
  }
};

export const addSheetToDashboardController = async (req: Request, res: Response) => {
  try {
    const username = (req.body.username || req.headers['x-user-email'] || 'anonymous@example.com') as string;
    const { dashboardId } = req.params as { dashboardId: string };
    const { name } = req.body;
    if (!name || typeof name !== 'string' || name.trim().length === 0) {
      return res.status(400).json({ error: 'Sheet name is required' });
    }
    const updated = await addSheetToDashboard(dashboardId, username, name.trim());
    res.json(updated);
  } catch (error: any) {
    // Check if it's a duplicate name error
    if (error?.message?.includes('already exists')) {
      res.status(409).json({ error: error.message });
    } else {
      res.status(400).json({ error: error?.message || 'Failed to add sheet' });
    }
  }
};

export const removeSheetFromDashboardController = async (req: Request, res: Response) => {
  try {
    const username = (req.body.username || req.headers['x-user-email'] || 'anonymous@example.com') as string;
    const { dashboardId, sheetId } = req.params as { dashboardId: string; sheetId: string };
    const updated = await removeSheetFromDashboard(dashboardId, username, sheetId);
    res.json(updated);
  } catch (error: any) {
    res.status(400).json({ error: error?.message || 'Failed to remove sheet' });
  }
};

export const renameSheetController = async (req: Request, res: Response) => {
  try {
    const username = (req.body.username || req.headers['x-user-email'] || 'anonymous@example.com') as string;
    const { dashboardId, sheetId } = req.params as { dashboardId: string; sheetId: string };
    const { name } = req.body;
    if (!name || typeof name !== 'string' || name.trim().length === 0) {
      return res.status(400).json({ error: 'Sheet name is required' });
    }
    const updated = await renameSheet(dashboardId, username, sheetId, name.trim());
    res.json(updated);
  } catch (error: any) {
    // Check if it's a duplicate name error
    if (error?.message?.includes('already exists')) {
      res.status(409).json({ error: error.message });
    } else {
      res.status(400).json({ error: error?.message || 'Failed to rename sheet' });
    }
  }
};

export const renameDashboardController = async (req: Request, res: Response) => {
  try {
    const username = (req.body.username || req.headers['x-user-email'] || 'anonymous@example.com') as string;
    const { dashboardId } = req.params as { dashboardId: string };
    const { name } = req.body;
    if (!name || typeof name !== 'string' || name.trim().length === 0) {
      return res.status(400).json({ error: 'Dashboard name is required' });
    }
    const updated = await renameDashboard(dashboardId, username, name.trim());
    res.json(updated);
  } catch (error: any) {
    // Check if it's a duplicate name error
    if (error?.message?.includes('already exists')) {
      res.status(409).json({ error: error.message });
    } else {
      res.status(400).json({ error: error?.message || 'Failed to rename dashboard' });
    }
  }
};

export const removeChartFromDashboardController = async (req: Request, res: Response) => {
  try {
    const username = (req.body.username || req.headers['x-user-email'] || 'anonymous@example.com') as string;
    const { dashboardId } = req.params as { dashboardId: string };
    const parsed = removeChartFromDashboardRequestSchema.parse(req.body);
    const updated = await removeChartFromDashboard(dashboardId, username, parsed);
    res.json(updated);
  } catch (error: any) {
    res.status(400).json({ error: error?.message || 'Failed to remove chart' });
  }
};

export const updateChartInsightOrRecommendationController = async (req: Request, res: Response) => {
  try {
    const username = (req.body.username || req.headers['x-user-email'] || 'anonymous@example.com') as string;
    const { dashboardId, chartIndex: chartIndexParam } = req.params as { dashboardId: string; chartIndex: string };
    const { sheetId, keyInsight } = req.body;
    const chartIndex = parseInt(chartIndexParam, 10);

    if (isNaN(chartIndex) || chartIndex < 0) {
      return res.status(400).json({ error: 'Valid chartIndex is required' });
    }

    if (keyInsight === undefined) {
      return res.status(400).json({ error: 'keyInsight must be provided' });
    }

    const updates: { keyInsight?: string } = {};
    if (keyInsight !== undefined) {
      updates.keyInsight = typeof keyInsight === 'string' ? keyInsight : undefined;
    }

    const updated = await updateChartInsightOrRecommendation(
      dashboardId,
      username,
      chartIndex,
      sheetId,
      updates
    );
    res.json(updated);
  } catch (error: any) {
    res.status(400).json({ error: error?.message || 'Failed to update chart insight or recommendation' });
  }
};



