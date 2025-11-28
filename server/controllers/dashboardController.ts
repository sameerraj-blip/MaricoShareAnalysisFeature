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
    res.json({ dashboards });
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
    const updated = await addChartToDashboard(dashboardId, username, parsed.chart, parsed.sheetId);
    res.json(updated);
  } catch (error: any) {
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



