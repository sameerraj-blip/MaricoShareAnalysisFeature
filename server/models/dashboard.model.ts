/**
 * Dashboard Model
 * Handles all database operations for dashboards
 */
import { ChartSpec, Dashboard } from "../shared/schema.js";
import { waitForDashboardsContainer } from "./database.config.js";

/**
 * Create a new dashboard
 */
export const createDashboard = async (
  username: string,
  name: string,
  charts: ChartSpec[] = []
): Promise<Dashboard> => {
  const dashboardsContainer = await waitForDashboardsContainer();
  
  // Check if a dashboard with the same name already exists for this username
  const existingDashboards = await getUserDashboards(username);
  const duplicateDashboard = existingDashboards.find(
    d => d.name.toLowerCase().trim() === name.toLowerCase().trim()
  );
  
  if (duplicateDashboard) {
    throw new Error(`A dashboard with the name "${name}" already exists. Please enter a different name.`);
  }
  
  const timestamp = Date.now();
  const id = `${name.replace(/[^a-zA-Z0-9]/g, '_')}_${timestamp}`;
  
  // Create default sheet with charts
  const defaultSheet = {
    id: 'default',
    name: 'Overview',
    charts,
    order: 0,
  };
  
  const dashboard: Dashboard = {
    id,
    username,
    name,
    createdAt: timestamp,
    updatedAt: timestamp,
    charts, // Keep for backward compatibility
    sheets: [defaultSheet],
  };
  const { resource } = await dashboardsContainer.items.create(dashboard);
  return resource as unknown as Dashboard;
};

/**
 * Get all dashboards for a user
 */
export const getUserDashboards = async (username: string): Promise<Dashboard[]> => {
  try {
    const dashboardsContainer = await waitForDashboardsContainer();
    const { resources } = await dashboardsContainer.items.query({
      query: "SELECT * FROM c WHERE c.username = @username ORDER BY c.createdAt DESC",
      parameters: [{ name: "@username", value: username }],
    }).fetchAll();
    return resources as unknown as Dashboard[];
  } catch (error) {
    console.error("Failed to get user dashboards:", error);
    return [];
  }
};

/**
 * Get dashboard by ID
 */
export const getDashboardById = async (id: string, username: string): Promise<Dashboard | null> => {
  try {
    const dashboardsContainer = await waitForDashboardsContainer();
    const { resource } = await dashboardsContainer.item(id, username).read();
    const dashboard = resource as unknown as Dashboard;
    
    // Update lastOpenedAt when dashboard is accessed
    if (dashboard) {
      dashboard.lastOpenedAt = Date.now();
      return await updateDashboard(dashboard);
    }
    
    return dashboard;
  } catch (error: any) {
    if (error.code === 404) return null;
    throw error;
  }
};

/**
 * Rename a dashboard
 */
export const renameDashboard = async (
  id: string,
  username: string,
  newName: string
): Promise<Dashboard> => {
  const dashboard = await getDashboardById(id, username);
  if (!dashboard) throw new Error("Dashboard not found");
  
  // Check if a dashboard with the same name already exists for this username (excluding current dashboard)
  const existingDashboards = await getUserDashboards(username);
  const duplicateDashboard = existingDashboards.find(
    d => d.id !== id && d.name.toLowerCase().trim() === newName.toLowerCase().trim()
  );
  
  if (duplicateDashboard) {
    throw new Error(`A dashboard with the name "${newName}" already exists. Please enter a different name.`);
  }
  
  dashboard.name = newName;
  dashboard.updatedAt = Date.now();
  return updateDashboard(dashboard);
};

/**
 * Update dashboard
 */
export const updateDashboard = async (dashboard: Dashboard): Promise<Dashboard> => {
  const dashboardsContainer = await waitForDashboardsContainer();
  dashboard.updatedAt = Date.now();
  const { resource } = await dashboardsContainer.items.upsert(dashboard);
  return resource as unknown as Dashboard;
};

/**
 * Delete dashboard
 */
export const deleteDashboard = async (id: string, username: string): Promise<void> => {
  const dashboardsContainer = await waitForDashboardsContainer();
  await dashboardsContainer.item(id, username).delete();
};

/**
 * Add chart to dashboard
 */
export const addChartToDashboard = async (
  id: string,
  username: string,
  chart: ChartSpec,
  sheetId?: string
): Promise<Dashboard> => {
  const dashboard = await getDashboardById(id, username);
  if (!dashboard) throw new Error("Dashboard not found");
  
  // Initialize sheets if not present (backward compatibility)
  if (!dashboard.sheets || dashboard.sheets.length === 0) {
    dashboard.sheets = [{
      id: 'default',
      name: 'Overview',
      charts: [...dashboard.charts],
      order: 0,
    }];
  }
  
  // If sheetId is provided, add to that sheet; otherwise add to first sheet
  const targetSheetId = sheetId || dashboard.sheets[0].id;
  const targetSheet = dashboard.sheets.find(s => s.id === targetSheetId);
  
  if (!targetSheet) {
    throw new Error(`Sheet with id ${targetSheetId} not found`);
  }
  
  targetSheet.charts.push(chart);
  
  // Also update the legacy charts array for backward compatibility
  dashboard.charts.push(chart);
  
  return updateDashboard(dashboard);
};

/**
 * Add sheet to dashboard
 */
export const addSheetToDashboard = async (
  id: string,
  username: string,
  sheetName: string
): Promise<Dashboard> => {
  const dashboard = await getDashboardById(id, username);
  if (!dashboard) throw new Error("Dashboard not found");
  
  // Initialize sheets if not present
  if (!dashboard.sheets || dashboard.sheets.length === 0) {
    dashboard.sheets = [{
      id: 'default',
      name: 'Overview',
      charts: [...dashboard.charts],
      order: 0,
    }];
  }
  
  const trimmedName = sheetName.trim();
  
  // Check for duplicate sheet names (case-insensitive)
  const duplicateSheet = dashboard.sheets.find(s => 
    s.name.toLowerCase().trim() === trimmedName.toLowerCase()
  );
  
  if (duplicateSheet) {
    throw new Error(`A sheet with the name "${trimmedName}" already exists. Please enter a different name.`);
  }
  
  const newSheet = {
    id: `sheet-${Date.now()}`,
    name: trimmedName,
    charts: [],
    order: dashboard.sheets.length,
  };
  
  dashboard.sheets.push(newSheet);
  return updateDashboard(dashboard);
};

/**
 * Remove sheet from dashboard
 */
export const removeSheetFromDashboard = async (
  id: string,
  username: string,
  sheetId: string
): Promise<Dashboard> => {
  const dashboard = await getDashboardById(id, username);
  if (!dashboard) throw new Error("Dashboard not found");
  
  if (!dashboard.sheets || dashboard.sheets.length <= 1) {
    throw new Error("Cannot remove the last sheet");
  }
  
  dashboard.sheets = dashboard.sheets.filter(s => s.id !== sheetId);
  return updateDashboard(dashboard);
};

/**
 * Rename sheet in dashboard
 */
export const renameSheet = async (
  id: string,
  username: string,
  sheetId: string,
  newName: string
): Promise<Dashboard> => {
  const dashboard = await getDashboardById(id, username);
  if (!dashboard) throw new Error("Dashboard not found");
  
  if (!dashboard.sheets) {
    throw new Error("No sheets found");
  }
  
  const sheet = dashboard.sheets.find(s => s.id === sheetId);
  if (!sheet) throw new Error("Sheet not found");
  
  const trimmedName = newName.trim();
  
  // Check for duplicate sheet names (case-insensitive, excluding current sheet)
  const duplicateSheet = dashboard.sheets.find(s => 
    s.id !== sheetId && s.name.toLowerCase().trim() === trimmedName.toLowerCase()
  );
  
  if (duplicateSheet) {
    throw new Error(`A sheet with the name "${trimmedName}" already exists. Please enter a different name.`);
  }
  
  sheet.name = trimmedName;
  return updateDashboard(dashboard);
};

/**
 * Remove chart from dashboard
 */
export const removeChartFromDashboard = async (
  id: string,
  username: string,
  predicate: { index?: number; title?: string; type?: ChartSpec["type"]; sheetId?: string }
): Promise<Dashboard> => {
  const dashboard = await getDashboardById(id, username);
  if (!dashboard) throw new Error("Dashboard not found");

  // If sheetId is provided, remove from that specific sheet
  if (predicate.sheetId && dashboard.sheets && dashboard.sheets.length > 0) {
    const sheet = dashboard.sheets.find(s => s.id === predicate.sheetId);
    if (!sheet) {
      // If sheet not found, check if it's a default sheet (backward compatibility)
      if (predicate.sheetId === 'default' && dashboard.charts.length > 0) {
        // For default sheet, remove from main charts array
        if (typeof predicate.index === 'number' && predicate.index >= 0 && predicate.index < dashboard.charts.length) {
          dashboard.charts.splice(predicate.index, 1);
        }
        return updateDashboard(dashboard);
      }
      throw new Error(`Sheet with id "${predicate.sheetId}" not found`);
    }

    if (typeof predicate.index === 'number') {
      if (predicate.index >= 0 && predicate.index < sheet.charts.length) {
        // Get the chart BEFORE removing it
        const chartToRemove = sheet.charts[predicate.index];
        
        // Remove from the specific sheet
        sheet.charts.splice(predicate.index, 1);
        
        // Check if this chart exists in other sheets
        const existsInOtherSheets = dashboard.sheets.some(s => 
          s.id !== sheet.id && s.charts.some(c => 
            c.title === chartToRemove.title && c.type === chartToRemove.type
          )
        );
        
        // Only remove from main charts array if it doesn't exist in other sheets
        if (!existsInOtherSheets) {
          const mainIndex = dashboard.charts.findIndex(c => 
            c.title === chartToRemove.title && c.type === chartToRemove.type
          );
          if (mainIndex >= 0) {
            dashboard.charts.splice(mainIndex, 1);
          }
        }
      }
    } else if (predicate.title || predicate.type) {
      // Filter sheet charts
      const removedCharts = sheet.charts.filter(c => {
        const titleMatch = predicate.title ? c.title === predicate.title : false;
        const typeMatch = predicate.type ? c.type === predicate.type : false;
        return titleMatch || typeMatch;
      });
      
      sheet.charts = sheet.charts.filter(c => {
        const titleMatch = predicate.title ? c.title !== predicate.title : true;
        const typeMatch = predicate.type ? c.type !== predicate.type : true;
        return titleMatch && typeMatch;
      });
      
      // Remove from main charts array only if not in other sheets
      removedCharts.forEach(removedChart => {
        const existsInOtherSheets = dashboard.sheets && dashboard.sheets.some(s => 
          s.id !== sheet.id && s.charts.some(c => 
            c.title === removedChart.title && c.type === removedChart.type
          )
        );
        
        if (!existsInOtherSheets) {
          const mainIndex = dashboard.charts.findIndex(c => 
            c.title === removedChart.title && c.type === removedChart.type
          );
          if (mainIndex >= 0) {
            dashboard.charts.splice(mainIndex, 1);
          }
        }
      });
    }
  } else {
    // Legacy behavior: remove from main charts array
    if (typeof predicate.index === 'number') {
      dashboard.charts.splice(predicate.index, 1);
      // Also remove from all sheets
      if (dashboard.sheets) {
        dashboard.sheets.forEach(sheet => {
          if (predicate.index! < sheet.charts.length) {
            sheet.charts.splice(predicate.index!, 1);
          }
        });
      }
    } else if (predicate.title || predicate.type) {
      dashboard.charts = dashboard.charts.filter(c => {
        const titleMatch = predicate.title ? c.title !== predicate.title : true;
        const typeMatch = predicate.type ? c.type !== predicate.type : true;
        return titleMatch && typeMatch;
      });
      // Also remove from all sheets
      if (dashboard.sheets) {
        dashboard.sheets.forEach(sheet => {
          sheet.charts = sheet.charts.filter(c => {
            const titleMatch = predicate.title ? c.title !== predicate.title : true;
            const typeMatch = predicate.type ? c.type !== predicate.type : true;
            return titleMatch && typeMatch;
          });
        });
      }
    }
  }

  return updateDashboard(dashboard);
};

/**
 * Update chart insight or recommendation
 */
export const updateChartInsightOrRecommendation = async (
  id: string,
  username: string,
  chartIndex: number,
  sheetId: string | undefined,
  updates: { keyInsight?: string }
): Promise<Dashboard> => {
  const dashboard = await getDashboardById(id, username);
  if (!dashboard) throw new Error("Dashboard not found");

  // Initialize sheets if not present (backward compatibility)
  if (!dashboard.sheets || dashboard.sheets.length === 0) {
    dashboard.sheets = [{
      id: 'default',
      name: 'Overview',
      charts: [...dashboard.charts],
      order: 0,
    }];
  }

  // Find the target sheet
  const targetSheetId = sheetId || dashboard.sheets[0].id;
  const targetSheet = dashboard.sheets.find(s => s.id === targetSheetId);

  if (!targetSheet) {
    throw new Error(`Sheet with id ${targetSheetId} not found`);
  }

  if (chartIndex < 0 || chartIndex >= targetSheet.charts.length) {
    throw new Error(`Chart index ${chartIndex} is out of range`);
  }

  const chart = targetSheet.charts[chartIndex];

  // Update the chart's keyInsight
  if (updates.keyInsight !== undefined) {
    // If empty string, set to undefined to remove it
    chart.keyInsight = updates.keyInsight === '' ? undefined : updates.keyInsight;
  }

  // Also update in the legacy charts array for backward compatibility
  // Find the matching chart in the main charts array
  const mainChartIndex = dashboard.charts.findIndex(c => 
    c.title === chart.title && c.type === chart.type
  );
  if (mainChartIndex >= 0) {
    if (updates.keyInsight !== undefined) {
      // If empty string, set to undefined to remove it
      dashboard.charts[mainChartIndex].keyInsight = updates.keyInsight === '' ? undefined : updates.keyInsight;
    }
  }

  return updateDashboard(dashboard);
};

