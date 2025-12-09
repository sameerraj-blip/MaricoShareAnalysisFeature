import { useCallback, useMemo, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ChartSpec, Dashboard as ServerDashboard, DashboardSheet } from '@/shared/schema';
import { dashboardsApi } from '@/lib/api';

export interface DashboardData {
  id: string;
  name: string;
  charts: ChartSpec[];
  sheets?: DashboardSheet[];
  createdAt: Date;
  updatedAt: Date;
  lastOpenedAt?: Date;
  username?: string; // Owner's email/username
  isShared?: boolean; // Whether this is a shared dashboard (shared WITH the current user)
  sharedPermission?: "view" | "edit"; // Permission level for shared dashboards
  sharedBy?: string; // Email of the user who shared this dashboard
  permission?: "view" | "edit"; // Computed permission (for convenience)
  hasCollaborators?: boolean; // Whether this dashboard has been shared with others (owned by current user but shared)
  collaborators?: Array<{ userId: string; permission: "view" | "edit" }>; // List of collaborators
}

export const normalizeDashboard = (dashboard: ServerDashboard & { isShared?: boolean; sharedPermission?: "view" | "edit"; sharedBy?: string }): DashboardData => {
  const normalized: DashboardData = {
    id: dashboard.id,
    name: dashboard.name,
    charts: dashboard.charts || [],
    sheets: dashboard.sheets || [],
    createdAt: new Date(dashboard.createdAt),
    updatedAt: new Date(dashboard.updatedAt),
    lastOpenedAt: dashboard.lastOpenedAt ? new Date(dashboard.lastOpenedAt) : undefined,
    username: dashboard.username,
    // Preserve shared dashboard properties
    isShared: (dashboard as any).isShared || false,
    sharedPermission: (dashboard as any).sharedPermission,
    sharedBy: (dashboard as any).sharedBy,
    // Check if dashboard has collaborators (has been shared by owner)
    collaborators: (dashboard as any).collaborators || [],
    hasCollaborators: ((dashboard as any).collaborators && (dashboard as any).collaborators.length > 0) || false,
  };
  
  // Set permission for convenience (use sharedPermission if it's a shared dashboard)
  if (normalized.isShared && normalized.sharedPermission) {
    normalized.permission = normalized.sharedPermission;
  }
  
  return normalized;
};

export const useDashboardState = () => {
  const queryClient = useQueryClient();
  const [currentDashboard, setCurrentDashboard] = useState<DashboardData | null>(null);

  const {
    data: dashboards = [],
    isFetching,
    isLoading,
    refetch,
    error,
  } = useQuery({
    queryKey: ['dashboards', 'list'],
    queryFn: async () => {
      const res = await dashboardsApi.list();
      const normalized = res.dashboards.map(normalizeDashboard);
      return normalized;
    },
    staleTime: 0, // Always refetch to get latest shared dashboards
  });

  const createDashboardMutation = useMutation({
    mutationFn: async (name: string) => {
      const created = await dashboardsApi.create(name);
      return normalizeDashboard(created);
    },
    onSuccess: (createdDashboard) => {
      queryClient.setQueryData<DashboardData[]>(['dashboards', 'list'], (prev) => {
        const existing = prev ?? [];
        return [...existing, createdDashboard];
      });
      setCurrentDashboard(createdDashboard);
    },
  });

  const addChartMutation = useMutation({
    mutationFn: async ({ dashboardId, chart, sheetId }: { dashboardId: string; chart: ChartSpec; sheetId?: string }) => {
      const updated = await dashboardsApi.addChart(dashboardId, chart, sheetId);
      return normalizeDashboard(updated);
    },
    onSuccess: (updatedDashboard) => {
      queryClient.setQueryData<DashboardData[]>(['dashboards', 'list'], (prev) => {
        if (!prev) return [updatedDashboard];
        return prev.map((dashboard) => (dashboard.id === updatedDashboard.id ? updatedDashboard : dashboard));
      });
      setCurrentDashboard((prev) => (prev?.id === updatedDashboard.id ? updatedDashboard : prev));
    },
  });

  const removeChartMutation = useMutation({
    mutationFn: async ({ dashboardId, chartIndex, sheetId }: { dashboardId: string; chartIndex: number; sheetId?: string }) => {
      const updated = await dashboardsApi.removeChart(dashboardId, { index: chartIndex, sheetId });
      return normalizeDashboard(updated);
    },
    onSuccess: (updatedDashboard) => {
      queryClient.setQueryData<DashboardData[]>(['dashboards', 'list'], (prev) => {
        if (!prev) return [updatedDashboard];
        return prev.map((dashboard) => (dashboard.id === updatedDashboard.id ? updatedDashboard : dashboard));
      });
      setCurrentDashboard((prev) => (prev?.id === updatedDashboard.id ? updatedDashboard : prev));
    },
  });

  const deleteDashboardMutation = useMutation({
    mutationFn: async (dashboardId: string) => {
      await dashboardsApi.remove(dashboardId);
      return dashboardId;
    },
    onSuccess: (dashboardId) => {
      queryClient.setQueryData<DashboardData[]>(['dashboards', 'list'], (prev) =>
        (prev ?? []).filter((dashboard) => dashboard.id !== dashboardId)
      );
      setCurrentDashboard((prev) => (prev?.id === dashboardId ? null : prev));
    },
  });

  const getDashboardById = useCallback(
    (dashboardId: string): DashboardData | undefined => dashboards.find((dashboard) => dashboard.id === dashboardId),
    [dashboards]
  );

  const fetchDashboardById = useCallback(
    async (dashboardId: string): Promise<DashboardData> => {
      const dashboard = await dashboardsApi.get(dashboardId);
      const normalized = normalizeDashboard(dashboard);
      // Update the cache
      queryClient.setQueryData<DashboardData[]>(['dashboards', 'list'], (prev) => {
        if (!prev) return [normalized];
        return prev.map((d) => (d.id === normalized.id ? normalized : d));
      });
      return normalized;
    },
    [queryClient]
  );

  const createDashboard = useCallback((name: string) => createDashboardMutation.mutateAsync(name), [
    createDashboardMutation,
  ]);

  const addChartToDashboard = useCallback(
    (dashboardId: string, chart: ChartSpec, sheetId?: string) => addChartMutation.mutateAsync({ dashboardId, chart, sheetId }),
    [addChartMutation]
  );

  const removeChartFromDashboard = useCallback(
    (dashboardId: string, chartIndex: number, sheetId?: string) =>
      removeChartMutation.mutateAsync({ dashboardId, chartIndex, sheetId }),
    [removeChartMutation]
  );

  const deleteDashboard = useCallback(
    async (dashboardId: string) => {
      await deleteDashboardMutation.mutateAsync(dashboardId);
    },
    [deleteDashboardMutation]
  );

  const renameDashboardMutation = useMutation({
    mutationFn: async ({ dashboardId, name }: { dashboardId: string; name: string }) => {
      const updated = await dashboardsApi.rename(dashboardId, name);
      return normalizeDashboard(updated);
    },
    onSuccess: (updatedDashboard) => {
      queryClient.setQueryData<DashboardData[]>(['dashboards', 'list'], (prev) => {
        if (!prev) return [updatedDashboard];
        return prev.map((dashboard) => (dashboard.id === updatedDashboard.id ? updatedDashboard : dashboard));
      });
      setCurrentDashboard((prev) => (prev?.id === updatedDashboard.id ? updatedDashboard : prev));
    },
    onError: (error: any) => {
      // Error will be handled by the component showing toast
      throw error;
    },
  });

  const renameSheetMutation = useMutation({
    mutationFn: async ({ dashboardId, sheetId, name }: { dashboardId: string; sheetId: string; name: string }) => {
      const updated = await dashboardsApi.renameSheet(dashboardId, sheetId, name);
      return normalizeDashboard(updated);
    },
    onSuccess: (updatedDashboard) => {
      queryClient.setQueryData<DashboardData[]>(['dashboards', 'list'], (prev) => {
        if (!prev) return [updatedDashboard];
        return prev.map((dashboard) => (dashboard.id === updatedDashboard.id ? updatedDashboard : dashboard));
      });
      setCurrentDashboard((prev) => (prev?.id === updatedDashboard.id ? updatedDashboard : prev));
    },
    onError: (error: any) => {
      // Error will be handled by the component showing toast
      throw error;
    },
  });

  const renameDashboard = useCallback(
    (dashboardId: string, name: string) => renameDashboardMutation.mutateAsync({ dashboardId, name }),
    [renameDashboardMutation]
  );

  const renameSheet = useCallback(
    (dashboardId: string, sheetId: string, name: string) => renameSheetMutation.mutateAsync({ dashboardId, sheetId, name }),
    [renameSheetMutation]
  );

  const removeSheetMutation = useMutation({
    mutationFn: async ({ dashboardId, sheetId }: { dashboardId: string; sheetId: string }) => {
      const updated = await dashboardsApi.removeSheet(dashboardId, sheetId);
      return normalizeDashboard(updated);
    },
    onSuccess: (updatedDashboard) => {
      queryClient.setQueryData<DashboardData[]>(['dashboards', 'list'], (prev) => {
        if (!prev) return [updatedDashboard];
        return prev.map((dashboard) => (dashboard.id === updatedDashboard.id ? updatedDashboard : dashboard));
      });
      setCurrentDashboard((prev) => (prev?.id === updatedDashboard.id ? updatedDashboard : prev));
    },
    onError: (error: any) => {
      // Error will be handled by the component showing toast
      throw error;
    },
  });

  const removeSheet = useCallback(
    (dashboardId: string, sheetId: string) => removeSheetMutation.mutateAsync({ dashboardId, sheetId }),
    [removeSheetMutation]
  );

  const addSheetMutation = useMutation({
    mutationFn: async ({ dashboardId, name }: { dashboardId: string; name: string }) => {
      const updated = await dashboardsApi.addSheet(dashboardId, name);
      return normalizeDashboard(updated);
    },
    onSuccess: (updatedDashboard) => {
      queryClient.setQueryData<DashboardData[]>(['dashboards', 'list'], (prev) => {
        if (!prev) return [updatedDashboard];
        return prev.map((dashboard) => (dashboard.id === updatedDashboard.id ? updatedDashboard : dashboard));
      });
      setCurrentDashboard((prev) => (prev?.id === updatedDashboard.id ? updatedDashboard : prev));
    },
    onError: (error: any) => {
      // Error will be handled by the component showing toast
      throw error;
    },
  });

  const addSheet = useCallback(
    (dashboardId: string, name: string) => addSheetMutation.mutateAsync({ dashboardId, name }),
    [addSheetMutation]
  );

  const updateChartInsightOrRecommendationMutation = useMutation({
    mutationFn: async ({ dashboardId, chartIndex, updates, sheetId }: { dashboardId: string; chartIndex: number; updates: { keyInsight?: string }; sheetId?: string }) => {
      const updated = await dashboardsApi.updateChartInsightOrRecommendation(dashboardId, chartIndex, updates, sheetId);
      return normalizeDashboard(updated);
    },
    onSuccess: (updatedDashboard) => {
      queryClient.setQueryData<DashboardData[]>(['dashboards', 'list'], (prev) => {
        if (!prev) return [updatedDashboard];
        return prev.map((dashboard) => (dashboard.id === updatedDashboard.id ? updatedDashboard : dashboard));
      });
      setCurrentDashboard((prev) => (prev?.id === updatedDashboard.id ? updatedDashboard : prev));
    },
  });

  const updateChartInsightOrRecommendation = useCallback(
    (dashboardId: string, chartIndex: number, updates: { keyInsight?: string }, sheetId?: string) =>
      updateChartInsightOrRecommendationMutation.mutateAsync({ dashboardId, chartIndex, updates, sheetId }),
    [updateChartInsightOrRecommendationMutation]
  );

  const status = useMemo(
    () => ({
      isLoading,
      isFetching,
      error,
      refreshing: isFetching && !isLoading,
    }),
    [error, isFetching, isLoading]
  );

  return {
    dashboards,
    currentDashboard,
    setCurrentDashboard,
    createDashboard,
    addChartToDashboard,
    removeChartFromDashboard,
    deleteDashboard,
    renameDashboard,
    renameSheet,
    addSheet,
    removeSheet,
    updateChartInsightOrRecommendation,
    getDashboardById,
    fetchDashboardById,
    status,
    refetch,
  };
};
