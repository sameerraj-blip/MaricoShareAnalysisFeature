import React, { useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useDashboardContext } from './context/DashboardContext';
import { DashboardData } from './modules/useDashboardState';
import { DashboardList } from './Components/DashboardList';
import { DashboardView } from './Components/DashboardView';
import { DeleteDashboardDialog } from './Components/DeleteDashboardDialog';
import { SharedDashboardsPanel } from './Components/SharedDashboardsPanel';
import { Dashboard as ServerDashboard } from '@/shared/schema';
import { normalizeDashboard } from './modules/useDashboardState';

export default function Dashboard() {
  const queryClient = useQueryClient();
  const { 
    dashboards, 
    currentDashboard, 
    setCurrentDashboard, 
    deleteDashboard,
    removeChartFromDashboard,
    fetchDashboardById,
    status,
    refetch,
  } = useDashboardContext();

  const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false);
  const [dashboardToDelete, setDashboardToDelete] = useState<string | null>(null);

  const handleViewDashboard = async (dashboard: DashboardData) => {
    // Fetch fresh dashboard data to get updated lastOpenedAt
    try {
      const freshDashboard = await fetchDashboardById(dashboard.id);
      // Preserve permission and shared status if it's a shared dashboard
      const dashboardWithPermission = {
        ...freshDashboard,
        isShared: dashboard.isShared,
        sharedPermission: dashboard.sharedPermission,
        sharedBy: dashboard.sharedBy,
        permission: dashboard.permission || dashboard.sharedPermission,
        collaborators: freshDashboard.collaborators || dashboard.collaborators,
        hasCollaborators: freshDashboard.hasCollaborators || dashboard.hasCollaborators,
      };
      setCurrentDashboard(dashboardWithPermission);
    } catch (error) {
      // Fallback to cached dashboard if fetch fails
      console.error('Failed to fetch dashboard:', error);
      setCurrentDashboard(dashboard);
    }
  };

  const handleBackToList = () => {
    setCurrentDashboard(null);
  };

  const handleDeleteClick = (dashboardId: string) => {
    setDashboardToDelete(dashboardId);
    setDeleteConfirmOpen(true);
  };

  const handleConfirmDelete = async () => {
    if (dashboardToDelete) {
      await deleteDashboard(dashboardToDelete);
      setDeleteConfirmOpen(false);
      setDashboardToDelete(null);
    }
  };

  const handleDeleteChart = async (chartIndex: number, sheetId?: string) => {
    console.log('Delete chart clicked:', { chartIndex, sheetId, currentDashboard: currentDashboard?.id });
    if (currentDashboard) {
      console.log('Proceeding with chart deletion');
      const updatedDashboard = await removeChartFromDashboard(currentDashboard.id, chartIndex, sheetId);
      setCurrentDashboard(updatedDashboard);
      await refetch();
    }
  };

  const handleSharedDashboardAccepted = async (data: { invite: any; dashboard: ServerDashboard }) => {
    try {
      // Normalize the dashboard data first
      const normalizedDashboard = normalizeDashboard({
        ...data.dashboard,
        isShared: true,
        sharedPermission: data.invite.permission,
        sharedBy: data.invite.ownerEmail,
      });
      // Set the permission based on the invite
      const dashboardWithPermission = {
        ...normalizedDashboard,
        permission: data.invite.permission as "view" | "edit",
      };
      
      // Immediately add to the cache so it appears in the list right away
      queryClient.setQueryData<DashboardData[]>(['dashboards', 'list'], (prev) => {
        const existing = prev ?? [];
        // Check if dashboard already exists
        const exists = existing.some(d => d.id === dashboardWithPermission.id);
        if (exists) {
          // Update existing dashboard
          return existing.map(d => d.id === dashboardWithPermission.id ? dashboardWithPermission : d);
        }
        // Add new shared dashboard
        return [...existing, dashboardWithPermission];
      });
      
      // Also invalidate and refetch to ensure we have the latest data from backend
      setTimeout(async () => {
        await queryClient.invalidateQueries({ queryKey: ['dashboards', 'list'] });
        await refetch();
      }, 500);
      
      setCurrentDashboard(dashboardWithPermission);
    } catch (error) {
      console.error('Failed to load shared dashboard:', error);
      // Still try to refetch even if there's an error
      await queryClient.invalidateQueries({ queryKey: ['dashboards', 'list'] });
      await refetch();
    }
  };

  const handleViewSharedDashboard = async (dashboardId: string, permission: "view" | "edit") => {
    try {
      const dashboard = await fetchDashboardById(dashboardId);
      const dashboardWithPermission = {
        ...dashboard,
        permission,
        isShared: true,
        sharedPermission: permission,
        // Preserve collaborators if they exist
        collaborators: dashboard.collaborators,
        hasCollaborators: dashboard.hasCollaborators,
      };
      setCurrentDashboard(dashboardWithPermission);
    } catch (error) {
      console.error('Failed to load shared dashboard:', error);
    }
  };

  if (currentDashboard) {
    return (
      <DashboardView
        dashboard={currentDashboard}
        onBack={handleBackToList}
        onDeleteChart={handleDeleteChart}
        isRefreshing={status.refreshing}
        onRefresh={refetch}
      />
    );
  }

  const dashboardToDeleteName = dashboardToDelete
    ? dashboards.find((d) => d.id === dashboardToDelete)?.name
    : null;

  const handleDeleteCancel = () => {
    setDeleteConfirmOpen(false);
    setDashboardToDelete(null);
  };

  return (
    <>
      <div className="h-[calc(100vh-72px)] flex gap-6 p-6">
        {/* Left Column: Shared Dashboards Panel */}
        <div className="w-96 flex-shrink-0 flex flex-col min-h-0">
          <SharedDashboardsPanel 
            onAccepted={handleSharedDashboardAccepted}
            onViewDashboard={handleViewSharedDashboard}
          />
        </div>

        {/* Right Column: Dashboard List */}
        <div className="flex-1 flex flex-col min-w-0">
          <DashboardList
            dashboards={dashboards}
            isLoading={status.isLoading}
            isRefreshing={status.refreshing}
            onViewDashboard={handleViewDashboard}
            onDeleteDashboard={handleDeleteClick}
          />
        </div>
      </div>

      <DeleteDashboardDialog
        open={deleteConfirmOpen}
        onOpenChange={setDeleteConfirmOpen}
        dashboardName={dashboardToDeleteName}
        onConfirm={handleConfirmDelete}
        onCancel={handleDeleteCancel}
      />
    </>
  );
}