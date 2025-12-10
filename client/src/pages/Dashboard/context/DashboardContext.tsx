import React, { createContext, useContext, ReactNode } from 'react';
import { useDashboardState, DashboardData } from '../modules/useDashboardState';

interface DashboardContextType {
  dashboards: DashboardData[];
  currentDashboard: DashboardData | null;
  setCurrentDashboard: (dashboard: DashboardData | null) => void;
  createDashboard: (name: string) => Promise<DashboardData>;
  addChartToDashboard: (dashboardId: string, chart: any, sheetId?: string) => Promise<DashboardData>;
  removeChartFromDashboard: (dashboardId: string, chartIndex: number, sheetId?: string) => Promise<DashboardData>;
  deleteDashboard: (dashboardId: string) => Promise<void>;
  renameDashboard: (dashboardId: string, name: string) => Promise<DashboardData>;
  renameSheet: (dashboardId: string, sheetId: string, name: string) => Promise<DashboardData>;
  addSheet: (dashboardId: string, name: string) => Promise<DashboardData>;
  removeSheet: (dashboardId: string, sheetId: string) => Promise<DashboardData>;
  updateChartInsightOrRecommendation: (dashboardId: string, chartIndex: number, updates: { keyInsight?: string }, sheetId?: string) => Promise<DashboardData>;
  getDashboardById: (dashboardId: string) => DashboardData | undefined;
  fetchDashboardById: (dashboardId: string) => Promise<DashboardData>;
  status: {
    isLoading: boolean;
    isFetching: boolean;
    error: unknown;
    refreshing: boolean;
  };
  refetch: () => Promise<any>;
}

export const DashboardContext = createContext<DashboardContextType | undefined>(undefined);

interface DashboardProviderProps {
  children: ReactNode;
}

export function DashboardProvider({ children }: DashboardProviderProps) {
  const dashboardState = useDashboardState();

  return (
    <DashboardContext.Provider value={dashboardState}>
      {children}
    </DashboardContext.Provider>
  );
}

export function useDashboardContext() {
  const context = useContext(DashboardContext);
  if (context === undefined) {
    throw new Error('useDashboardContext must be used within a DashboardProvider');
  }
  return context;
}
