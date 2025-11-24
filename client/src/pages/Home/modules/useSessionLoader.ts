import { useEffect } from 'react';

interface UseSessionLoaderProps {
  loadedSessionData?: any;
  setSessionId: (id: string | null) => void;
  setInitialCharts: (charts: any[]) => void;
  setInitialInsights: (insights: any[]) => void;
  setSampleRows: (rows: Record<string, any>[]) => void;
  setColumns: (columns: string[]) => void;
  setNumericColumns: (columns: string[]) => void;
  setDateColumns: (columns: string[]) => void;
  setTotalRows: (rows: number) => void;
  setTotalColumns: (columns: number) => void;
  setMessages: (messages: any[] | ((prev: any[]) => any[])) => void;
  setCollaborators?: (collaborators: string[]) => void;
}

/**
 * Custom hook for loading session data into the Home component
 * Handles populating state when a session is loaded from the Analysis page
 */
export const useSessionLoader = ({
  loadedSessionData,
  setSessionId,
  setInitialCharts,
  setInitialInsights,
  setSampleRows,
  setColumns,
  setNumericColumns,
  setDateColumns,
  setTotalRows,
  setTotalColumns,
  setMessages,
  setCollaborators,
}: UseSessionLoaderProps) => {
  useEffect(() => {
    if (!loadedSessionData) return;
    console.log('🔄 Loading session data into Home component:', loadedSessionData);
    const session = loadedSessionData.session;
    if (!session) return;

    // Set session ID
    setSessionId(session.sessionId);

    // Set collaborators if available
    if (setCollaborators && session.collaborators && Array.isArray(session.collaborators)) {
      setCollaborators(session.collaborators);
    }

    // Set initial charts and insights for the first assistant message context
    setInitialCharts(session.charts || []);
    setInitialInsights(session.insights || []);

    // Set data summary information
    if (session.dataSummary) {
      setSampleRows(session.sampleRows || []);
      setColumns(session.dataSummary.columns?.map((c: any) => c.name) || []);
      setNumericColumns(session.dataSummary.numericColumns || []);
      setDateColumns(session.dataSummary.dateColumns || []);
      setTotalRows(session.dataSummary.rowCount || 0);
      setTotalColumns(session.dataSummary.columnCount || 0);
    }

    // Build an initial analysis message so the user immediately sees the original charts/insights
    const initialAnalysisMessage = {
      role: 'assistant' as const,
      content: `Initial analysis for ${session.fileName}.`,
      charts: session.charts || [],
      insights: session.insights || [],
      timestamp: Date.now(),
    };

    // If backend already has messages, prepend the initial analysis snapshot (unless it already exists)
    if (Array.isArray(session.messages) && session.messages.length > 0) {
      const existing = session.messages as any[];
      const hasChartsInFirst = !!(existing[0]?.charts && existing[0].charts.length);
      const merged = hasChartsInFirst ? existing : [initialAnalysisMessage, ...existing];
      setMessages(merged as any);
    } else {
      // Otherwise show just the initial analysis snapshot
      setMessages([initialAnalysisMessage] as any);
    }
  }, [
    loadedSessionData,
    setSessionId,
    setInitialCharts,
    setInitialInsights,
    setSampleRows,
    setColumns,
    setNumericColumns,
    setDateColumns,
    setTotalRows,
    setTotalColumns,
    setMessages,
    setCollaborators,
  ]);
};

