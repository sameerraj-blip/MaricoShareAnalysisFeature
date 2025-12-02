import { useState, useCallback, useEffect } from 'react';
import { Message, UploadResponse } from '@/shared/schema';

export interface HomeState {
  sessionId: string | null;
  messages: Message[];
  initialCharts: UploadResponse['charts'];
  initialInsights: UploadResponse['insights'];
  sampleRows: Record<string, any>[];
  columns: string[];
  numericColumns: string[];
  dateColumns: string[];
  totalRows: number;
  totalColumns: number;
  dataOpsMode: boolean;
}

export const useHomeState = () => {
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [messages, setMessages] = useState<Message[]>([]);
  const [initialCharts, setInitialCharts] = useState<UploadResponse['charts']>([]);
  const [initialInsights, setInitialInsights] = useState<UploadResponse['insights']>([]);
  const [sampleRows, setSampleRows] = useState<Record<string, any>[]>([]);
  const [columns, setColumns] = useState<string[]>([]);
  const [numericColumns, setNumericColumns] = useState<string[]>([]);
  const [dateColumns, setDateColumns] = useState<string[]>([]);
  const [totalRows, setTotalRows] = useState<number>(0);
  const [totalColumns, setTotalColumns] = useState<number>(0);
  const [dataOpsMode, setDataOpsMode] = useState<boolean>(false);

  // Load dataOpsMode from localStorage when sessionId changes
  useEffect(() => {
    if (sessionId) {
      const stored = localStorage.getItem(`dataOpsMode_${sessionId}`);
      if (stored !== null) {
        setDataOpsMode(stored === 'true');
      }
    } else {
      setDataOpsMode(false);
    }
  }, [sessionId]);

  // Save dataOpsMode to localStorage when it changes
  useEffect(() => {
    if (sessionId) {
      localStorage.setItem(`dataOpsMode_${sessionId}`, String(dataOpsMode));
    }
  }, [dataOpsMode, sessionId]);

  const resetState = useCallback(() => {
    setSessionId(null);
    setMessages([]);
    setInitialCharts([]);
    setInitialInsights([]);
    setSampleRows([]);
    setColumns([]);
    setNumericColumns([]);
    setDateColumns([]);
    setTotalRows(0);
    setTotalColumns(0);
    setDataOpsMode(false);
  }, []);

  return {
    // State values
    sessionId,
    messages,
    initialCharts,
    initialInsights,
    sampleRows,
    columns,
    numericColumns,
    dateColumns,
    totalRows,
    totalColumns,
    dataOpsMode,
    
    // State setters
    setSessionId,
    setMessages,
    setInitialCharts,
    setInitialInsights,
    setSampleRows,
    setColumns,
    setNumericColumns,
    setDateColumns,
    setTotalRows,
    setTotalColumns,
    setDataOpsMode,
    
    // Helper functions
    resetState,
  };
};
