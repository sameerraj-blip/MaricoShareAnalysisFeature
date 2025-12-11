import { useState, useCallback, useEffect } from 'react';
import { Message, UploadResponse } from '@/shared/schema';

export interface HomeState {
  sessionId: string | null;
  fileName: string | null;
  messages: Message[];
  initialCharts: UploadResponse['charts'];
  initialInsights: UploadResponse['insights'];
  sampleRows: Record<string, any>[];
  columns: string[];
  numericColumns: string[];
  dateColumns: string[];
  totalRows: number;
  totalColumns: number;
  mode: 'general' | 'analysis' | 'dataOps' | 'modeling';
}

export const useHomeState = () => {
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [fileName, setFileName] = useState<string | null>(null);
  const [messages, setMessages] = useState<Message[]>([]);
  const [initialCharts, setInitialCharts] = useState<UploadResponse['charts']>([]);
  const [initialInsights, setInitialInsights] = useState<UploadResponse['insights']>([]);
  const [sampleRows, setSampleRows] = useState<Record<string, any>[]>([]);
  const [columns, setColumns] = useState<string[]>([]);
  const [numericColumns, setNumericColumns] = useState<string[]>([]);
  const [dateColumns, setDateColumns] = useState<string[]>([]);
  const [totalRows, setTotalRows] = useState<number>(0);
  const [totalColumns, setTotalColumns] = useState<number>(0);
  const [mode, setMode] = useState<'general' | 'analysis' | 'dataOps' | 'modeling'>('general');

  // Load mode from localStorage when sessionId changes
  useEffect(() => {
    if (sessionId) {
      const stored = localStorage.getItem(`mode_${sessionId}`);
      if (stored && (stored === 'general' || stored === 'analysis' || stored === 'dataOps' || stored === 'modeling')) {
        setMode(stored as 'general' | 'analysis' | 'dataOps' | 'modeling');
      } else {
        // Default to 'general' if no stored mode or invalid value
        setMode('general');
      }
    } else {
      setMode('general');
    }
  }, [sessionId]);

  // Save mode to localStorage when it changes
  useEffect(() => {
    if (sessionId) {
      localStorage.setItem(`mode_${sessionId}`, mode);
    }
  }, [mode, sessionId]);

  const resetState = useCallback(() => {
    setSessionId(null);
    setFileName(null);
    setMessages([]);
    setInitialCharts([]);
    setInitialInsights([]);
    setSampleRows([]);
    setColumns([]);
    setNumericColumns([]);
    setDateColumns([]);
    setTotalRows(0);
    setTotalColumns(0);
    setMode('general');
  }, []);

  return {
    // State values
    sessionId,
    fileName,
    messages,
    initialCharts,
    initialInsights,
    sampleRows,
    columns,
    numericColumns,
    dateColumns,
    totalRows,
    totalColumns,
    mode,
    
    // State setters
    setSessionId,
    setFileName,
    setMessages,
    setInitialCharts,
    setInitialInsights,
    setSampleRows,
    setColumns,
    setNumericColumns,
    setDateColumns,
    setTotalRows,
    setTotalColumns,
    setMode,
    
    // Helper functions
    resetState,
  };
};
