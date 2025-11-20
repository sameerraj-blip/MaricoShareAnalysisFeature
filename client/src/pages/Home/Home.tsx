import { useEffect, useState } from 'react';
import { FileUpload } from '@/pages/Home/Components/FileUpload';
import { ChatInterface } from './Components/ChatInterface';
import { useHomeState, useHomeMutations, useHomeHandlers } from './modules';
import { sessionsApi } from '@/lib/api';

interface HomeProps {
  resetTrigger?: number;
  loadedSessionData?: any;
}

export default function Home({ resetTrigger = 0, loadedSessionData }: HomeProps) {
  const [isLoadingHistory, setIsLoadingHistory] = useState(false);
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const {
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
    resetState,
  } = useHomeState();

  const { uploadMutation, chatMutation, cancelChatRequest, thinkingSteps, thinkingTargetTimestamp } = useHomeMutations({
    sessionId,
    messages,
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
    setSuggestions,
  });

  const { handleFileSelect, handleSendMessage, handleUploadNew, handleEditMessage } = useHomeHandlers({
    sessionId,
    messages,
    setMessages,
    uploadMutation,
    chatMutation,
    resetState,
  });

  const handleStopGeneration = () => {
    cancelChatRequest();
  };

  const handleLoadHistory = async () => {
    if (!sessionId || isLoadingHistory) return;
    setIsLoadingHistory(true);
    try {
      const data = await sessionsApi.getSessionDetails(sessionId);
      if (data && Array.isArray(data.messages)) {
        setMessages(data.messages as any);
      }
    } catch (e) {
      console.error('Failed to load chat history', e);
    } finally {
      setIsLoadingHistory(false);
    }
  };

  // Reset state only when resetTrigger changes (upload new file)
  useEffect(() => {
    if (resetTrigger > 0) {
      resetState();
      setSuggestions([]); // Clear suggestions when resetting
    }
  }, [resetTrigger, resetState]);

  // Load session data when provided (and populate existing chat history)
  useEffect(() => {
    if (!loadedSessionData) return;
    console.log('🔄 Loading session data into Home component:', loadedSessionData);
    const session = loadedSessionData.session;
    if (!session) return;

    // Set session ID
    setSessionId(session.sessionId);

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
  }, [loadedSessionData, setSessionId, setInitialCharts, setInitialInsights, setSampleRows, setColumns, setNumericColumns, setDateColumns, setTotalRows, setTotalColumns, setMessages]);

  if (!sessionId) {
    return (
      <FileUpload
        onFileSelect={handleFileSelect}
        isUploading={uploadMutation.isPending}
        autoOpenTrigger={resetTrigger}
      />
    );
  }

  return (
    <ChatInterface
      messages={messages}
      onSendMessage={handleSendMessage}
      onUploadNew={handleUploadNew}
      isLoading={chatMutation.isPending}
      onLoadHistory={handleLoadHistory}
      canLoadHistory={!!sessionId}
      loadingHistory={isLoadingHistory}
      sampleRows={sampleRows}
      columns={columns}
      numericColumns={numericColumns}
      dateColumns={dateColumns}
      totalRows={totalRows}
      totalColumns={totalColumns}
      onStopGeneration={handleStopGeneration}
      onEditMessage={handleEditMessage}
      thinkingSteps={thinkingSteps}
      thinkingTargetTimestamp={thinkingTargetTimestamp}
      aiSuggestions={suggestions}
    />
  );
}
