import { useEffect, useState } from 'react';
import { FileUpload } from '@/pages/Home/Components/FileUpload';
import { ChatInterface } from './Components/ChatInterface';
import { useHomeState, useHomeMutations, useHomeHandlers, useSessionLoader } from './modules';
import { sessionsApi } from '@/lib/api';
import { useChatMessagesStream } from '@/hooks/useChatMessagesStream';

interface HomeProps {
  resetTrigger?: number;
  loadedSessionData?: any;
}

export default function Home({ resetTrigger = 0, loadedSessionData }: HomeProps) {
  const [isLoadingHistory, setIsLoadingHistory] = useState(false);
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [collaborators, setCollaborators] = useState<string[]>([]);
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
    dataOpsMode,
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
    resetState,
  } = useHomeState();

  const { uploadMutation, chatMutation, cancelChatRequest, thinkingSteps, thinkingTargetTimestamp } = useHomeMutations({
    sessionId,
    messages,
    dataOpsMode,
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
      if (data) {
        if (data.session) {
          // Handle response with session object
          if (Array.isArray(data.session.messages)) {
            setMessages(data.session.messages as any);
          }
          if (data.session.collaborators && Array.isArray(data.session.collaborators)) {
            setCollaborators(data.session.collaborators);
          }
        } else {
          // Handle direct response
          if (Array.isArray(data.messages)) {
            setMessages(data.messages as any);
          }
          if (data.collaborators && Array.isArray(data.collaborators)) {
            setCollaborators(data.collaborators);
          }
        }
      }
    } catch (e) {
      console.error('Failed to load chat history', e);
    } finally {
      setIsLoadingHistory(false);
    }
  };

  // Real-time chat message streaming for collaborative sessions
  useChatMessagesStream({
    sessionId,
    enabled: !!sessionId,
    onNewMessages: (newMessages) => {
      // Append new messages to existing messages, avoiding duplicates
      setMessages((prev) => {
        // Avoid duplicates by checking timestamps
        const existingTimestamps = new Set(prev.map(m => m.timestamp));
        const uniqueNewMessages = newMessages.filter(m => !existingTimestamps.has(m.timestamp));
        
        if (uniqueNewMessages.length > 0) {
          return [...prev, ...uniqueNewMessages];
        }
        return prev;
      });
    },
  });

  // Reset state only when resetTrigger changes (upload new file)
  useEffect(() => {
    if (resetTrigger > 0) {
      resetState();
      setSuggestions([]); // Clear suggestions when resetting
    }
  }, [resetTrigger, resetState]);

  // Load session data when provided (and populate existing chat history)
  useSessionLoader({
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
  });

  // Fetch collaborators when sessionId is available
  useEffect(() => {
    const fetchCollaborators = async () => {
      if (!sessionId) return;
      try {
        const data = await sessionsApi.getSessionDetails(sessionId);
        if (data) {
          const sessionData = data.session || data;
          if (sessionData.collaborators && Array.isArray(sessionData.collaborators)) {
            setCollaborators(sessionData.collaborators);
          }
        }
      } catch (e) {
        console.error('Failed to fetch collaborators', e);
      }
    };
    fetchCollaborators();
  }, [sessionId]);

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
      collaborators={collaborators}
      dataOpsMode={dataOpsMode}
      onDataOpsModeChange={setDataOpsMode}
    />
  );
}
