import { useEffect, useState, useRef } from 'react';
import { FileUpload } from '@/pages/Home/Components/FileUpload';
import { ChatInterface } from './Components/ChatInterface';
import { useHomeState, useHomeMutations, useHomeHandlers, useSessionLoader } from './modules';
import { sessionsApi } from '@/lib/api';
import { useChatMessagesStream } from '@/hooks/useChatMessagesStream';

interface HomeProps {
  resetTrigger?: number;
  loadedSessionData?: any;
  initialMode?: 'general' | 'analysis' | 'dataOps' | 'modeling';
  onModeChange?: (mode: 'general' | 'analysis' | 'dataOps' | 'modeling') => void;
  onSessionChange?: (sessionId: string | null, fileName: string | null) => void;
}

export default function Home({ resetTrigger = 0, loadedSessionData, initialMode, onModeChange, onSessionChange }: HomeProps) {
  const [isLoadingHistory, setIsLoadingHistory] = useState(false);
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [collaborators, setCollaborators] = useState<string[]>([]);
  const {
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
    resetState,
  } = useHomeState();

  const { uploadMutation, chatMutation, cancelChatRequest, thinkingSteps, thinkingTargetTimestamp } = useHomeMutations({
    sessionId,
    messages,
    mode,
    setSessionId,
    setFileName,
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

  // Track if initial analysis is complete - disable SSE after that
  const [initialAnalysisComplete, setInitialAnalysisComplete] = useState(false);

  // Real-time chat message streaming for collaborative sessions
  // Only enabled until initial analysis is complete, then disabled to prevent duplicates
  useChatMessagesStream({
    sessionId,
    enabled: !!sessionId && !initialAnalysisComplete, // Disable after initial analysis
    onNewMessages: (newMessages) => {
      // Append new messages to existing messages, avoiding duplicates
      setMessages((prev) => {
        // Early return if no new messages
        if (!newMessages || newMessages.length === 0) {
          return prev;
        }

        // Helper function to normalize content for comparison (remove extra whitespace, normalize quotes)
        const normalizeContent = (content: string): string => {
          if (!content) return '';
          return content
            .replace(/\s+/g, ' ')
            .replace(/[""]/g, '"')
            .replace(/['']/g, "'")
            .trim()
            .toLowerCase();
        };

        // Helper function to check if a message is the unwanted "Hi! I've just finished analyzing..." message
        // This is different from the "Initial analysis for [filename]" message which we want to keep
        const isUnwantedInitialMessage = (msg: Message): boolean => {
          if (msg.role !== 'assistant' || !msg.content) return false;
          const content = normalizeContent(msg.content);
          // Check for the unwanted message pattern: "Hi! I've just finished analyzing your data..."
          // This message contains dataset summary info that's redundant with the UI components
          return (
            (content.includes("i've just finished analyzing") || content.includes("hi! 👋 i've just finished")) &&
            (content.includes("your dataset has") && content.includes("rows and") && content.includes("columns")) &&
            (content.includes("numeric columns to work with") || content.includes("date columns for time-based"))
          );
        };

        // Process new messages - only filter out unwanted initial messages
        // Allow users to ask the same question multiple times, so we don't deduplicate regular messages
        const uniqueNewMessages: Message[] = [];
        const seenUnwantedInitial = new Set<string>();
        
        for (const newMsg of newMessages) {
          if (!newMsg.content) {
            continue; // Skip messages without content
          }
          
          // FILTER OUT the unwanted "Hi! I've just finished analyzing..." message
          // We only want the "Initial analysis for [filename]" message which shows the UI components
          if (isUnwantedInitialMessage(newMsg)) {
            const normalizedContent = normalizeContent(newMsg.content);
            const key = `${newMsg.role}|${normalizedContent}`;
            
            // Only filter if we've already seen this unwanted initial message
            if (seenUnwantedInitial.has(key)) {
              console.log('🔄 SSE: Filtering out duplicate unwanted initial analysis message');
              continue;
            }
            
            // Check if this unwanted message already exists in previous messages
            const existsInPrev = prev.some(msg => {
              if (!msg.content) return false;
              return isUnwantedInitialMessage(msg);
            });
            
            if (existsInPrev) {
              console.log('🔄 SSE: Filtering out unwanted initial analysis message (already exists)');
              continue;
            }
            
            seenUnwantedInitial.add(key);
          }
          
          // For all other messages (including regular chat), allow duplicates
          // Users should be able to ask the same question multiple times
          uniqueNewMessages.push(newMsg);
        }
        
        // Check if this is the initial analysis message - if so, mark analysis as complete
        // The SSE connection should already be closed by the hook, but we mark it here too
        if (uniqueNewMessages.length > 0 && !initialAnalysisComplete) {
          const hasInitialAnalysis = uniqueNewMessages.some(msg => 
            msg.role === 'assistant' && 
            msg.content && 
            (msg.content.toLowerCase().includes('initial analysis for') || 
             msg.charts?.length > 0 || 
             msg.insights?.length > 0)
          );
          if (hasInitialAnalysis) {
            console.log('✅ Initial analysis detected - SSE connection should already be closed');
            setInitialAnalysisComplete(true);
          }
        }
        
        // Combine: keep all existing messages + add only unique new messages
        const result = [...prev, ...uniqueNewMessages];
        
        if (uniqueNewMessages.length < newMessages.length) {
          console.log(`✅ SSE Deduplication: ${newMessages.length} → ${uniqueNewMessages.length} unique messages added`);
        }
        
        return result;
      });
    },
  });

  // Sync mode with initialMode prop (from URL) - only when initialMode changes
  // This handles backward compatibility when coming from old routes (/data-ops, /modeling)
  useEffect(() => {
    if (initialMode && initialMode !== mode && initialMode !== 'general') {
      // Only set mode if it's a specific mode (not 'general'), for backward compatibility
      setMode(initialMode);
    }
  }, [initialMode]); // Only depend on initialMode, not mode

  // Deduplicate messages to catch any duplicates that might slip through
  // Specifically ensure only ONE initial message exists
  useEffect(() => {
    if (messages.length <= 1) return;
    
    // Helper function to normalize content for comparison
    const normalizeContent = (content: string): string => {
      return content
        .replace(/\s+/g, ' ')
        .replace(/[""]/g, '"')
        .replace(/['']/g, "'")
        .trim()
        .toLowerCase();
    };

    // Helper function to check if a message is the unwanted "Hi! I've just finished analyzing..." message
    // This is different from the "Initial analysis for [filename]" message which we want to keep
    const isUnwantedInitialMessage = (msg: Message): boolean => {
      if (msg.role !== 'assistant' || !msg.content) return false;
      const content = normalizeContent(msg.content);
      // Check for the unwanted message pattern: "Hi! I've just finished analyzing your data..."
      // This message contains dataset summary info that's redundant with the UI components
      return (
        (content.includes("i've just finished analyzing") || content.includes("hi! 👋 i've just finished")) &&
        (content.includes("your dataset has") && content.includes("rows and") && content.includes("columns")) &&
        (content.includes("numeric columns to work with") || content.includes("date columns for time-based"))
      );
    };

    // Helper function to check if two messages are essentially the same
    const areMessagesSimilar = (msg1: Message, msg2: Message): boolean => {
      if (msg1.role !== msg2.role) return false;
      if (!msg1.content || !msg2.content) return false;
      
      const content1 = normalizeContent(msg1.content);
      const content2 = normalizeContent(msg2.content);
      
      // Exact match after normalization
      if (content1 === content2) return true;
      
      // For assistant messages, check if they're very similar
      if (msg1.role === 'assistant') {
        // Check if one contains the other (for truncated messages)
        if (content1.includes(content2) || content2.includes(content1)) {
          return true;
        }
        
        // Check for unwanted initial analysis message patterns - if both are unwanted initial messages, they're duplicates
        if (isUnwantedInitialMessage(msg1) && isUnwantedInitialMessage(msg2)) {
          return true;
        }
      }
      
      return false;
    };

    // Periodic cleanup - only filter out duplicate unwanted initial messages
    // Allow users to ask the same question multiple times, so we don't deduplicate regular messages
    const seenUnwantedInitial = new Set<string>();
    let hasUnwantedInitial = false;
    
    const cleanedMessages = messages.filter((msg, index) => {
      // Only filter unwanted initial messages, not regular chat messages
      if (isUnwantedInitialMessage(msg)) {
        const normalizedContent = normalizeContent(msg.content || '');
        const key = `${msg.role}|${normalizedContent}`;
        
        // Keep only the first unwanted initial message
        if (hasUnwantedInitial || seenUnwantedInitial.has(key)) {
          console.log('🔄 Periodic cleanup: Removing duplicate unwanted initial analysis message');
          return false;
        }
        
        hasUnwantedInitial = true;
        seenUnwantedInitial.add(key);
      }
      
      // Keep all other messages (including duplicates of regular chat)
      return true;
    });
    
    // Only update if we removed unwanted initial message duplicates
    if (cleanedMessages.length < messages.length) {
      console.log(`🔄 Periodic cleanup: Removed ${messages.length - cleanedMessages.length} duplicate unwanted initial message(s)`);
      // Use functional update to avoid dependency issues
      setMessages(() => cleanedMessages);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [messages.length]); // Only depend on length to avoid infinite loops

  // Reset state only when resetTrigger changes (upload new file)
  // Only reset if resetTrigger > 0 AND we're not loading a session
  useEffect(() => {
    if (resetTrigger > 0 && !loadedSessionData) {
      resetState();
      setSuggestions([]); // Clear suggestions when resetting
      setInitialAnalysisComplete(false); // Reset SSE completion flag
    }
  }, [resetTrigger, resetState, loadedSessionData]);

  // Load session data when provided (and populate existing chat history)
  useSessionLoader({
    loadedSessionData,
    setSessionId,
    setFileName,
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

  // Notify parent when sessionId or fileName changes
  useEffect(() => {
    if (onSessionChange) {
      onSessionChange(sessionId, fileName);
    }
  }, [sessionId, fileName, onSessionChange]);

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

  // Don't show file upload if we're loading a session (even if sessionId isn't set yet)
  // Only show file upload if there's no session data being loaded AND no sessionId
  // Only auto-open the dialog when resetTrigger > 0 (explicitly starting new analysis)
  if (!sessionId && !loadedSessionData) {
    return (
      <FileUpload
        onFileSelect={handleFileSelect}
        isUploading={uploadMutation.isPending}
        autoOpenTrigger={resetTrigger > 0 ? resetTrigger : 0}
      />
    );
  }

  // If we're loading a session but sessionId isn't set yet, show loading state
  if (!sessionId && loadedSessionData) {
    return (
      <div className="h-[calc(100vh-80px)] bg-gradient-to-br from-slate-50 to-white flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary mx-auto mb-4"></div>
          <p className="text-gray-600">Loading analysis...</p>
        </div>
      </div>
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
      mode={mode}
      sessionId={sessionId}
      onModeChange={(newMode) => {
        setMode(newMode);
        // onModeChange will update the URL, which will update initialMode prop
        if (onModeChange) {
          onModeChange(newMode);
        }
      }}
    />
  );
}
