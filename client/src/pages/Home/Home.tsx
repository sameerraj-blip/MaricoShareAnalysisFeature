import { useEffect, useState, useRef } from 'react';
import { FileUpload } from '@/pages/Home/Components/FileUpload';
import { ChatInterface } from './Components/ChatInterface';
import { useHomeState, useHomeMutations, useHomeHandlers, useSessionLoader } from './modules';
import { sessionsApi } from '@/lib/api';
import { useChatMessagesStream } from '@/hooks/useChatMessagesStream';
import { Message } from '@/shared/schema';

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
  // Track if we're waiting for initial analysis (have sessionId but no initial analysis yet)
  const [waitingForInitialAnalysis, setWaitingForInitialAnalysis] = useState(false);
  
  // Set waiting state when we have a sessionId but no initial analysis yet
  useEffect(() => {
    if (sessionId && !initialAnalysisComplete) {
      // Check if we already have initial analysis in messages
      const initialAnalysisMsg = messages.find(msg => 
        msg.role === 'assistant' && 
        msg.content && 
        (msg.content.toLowerCase().includes('initial analysis for') || 
         msg.charts?.length > 0 || 
         msg.insights?.length > 0)
      );
      
      if (!initialAnalysisMsg) {
        if (!waitingForInitialAnalysis) {
          console.log('⏳ Setting waitingForInitialAnalysis=true - no initial analysis found yet');
        }
        setWaitingForInitialAnalysis(true);
      } else {
        if (waitingForInitialAnalysis) {
          console.log('✅ Setting waitingForInitialAnalysis=false - initial analysis found:', {
            chartsCount: initialAnalysisMsg.charts?.length || 0,
            insightsCount: initialAnalysisMsg.insights?.length || 0,
          });
        }
        setWaitingForInitialAnalysis(false);
        setInitialAnalysisComplete(true);
      }
    } else {
      setWaitingForInitialAnalysis(false);
    }
  }, [sessionId, initialAnalysisComplete, messages, waitingForInitialAnalysis]);

  // Poll for initial analysis if we're waiting and don't have it yet
  const pollingRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const shouldPollRef = useRef(false);
  
  useEffect(() => {
    shouldPollRef.current = !!sessionId && !initialAnalysisComplete && waitingForInitialAnalysis;
  }, [sessionId, initialAnalysisComplete, waitingForInitialAnalysis]);

  useEffect(() => {
    // Clear any existing polling
    if (pollingRef.current) {
      clearTimeout(pollingRef.current);
      pollingRef.current = null;
    }

    if (!shouldPollRef.current) {
      return;
    }

    console.log('🔄 Starting polling for initial analysis...');
    let pollCount = 0;
    const maxPolls = 60; // Poll for up to 5 minutes (60 * 5 seconds)
    const pollInterval = 5000; // Poll every 5 seconds
    let isPolling = true;

    const pollForInitialAnalysis = async () => {
      // Check if we should still be polling
      if (!shouldPollRef.current || !isPolling) {
        console.log('🛑 Stopping polling - conditions changed');
        return;
      }

      pollCount++;
      console.log(`🔄 Polling for initial analysis (attempt ${pollCount}/${maxPolls})...`);

      try {
        const data = await sessionsApi.getSessionDetails(sessionId!);
        if (data) {
          const session = data.session || data;
          const sessionMessages = session.messages || [];
          
          // Check if we have initial analysis in the session
          const hasInitialAnalysis = sessionMessages.some((msg: any) => 
            msg.role === 'assistant' && 
            msg.content && 
            (msg.content.toLowerCase().includes('initial analysis for') || 
             msg.charts?.length > 0 || 
             msg.insights?.length > 0)
          );

          if (hasInitialAnalysis && sessionMessages.length > 0) {
            console.log('✅ Initial analysis found via polling! Loading messages...', {
              messagesCount: sessionMessages.length,
              chartsCount: session.charts?.length || 0,
              insightsCount: session.insights?.length || 0,
            });
            
            // Stop polling
            isPolling = false;
            if (pollingRef.current) {
              clearTimeout(pollingRef.current);
              pollingRef.current = null;
            }
            
            // Load the messages - replace existing messages
            setMessages(sessionMessages as any);
            
            // Also update other state if available
            if (session.charts && session.charts.length > 0) {
              setInitialCharts(session.charts);
            }
            if (session.insights && session.insights.length > 0) {
              setInitialInsights(session.insights);
            }
            
            // Update metadata if available
            if (session.dataSummary) {
              if (session.sampleRows && session.sampleRows.length > 0) {
                setSampleRows(session.sampleRows);
              }
              if (session.dataSummary.columns) {
                setColumns(session.dataSummary.columns.map((c: any) => c.name));
              }
              setNumericColumns(session.dataSummary.numericColumns || []);
              setDateColumns(session.dataSummary.dateColumns || []);
              setTotalRows(session.dataSummary.rowCount);
              setTotalColumns(session.dataSummary.columnCount);
            }
            
            setInitialAnalysisComplete(true);
            setWaitingForInitialAnalysis(false);
            return; // Stop polling
          }
        }

        // Continue polling if we haven't found it yet and haven't exceeded max polls
        if (isPolling && pollCount < maxPolls && shouldPollRef.current) {
          pollingRef.current = setTimeout(pollForInitialAnalysis, pollInterval);
        } else if (pollCount >= maxPolls) {
          console.warn('⚠️ Polling timeout: Initial analysis not found after maximum attempts');
          isPolling = false;
        }
      } catch (error) {
        console.error('❌ Error polling for initial analysis:', error);
        // Continue polling on error (might be temporary)
        if (isPolling && pollCount < maxPolls && shouldPollRef.current) {
          pollingRef.current = setTimeout(pollForInitialAnalysis, pollInterval);
        }
      }
    };

    // Start polling after a short delay
    pollingRef.current = setTimeout(pollForInitialAnalysis, pollInterval);

    // Cleanup: stop polling if component unmounts or dependencies change
    return () => {
      isPolling = false;
      if (pollingRef.current) {
        clearTimeout(pollingRef.current);
        pollingRef.current = null;
      }
    };
  }, [sessionId, waitingForInitialAnalysis, initialAnalysisComplete, setMessages, setInitialCharts, setInitialInsights, setSampleRows, setColumns, setNumericColumns, setDateColumns, setTotalRows, setTotalColumns]);

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

        // Helper function to check if a message already exists in previous messages
        const messageExists = (msg: Message, existingMessages: Message[]): boolean => {
          if (!msg.content || !msg.timestamp) return false;
          
          // Check for exact match (same role, content, and timestamp)
          const exactMatch = existingMessages.some(existing => 
            existing.role === msg.role &&
            existing.content === msg.content &&
            existing.timestamp === msg.timestamp
          );
          
          if (exactMatch) return true;
          
          // Check for similar match (same role and content, timestamp within 5 seconds)
          // This handles cases where timestamps might differ slightly
          const similarMatch = existingMessages.some(existing => 
            existing.role === msg.role &&
            existing.content === msg.content &&
            Math.abs((existing.timestamp || 0) - (msg.timestamp || 0)) < 5000
          );
          
          return similarMatch;
        };

        // Process new messages - filter out duplicates and user messages that are already in state
        const uniqueNewMessages: Message[] = [];
        const seenUnwantedInitial = new Set<string>();
        
        for (const newMsg of newMessages) {
          if (!newMsg.content) {
            continue; // Skip messages without content
          }
          
          // CRITICAL FIX: Filter out user messages from SSE
          // User messages are already in local state when the user sends them
          // Receiving them again from SSE can cause loops and duplicate submissions
          if (newMsg.role === 'user') {
            // Check if this user message already exists in previous messages
            if (messageExists(newMsg, prev)) {
              console.log('🔄 SSE: Filtering out duplicate user message from SSE:', newMsg.content.substring(0, 50));
              continue;
            }
            // Even if it doesn't exist, we should be cautious about adding user messages from SSE
            // Only add if it's significantly different (more than 10 seconds old, suggesting it's from another session/collaborator)
            const messageAge = Date.now() - (newMsg.timestamp || 0);
            if (messageAge < 10000) {
              console.log('🔄 SSE: Filtering out recent user message from SSE (likely duplicate):', newMsg.content.substring(0, 50));
              continue;
            }
          }
          
          // FILTER OUT the unwanted "Hi! I've just finished analyzing..." message
          // BUT: Keep messages that have charts or insights - those are the real initial analysis
          // We only filter out the unwanted message if it has NO charts/insights (just text summary)
          if (isUnwantedInitialMessage(newMsg)) {
            // CRITICAL: Don't filter if message has charts or insights - that's the real initial analysis
            if (newMsg.charts && newMsg.charts.length > 0) {
              console.log('✅ Keeping initial analysis message with charts (even if content matches unwanted pattern)');
              // Don't filter - this is the real initial analysis
            } else if (newMsg.insights && newMsg.insights.length > 0) {
              console.log('✅ Keeping initial analysis message with insights (even if content matches unwanted pattern)');
              // Don't filter - this is the real initial analysis
            } else {
              // Only filter if it's just the text summary without charts/insights
              const normalizedContent = normalizeContent(newMsg.content);
              const key = `${newMsg.role}|${normalizedContent}`;
              
              // Only filter if we've already seen this unwanted initial message
              if (seenUnwantedInitial.has(key)) {
                console.log('🔄 SSE: Filtering out duplicate unwanted initial analysis message (no charts/insights)');
                continue;
              }
              
              // Check if this unwanted message already exists in previous messages
              const existsInPrev = prev.some(msg => {
                if (!msg.content) return false;
                return isUnwantedInitialMessage(msg) && !msg.charts?.length && !msg.insights?.length;
              });
              
              if (existsInPrev) {
                console.log('🔄 SSE: Filtering out unwanted initial analysis message (already exists, no charts/insights)');
                continue;
              }
              
              seenUnwantedInitial.add(key);
            }
          }
          
          // Check if this message already exists in previous messages (deduplication)
          if (messageExists(newMsg, prev)) {
            console.log('🔄 SSE: Filtering out duplicate message:', newMsg.content.substring(0, 50));
            continue;
          }
          
          uniqueNewMessages.push(newMsg);
        }

        // Check if this is the initial analysis message - if so, mark analysis as complete
        // The SSE connection should already be closed by the hook, but we mark it here too
        if (uniqueNewMessages.length > 0 && !initialAnalysisComplete) {
          const initialAnalysisMsg = uniqueNewMessages.find(msg => 
            msg.role === 'assistant' && 
            msg.content && 
            (msg.content.toLowerCase().includes('initial analysis for') || 
             msg.charts?.length > 0 || 
             msg.insights?.length > 0)
          );
          
          if (initialAnalysisMsg) {
            console.log('✅ Initial analysis detected - SSE connection should already be closed');
            console.log('📊 Initial analysis message details:', {
              hasContent: !!initialAnalysisMsg.content,
              chartsCount: initialAnalysisMsg.charts?.length || 0,
              insightsCount: initialAnalysisMsg.insights?.length || 0,
              contentPreview: initialAnalysisMsg.content?.substring(0, 100),
            });
          setInitialAnalysisComplete(true);
            setWaitingForInitialAnalysis(false); // Stop showing loading state
            console.log('✅ State updated: initialAnalysisComplete=true, waitingForInitialAnalysis=false');
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
      setWaitingForInitialAnalysis(false); // Reset waiting state
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

  // Show loading state when waiting for initial analysis from backend
  if (waitingForInitialAnalysis && sessionId) {
    return (
      <div className="h-[calc(100vh-80px)] bg-gradient-to-br from-slate-50 to-white flex items-center justify-center">
        <div className="text-center">
          <div className="relative mb-4">
            <div className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center mx-auto">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
            </div>
          </div>
          <h3 className="text-lg font-semibold text-gray-900 mb-1">Analyzing your data...</h3>
          <p className="text-sm text-gray-500">This may take a few moments for large files</p>
          {fileName && (
            <p className="text-xs text-gray-400 mt-2">Processing: {fileName}</p>
          )}
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
