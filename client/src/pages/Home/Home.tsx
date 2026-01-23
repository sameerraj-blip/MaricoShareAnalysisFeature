import { useEffect, useState, useRef, useCallback } from 'react';
import { FileUpload } from '@/pages/Home/Components/FileUpload';
import { ChatInterface } from './Components/ChatInterface';
import { ContextModal } from './Components/ContextModal';
import { DataSummaryModal } from './Components/DataSummaryModal';
import { useHomeState, useHomeMutations, useHomeHandlers, useSessionLoader } from './modules';
import { sessionsApi } from '@/lib/api';
import { useChatMessagesStream } from '@/hooks/useChatMessagesStream';
import { Message } from '@/shared/schema';
import { useToast } from '@/hooks/use-toast';
import { logger } from '@/lib/logger';

// Helper function to check if a message is initial analysis
// Moved outside component to prevent recreation on each render
const isInitialAnalysisMessage = (msg: Message): boolean => {
  if (msg.role !== 'assistant') return false;
  
  // Check for initial analysis by:
  // 1. Content containing "initial analysis for" OR "I've just finished analyzing"
  // 2. Having charts
  // 3. Having insights
  const content = msg.content?.toLowerCase() || '';
  return (
    content.includes('initial analysis for') ||
    content.includes("i've just finished analyzing") ||
    content.includes("just finished analyzing") ||
    (msg.charts && msg.charts.length > 0) ||
    (msg.insights && msg.insights.length > 0)
  );
};

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
  const [isLargeFileLoading, setIsLargeFileLoading] = useState(false);
  const [showContextModal, setShowContextModal] = useState(false);
  const [contextModalSessionId, setContextModalSessionId] = useState<string | null>(null);
  const [isSavingContext, setIsSavingContext] = useState(false);
  const [showDataSummaryModal, setShowDataSummaryModal] = useState(false);
  const contextModalShownRef = useRef<Set<string>>(new Set());
  const { toast } = useToast();
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
    setIsLargeFileLoading,
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
      logger.error('Failed to load chat history', e);
    } finally {
      setIsLoadingHistory(false);
    }
  };

  // Track if initial analysis has been received
  const [initialAnalysisReceived, setInitialAnalysisReceived] = useState(false);

  // Fallback: Fetch messages from session API when SSE completes but no messages received
  const fetchMessagesFromSession = useCallback(async (retryCount = 0): Promise<boolean> => {
    if (!sessionId || initialAnalysisReceived) {
      logger.log('⏭️ Skipping fetch - no sessionId or already received initial analysis');
      return false;
    }
    
    const MAX_RETRIES = 24; // Reduced to 24 retries (2 minutes max with exponential backoff)
    
    try {
      logger.log(`📥 Fetching messages from session API (attempt ${retryCount + 1}/${MAX_RETRIES})...`);
      const data = await sessionsApi.getSessionDetails(sessionId);
      const session = data.session || data;
      
      logger.log('📋 Session data:', {
        hasSession: !!session,
        hasMessages: !!(session?.messages),
        messagesLength: session?.messages?.length || 0,
        hasCharts: !!(session?.charts?.length),
        hasInsights: !!(session?.insights?.length)
      });
      
      // Check for charts/insights FIRST - this indicates analysis is ready
      // Even if messages exist, we need charts/insights to show the analysis
      const hasCharts = session?.charts && Array.isArray(session.charts) && session.charts.length > 0;
      const hasInsights = session?.insights && Array.isArray(session.insights) && session.insights.length > 0;
      
      if (hasCharts || hasInsights) {
        logger.log('✅ Analysis ready - charts/insights found');
        
        // If we have messages with charts/insights, use them
        if (session.messages && Array.isArray(session.messages) && session.messages.length > 0) {
          const hasInitialAnalysis = session.messages.some((msg: Message) => isInitialAnalysisMessage(msg));
          if (hasInitialAnalysis) {
            logger.log('✅ Initial analysis found in messages - setting messages and clearing loading state');
            setMessages(session.messages);
            setIsLargeFileLoading(false);
            setInitialAnalysisReceived(true);
            
            // Set metadata
            if (session.dataSummary) {
              if (session.sampleRows && session.sampleRows.length > 0) {
                setSampleRows(session.sampleRows);
              }
              setColumns(session.dataSummary.columns?.map((c: any) => c.name) || []);
              setNumericColumns(session.dataSummary.numericColumns || []);
              setDateColumns(session.dataSummary.dateColumns || []);
              setTotalRows(session.dataSummary.rowCount);
              setTotalColumns(session.dataSummary.columnCount);
            }
            if (session.charts) setInitialCharts(session.charts);
            if (session.insights) setInitialInsights(session.insights);
            return true;
          }
        }
        
        // Create initial message from charts/insights if no messages yet
        logger.log('✅ Creating initial message from charts/insights');
        const initialMessage: Message = {
          role: 'assistant',
          content: `Hi! 👋 I've just finished analyzing your data. Here's what I found:\n\n📊 Your dataset has ${session.dataSummary?.rowCount || 0} rows and ${session.dataSummary?.columnCount || 0} columns\n\nI've created ${session.charts?.length || 0} visualizations and ${session.insights?.length || 0} key insights to get you started. Feel free to ask me anything about your data - I'm here to help! What would you like to explore first?`,
          charts: session.charts || [],
          insights: session.insights || [],
          timestamp: Date.now(),
        };
        setMessages([initialMessage]);
        setIsLargeFileLoading(false);
        setInitialAnalysisReceived(true);
        
        // Set metadata
        if (session.dataSummary) {
          if (session.sampleRows && session.sampleRows.length > 0) {
            setSampleRows(session.sampleRows);
          }
          setColumns(session.dataSummary.columns?.map((c: any) => c.name) || []);
          setNumericColumns(session.dataSummary.numericColumns || []);
          setDateColumns(session.dataSummary.dateColumns || []);
          setTotalRows(session.dataSummary.rowCount);
          setTotalColumns(session.dataSummary.columnCount);
        }
        if (session.charts) setInitialCharts(session.charts);
        if (session.insights) setInitialInsights(session.insights);
        return true;
      }
      
      // Check messages only if charts/insights aren't ready yet
      if (session && session.messages && Array.isArray(session.messages) && session.messages.length > 0) {
        logger.log(`✅ Fetched ${session.messages.length} messages from session API`);
        logger.log('📝 Messages:', session.messages.map((m: Message) => ({
          role: m.role,
          hasContent: !!m.content,
          contentPreview: m.content?.substring(0, 50),
          chartsCount: m.charts?.length || 0,
          insightsCount: m.insights?.length || 0
        })));
        
        // Check if initial analysis exists using improved detection
        const hasInitialAnalysis = session.messages.some((msg: Message) => isInitialAnalysisMessage(msg));
        
        if (hasInitialAnalysis) {
          logger.log('✅ Initial analysis found in session - setting messages and clearing loading state');
          setMessages(session.messages);
          setIsLargeFileLoading(false);
          setInitialAnalysisReceived(true);
          
          // Also set metadata if available
          if (session.dataSummary) {
            if (session.sampleRows && session.sampleRows.length > 0) {
              setSampleRows(session.sampleRows);
            }
            setColumns(session.dataSummary.columns?.map((c: any) => c.name) || []);
            setNumericColumns(session.dataSummary.numericColumns || []);
            setDateColumns(session.dataSummary.dateColumns || []);
            setTotalRows(session.dataSummary.rowCount);
            setTotalColumns(session.dataSummary.columnCount);
          }
          if (session.charts) setInitialCharts(session.charts);
          if (session.insights) setInitialInsights(session.insights);
          return true; // Success
        } else {
          logger.log('⚠️ Messages found but no initial analysis detected - waiting for charts/insights');
        }
      } else {
        logger.log('⚠️ No messages found in session yet - waiting for analysis to complete');
      }
    } catch (error) {
      logger.error('⚠️ Failed to fetch messages from session API:', error);
    }
    
    // Retry if we haven't found messages yet and haven't exceeded max retries
    // Use exponential backoff: start with 2s, increase to 5s after 10 attempts
    if (retryCount < MAX_RETRIES - 1) {
      const baseInterval = retryCount < 10 ? 2000 : 5000;
      const backoffInterval = Math.min(baseInterval * Math.pow(1.1, Math.max(0, retryCount - 10)), 10000);
      setTimeout(() => {
        fetchMessagesFromSession(retryCount + 1);
      }, backoffInterval);
    } else {
      logger.warn('⚠️ Max retries reached - giving up on fetching messages');
    }
    
    return false;
  }, [sessionId, initialAnalysisReceived, setMessages, setIsLargeFileLoading, setInitialAnalysisReceived, setSampleRows, setColumns, setNumericColumns, setDateColumns, setTotalRows, setTotalColumns, setInitialCharts, setInitialInsights]);

  // Memoize callbacks for useChatMessagesStream to prevent hook count issues
  const handleNewMessages = useCallback((newMessages: Message[]) => {
    // Simply append all new messages - no cleaning, no deduplication
    setMessages((prev) => {
      if (!newMessages || newMessages.length === 0) {
        return prev;
      }

      logger.log(`📥 SSE: Received ${newMessages.length} messages`);
      
      // Check if initial analysis arrived to clear loading state and disable SSE
      const hasInitialAnalysis = newMessages.some(msg => isInitialAnalysisMessage(msg));
      
      if (hasInitialAnalysis && !initialAnalysisReceived) {
        logger.log('✅ Initial analysis received via SSE - disabling polling');
        setIsLargeFileLoading(false);
        setInitialAnalysisReceived(true); // Disable SSE polling after initial analysis
      }
      
      // Simply append all messages - no filtering
      return [...prev, ...newMessages];
    });
  }, [initialAnalysisReceived, setMessages, setIsLargeFileLoading, setInitialAnalysisReceived]);

  const handleSSEComplete = useCallback(() => {
    // When SSE completes, fetch messages from session API as fallback
    // This ensures we get the initial analysis even if SSE didn't deliver it
    logger.log('✅ SSE complete event received - checking for initial analysis');
    
    // Start fetching with retries - will keep trying until messages are found
    setTimeout(() => {
      fetchMessagesFromSession(0);
    }, 500); // Small delay to ensure backend has saved
  }, [fetchMessagesFromSession]);

  // SSE for initial analysis only - polls for existing messages when session first loads
  // Disabled after initial analysis is received to prevent continuous polling
  // Regular chat messages come from chat mutation which saves to CosmosDB
  useChatMessagesStream({
    sessionId,
    enabled: !!sessionId && !initialAnalysisReceived, // Only enabled until initial analysis is received
    onNewMessages: handleNewMessages,
    onComplete: handleSSEComplete,
  });

  // Sync mode with initialMode prop (from URL) - only when initialMode changes
  // This handles backward compatibility when coming from old routes (/data-ops, /modeling)
  useEffect(() => {
    if (initialMode && initialMode !== mode && initialMode !== 'general') {
      // Only set mode if it's a specific mode (not 'general'), for backward compatibility
      setMode(initialMode);
    }
  }, [initialMode]); // Only depend on initialMode, not mode

  // No message cleaning - all messages are kept as-is

  // Reset state only when resetTrigger changes (upload new file)
  // Only reset if resetTrigger > 0 AND we're not loading a session
  useEffect(() => {
    if (resetTrigger > 0 && !loadedSessionData) {
      resetState();
      setSuggestions([]); // Clear suggestions when resetting
      setIsLargeFileLoading(false); // Reset large file loading state
      setInitialAnalysisReceived(false); // Reset to allow SSE polling for new session
    }
  }, [resetTrigger, resetState, loadedSessionData]);

  // Reset initialAnalysisReceived when sessionId changes (new session)
  useEffect(() => {
    if (sessionId) {
      setInitialAnalysisReceived(false); // Allow SSE polling for new session
    }
  }, [sessionId]);

  // Check if we already have initial analysis in current messages
  useEffect(() => {
    if (!isLargeFileLoading || initialAnalysisReceived) {
      return;
    }

    // Check current messages for initial analysis
    const hasInitialAnalysis = messages.some(msg => isInitialAnalysisMessage(msg));

    if (hasInitialAnalysis) {
      logger.log('✅ Initial analysis found in current messages - clearing loading state');
      setIsLargeFileLoading(false);
      setInitialAnalysisReceived(true);
    }
  }, [messages, isLargeFileLoading, initialAnalysisReceived]);

  // Poll for messages when loading state is active and we haven't received initial analysis
  // This is a fallback in case SSE doesn't deliver messages
  // Also poll for all files (not just large ones) to ensure initial analysis is received
  useEffect(() => {
    // Poll if: (1) large file loading is active, OR (2) we have a session but no messages yet
    const shouldPoll = (isLargeFileLoading || (sessionId && messages.length === 0)) && !initialAnalysisReceived;
    
    if (!shouldPoll || !sessionId) {
      return;
    }

    logger.log('🔄 Starting polling for initial analysis messages...');
    let pollCount = 0;
    const MAX_POLLS = 60; // Reduced from 500 to 60 (2 minutes max with exponential backoff)
    const INITIAL_POLL_INTERVAL = 2000; // Start with 2 seconds for first 10 attempts
    const BASE_POLL_INTERVAL = 5000; // Then switch to 5 seconds
    const EXPONENTIAL_BACKOFF_MAX = 10000; // Max 10 seconds between polls
    let isCleared = false;
    let consecutiveErrors = 0;
    let timeoutId: NodeJS.Timeout | null = null;

    const scheduleNextPoll = (attemptNumber: number) => {
      if (isCleared) return;
      
      // Calculate interval with exponential backoff
      let interval: number;
      if (attemptNumber <= 10) {
        // First 10 attempts: use initial interval (2s)
        interval = INITIAL_POLL_INTERVAL;
      } else {
        // After 10 attempts: use base interval (5s) with exponential backoff for errors
        const backoffMultiplier = Math.min(Math.pow(1.2, consecutiveErrors), 2); // Max 2x multiplier
        interval = Math.min(BASE_POLL_INTERVAL * backoffMultiplier, EXPONENTIAL_BACKOFF_MAX);
      }
      
      timeoutId = setTimeout(async () => {
        if (isCleared) return;
        
        pollCount++;
        logger.log(`🔄 Polling attempt ${pollCount}/${MAX_POLLS} (interval: ${interval}ms)...`);
        
        try {
          const data = await sessionsApi.getSessionDetails(sessionId);
          consecutiveErrors = 0; // Reset error count on success
          const session = data.session || data;
          
          // Check for charts/insights FIRST - this indicates analysis is ready
          const hasCharts = session?.charts && Array.isArray(session.charts) && session.charts.length > 0;
          const hasInsights = session?.insights && Array.isArray(session.insights) && session.insights.length > 0;
          
          if (hasCharts || hasInsights) {
            logger.log('✅ Analysis ready via polling - charts/insights found');
            
            // If we have messages with charts/insights, use them
            if (session.messages && Array.isArray(session.messages) && session.messages.length > 0) {
              const hasInitialAnalysis = session.messages.some((msg: Message) => isInitialAnalysisMessage(msg));
              if (hasInitialAnalysis) {
                logger.log('✅ Initial analysis found in messages - setting messages and clearing loading state');
                setMessages(session.messages);
                setIsLargeFileLoading(false);
                setInitialAnalysisReceived(true);
                
                // Set metadata
                if (session.dataSummary) {
                  if (session.sampleRows && session.sampleRows.length > 0) {
                    setSampleRows(session.sampleRows);
                  }
                  setColumns(session.dataSummary.columns?.map((c: any) => c.name) || []);
                  setNumericColumns(session.dataSummary.numericColumns || []);
                  setDateColumns(session.dataSummary.dateColumns || []);
                  setTotalRows(session.dataSummary.rowCount);
                  setTotalColumns(session.dataSummary.columnCount);
                }
                if (session.charts) setInitialCharts(session.charts);
                if (session.insights) setInitialInsights(session.insights);
                
                isCleared = true;
                if (timeoutId) clearTimeout(timeoutId);
                return;
              }
            }
            
            // Create initial message from charts/insights
            logger.log('✅ Creating initial message from charts/insights');
            const initialMessage: Message = {
              role: 'assistant',
              content: `Hi! 👋 I've just finished analyzing your data. Here's what I found:\n\n📊 Your dataset has ${session.dataSummary?.rowCount || 0} rows and ${session.dataSummary?.columnCount || 0} columns\n\nI've created ${session.charts?.length || 0} visualizations and ${session.insights?.length || 0} key insights to get you started. Feel free to ask me anything about your data - I'm here to help! What would you like to explore first?`,
              charts: session.charts || [],
              insights: session.insights || [],
              timestamp: Date.now(),
            };
            setMessages([initialMessage]);
            setIsLargeFileLoading(false);
            setInitialAnalysisReceived(true);
            
            // Set metadata
            if (session.dataSummary) {
              if (session.sampleRows && session.sampleRows.length > 0) {
                setSampleRows(session.sampleRows);
              }
              setColumns(session.dataSummary.columns?.map((c: any) => c.name) || []);
              setNumericColumns(session.dataSummary.numericColumns || []);
              setDateColumns(session.dataSummary.dateColumns || []);
              setTotalRows(session.dataSummary.rowCount);
              setTotalColumns(session.dataSummary.columnCount);
            }
            if (session.charts) setInitialCharts(session.charts);
            if (session.insights) setInitialInsights(session.insights);
            
            isCleared = true;
            if (timeoutId) clearTimeout(timeoutId);
            return;
          } else {
            logger.log(`⏳ Analysis not ready yet - no charts/insights (attempt ${pollCount}/${MAX_POLLS})`);
          }
        } catch (error) {
          consecutiveErrors++;
          logger.error('⚠️ Error polling for messages:', error);
          
          // If we have too many consecutive errors, show user feedback
          if (consecutiveErrors >= 5) {
            toast({
              title: 'Connection Issue',
              description: 'Having trouble connecting to the server. Retrying with longer intervals...',
              variant: 'destructive',
            });
          }
        }
        
        // Continue polling if we haven't exceeded max attempts
        if (pollCount < MAX_POLLS && !isCleared) {
          scheduleNextPoll(pollCount + 1);
        } else if (pollCount >= MAX_POLLS) {
          logger.warn('⚠️ Polling timeout - giving up on fetching initial analysis');
          toast({
            title: 'Analysis Timeout',
            description: 'The analysis is taking longer than expected. Please refresh the page or try uploading again.',
            variant: 'destructive',
          });
          setIsLargeFileLoading(false);
          isCleared = true;
        }
      }, interval);
    };

    // Start polling
    scheduleNextPoll(1);

    return () => {
      isCleared = true;
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isLargeFileLoading, sessionId, initialAnalysisReceived, messages.length]);

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
        logger.error('Failed to fetch collaborators', e);
      }
    };
    fetchCollaborators();
  }, [sessionId]);

  // Show context modal when a new session is created (after upload)
  useEffect(() => {
    if (sessionId && !contextModalShownRef.current.has(sessionId)) {
      // Check if session already has context - if so, don't show modal
      const checkAndShowModal = async () => {
        try {
          const data = await sessionsApi.getSessionDetails(sessionId);
          const sessionData = data.session || data;
          // Only show modal if there's no existing permanent context
          if (!sessionData.permanentContext) {
            setContextModalSessionId(sessionId);
            setShowContextModal(true);
            contextModalShownRef.current.add(sessionId);
          }
        } catch (e) {
          logger.error('Failed to check session context:', e);
          // Show modal anyway if we can't check
          setContextModalSessionId(sessionId);
          setShowContextModal(true);
          contextModalShownRef.current.add(sessionId);
        }
      };
      checkAndShowModal();
    }
  }, [sessionId]);

  // Handle saving context
  const handleSaveContext = async (context: string) => {
    if (!contextModalSessionId) return;
    
    setIsSavingContext(true);
    try {
      await sessionsApi.updateSessionContext(contextModalSessionId, context);
      setShowContextModal(false);
      setContextModalSessionId(null);
      toast({
        title: 'Context Saved',
        description: 'Your context has been saved and will be included with each message.',
      });
    } catch (error) {
      logger.error('Failed to save context:', error);
      toast({
        title: 'Error',
        description: 'Failed to save context. Please try again.',
        variant: 'destructive',
      });
    } finally {
      setIsSavingContext(false);
    }
  };

  // Handle closing context modal
  const handleCloseContextModal = () => {
    setShowContextModal(false);
    setContextModalSessionId(null);
  };

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
        <div className="text-center max-w-md px-6">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary mx-auto mb-4"></div>
          <h3 className="text-lg font-semibold text-gray-900 mb-2">Loading analysis</h3>
          <p className="text-sm text-gray-600 mb-4">Preparing your data and insights...</p>
          <div className="w-full bg-gray-200 rounded-full h-2">
            <div className="bg-primary h-2 rounded-full animate-pulse" style={{ width: '40%' }}></div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <>
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
        isLargeFileLoading={isLargeFileLoading}
        onModeChange={(newMode) => {
          setMode(newMode);
          // onModeChange will update the URL, which will update initialMode prop
          if (onModeChange) {
            onModeChange(newMode);
          }
        }}
        onOpenDataSummary={() => setShowDataSummaryModal(true)}
      />
      <ContextModal
        isOpen={showContextModal}
        onClose={handleCloseContextModal}
        onSave={handleSaveContext}
        isLoading={isSavingContext}
      />
      <DataSummaryModal
        isOpen={showDataSummaryModal}
        onClose={() => setShowDataSummaryModal(false)}
        sessionId={sessionId}
        onSendMessage={handleSendMessage}
      />
    </>
  );
}
