import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Message, UploadResponse, ChatResponse, ThinkingStep } from '@/shared/schema';
import { uploadFile, streamChatRequest, streamDataOpsChatRequest, DataOpsResponse } from '@/lib/api';
import { sessionsApi } from '@/lib/api/sessions';
import { useToast } from '@/hooks/use-toast';
import { getUserEmail } from '@/utils/userStorage';
import { useRef, useEffect, useState } from 'react';

interface UseHomeMutationsProps {
  sessionId: string | null;
  messages: Message[];
  mode?: 'general' | 'analysis' | 'dataOps' | 'modeling';
  setSessionId: (id: string | null) => void;
  setFileName: (fileName: string | null) => void;
  setInitialCharts: (charts: UploadResponse['charts']) => void;
  setInitialInsights: (insights: UploadResponse['insights']) => void;
  setSampleRows: (rows: Record<string, any>[]) => void;
  setColumns: (columns: string[]) => void;
  setNumericColumns: (columns: string[]) => void;
  setDateColumns: (columns: string[]) => void;
  setTotalRows: (rows: number) => void;
  setTotalColumns: (columns: number) => void;
  setMessages: (messages: Message[] | ((prev: Message[]) => Message[])) => void;
  setSuggestions?: (suggestions: string[]) => void;
  setIsLargeFileLoading?: (isLoading: boolean) => void;
}

export const useHomeMutations = ({
  sessionId,
  messages,
  mode = 'general',
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
}: UseHomeMutationsProps) => {
  const { toast } = useToast();
  const queryClient = useQueryClient();
  const userEmail = getUserEmail();
  const abortControllerRef = useRef<AbortController | null>(null);
  const pendingUserMessageRef = useRef<{ content: string; timestamp: number } | null>(null);
  const messagesRef = useRef<Message[]>(messages);
  const [thinkingSteps, setThinkingSteps] = useState<ThinkingStep[]>([]);
  const [thinkingTargetTimestamp, setThinkingTargetTimestamp] = useState<number | null>(null);
  
  // Keep messagesRef in sync with messages
  useEffect(() => {
    messagesRef.current = messages;
  }, [messages]);

  // Don't sanitize markdown - we'll render it properly in MessageBubble
  // This preserves formatting like **bold** for headings

  const uploadMutation = useMutation({
    mutationFn: async ({ file, fileSize }: { file: File; fileSize: number }) => {
      // Store fileSize in a way we can access it in onSuccess
      (file as any)._fileSize = fileSize;
      return await uploadFile<any>('/api/upload', file);
    },
    onSuccess: async (data, variables) => {
      console.log("upload response from the backend", data);
      
      // Handle new async format (202 response with jobId and sessionId)
      if (data.jobId && data.sessionId && data.status === 'processing') {
        setSessionId(data.sessionId);
        
        // Check if file is large (>= 50MB) and show loading state instead of message
        const LARGE_FILE_THRESHOLD = 50 * 1024 * 1024; // 50MB
        const isLargeFile = variables.fileSize >= LARGE_FILE_THRESHOLD;
        
        if (isLargeFile && setIsLargeFileLoading) {
          // Show loading state for large files instead of processing message
          setIsLargeFileLoading(true);
          setMessages([]); // Clear messages to show loading state
        } else {
          // For small files, don't show any message - just wait for SSE
          setMessages([]);
        }
        
        // Fetch session details from placeholder (now it exists!)
        // We only fetch to set metadata, NOT to show the initial message
        // The initial message will come from SSE when processing completes
        try {
          const sessionData = await sessionsApi.getSessionDetails(data.sessionId);
          const session = sessionData.session || sessionData;
          
          if (session) {
            setFileName(session.fileName || null);
            setInitialCharts(session.charts || []);
            setInitialInsights(session.insights || []);
            
            // Only set metadata if full data is available
            if (session.dataSummary && session.dataSummary.rowCount > 0) {
              if (session.sampleRows && session.sampleRows.length > 0) {
                setSampleRows(session.sampleRows);
              }
              setColumns(session.dataSummary.columns?.map((c: any) => c.name) || []);
              setNumericColumns(session.dataSummary.numericColumns || []);
              setDateColumns(session.dataSummary.dateColumns || []);
              setTotalRows(session.dataSummary.rowCount);
              setTotalColumns(session.dataSummary.columnCount);
            }
            // Don't set the initial message here - let SSE handle it to avoid duplicates
          }
        } catch (sessionError) {
          console.error('Failed to fetch session details:', sessionError);
          // Processing message is already shown above, so user still sees feedback
          // The SSE stream will pick up the final message when processing completes
        }
        
        toast({
          title: 'Upload Accepted',
          description: 'Your file is being processed. Analysis will be available shortly.',
        });
      } 
      // Handle old synchronous format (backward compatibility)
      else if (data.sessionId && data.summary) {
        setSessionId(data.sessionId);
        setFileName(data.fileName || null);
        setInitialCharts(data.charts || []);
        setInitialInsights(data.insights || []);
        
        if (data.sampleRows && data.sampleRows.length > 0) {
          setSampleRows(data.sampleRows);
          setColumns(data.summary.columns.map((c: any) => c.name));
          setNumericColumns(data.summary.numericColumns);
          setDateColumns(data.summary.dateColumns);
          setTotalRows(data.summary.rowCount);
          setTotalColumns(data.summary.columnCount);
        }
        
        const initialMessage: Message = {
          role: 'assistant',
          content: `Hi! 👋 I've just finished analyzing your data. Here's what I found:\n\n📊 Your dataset has ${data.summary.rowCount} rows and ${data.summary.columnCount} columns\n🔢 ${data.summary.numericColumns.length} numeric columns to work with\n📅 ${data.summary.dateColumns.length} date columns for time-based analysis\n\nI've created ${(data.charts || []).length} visualizations and ${(data.insights || []).length} key insights to get you started. Feel free to ask me anything about your data - I'm here to help! What would you like to explore first?`,
          charts: data.charts || [],
          insights: data.insights || [],
          timestamp: Date.now(),
        };
        setMessages([initialMessage]);
        
        if (data.suggestions && data.suggestions.length > 0 && setSuggestions) {
          setSuggestions(data.suggestions);
        }
        
        toast({
          title: 'Analysis Complete',
          description: 'Your data has been analyzed successfully!',
        });
      }
      
      // Invalidate sessions query to refresh the analysis list
      if (userEmail) {
        queryClient.invalidateQueries({ queryKey: ['sessions', userEmail] });
        console.log('🔄 Invalidated sessions query for user:', userEmail);
      }
    },
    onError: (error) => {
      // Clear large file loading state on error
      if (setIsLargeFileLoading) {
        setIsLargeFileLoading(false);
      }
      toast({
        title: 'Upload Failed',
        description: error instanceof Error ? error.message : 'Failed to upload file',
        variant: 'destructive',
      });
    },
  });

  const chatMutation = useMutation({
    mutationFn: async ({ message, targetTimestamp }: { message: string; targetTimestamp?: number }): Promise<ChatResponse> => {
      // Cancel previous request if any
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
        // Remove the previous pending user message if it exists
        if (pendingUserMessageRef.current) {
          setMessages((prev) => {
            const updated = [...prev];
            const indexToRemove = updated.findIndex(
              m => m.role === 'user' && 
              m.content === pendingUserMessageRef.current!.content &&
              m.timestamp === pendingUserMessageRef.current!.timestamp
            );
            if (indexToRemove >= 0) {
              updated.splice(indexToRemove, 1);
            }
            return updated;
          });
          pendingUserMessageRef.current = null;
        }
      }
      
      // Create new abort controller
      abortControllerRef.current = new AbortController();
      
      // Track the current pending message - use the targetTimestamp if provided (from handleSendMessage)
      // or find the matching message in state by content
      const currentTimestamp = targetTimestamp || Date.now();
      // Find the actual message in state to get the exact timestamp
      const actualMessage = messagesRef.current
        .slice()
        .reverse()
        .find(m => m.role === 'user' && m.content === message);
      
      pendingUserMessageRef.current = { 
        content: message, 
        timestamp: actualMessage?.timestamp || currentTimestamp 
      };
      
      console.log('📌 Tracking pending message:', pendingUserMessageRef.current);
      
      // Clear previous thinking steps
      setThinkingSteps([]);
      setThinkingTargetTimestamp(null);
      
      console.log('📤 Sending chat message:', message);
      console.log('📋 SessionId:', sessionId);
      
      if (!sessionId) {
        throw new Error('Session ID is required');
      }
      
      // Use ref to get latest messages (important for edit functionality)
      const currentMessages = messagesRef.current;
      console.log('💬 Chat history length:', currentMessages.length);
      
      const lastUserMessage = targetTimestamp
        ? { timestamp: targetTimestamp }
        : [...currentMessages].reverse().find(msg => msg.role === 'user');
      if (lastUserMessage) {
        setThinkingTargetTimestamp(lastUserMessage.timestamp);
      } else {
        setThinkingTargetTimestamp(null);
      }
      
      // Send full chat history for context (last 15 messages to maintain conversation flow)
      // Truncate long messages to reduce token usage
      const chatHistory = currentMessages.slice(-15).map(msg => ({
        role: msg.role,
        content: msg.content.length > 500 
          ? msg.content.substring(0, 500) + '...' 
          : msg.content,
      }));
      
      console.log('📤 Request payload:', {
        sessionId,
        message,
        chatHistoryLength: chatHistory.length,
      });
      
      // Route to Data Ops, Modeling, or regular chat based on mode
      if (mode === 'dataOps') {
        return new Promise<ChatResponse>((resolve, reject) => {
          let responseData: DataOpsResponse | null = null;
          
          streamDataOpsChatRequest(
            sessionId,
            message,
            chatHistory,
            {
              onThinkingStep: (step: ThinkingStep) => {
                console.log('🧠 Data Ops thinking step received:', step);
                setThinkingSteps((prev) => {
                  const existingIndex = prev.findIndex(s => s.step === step.step);
                  if (existingIndex >= 0) {
                    const updated = [...prev];
                    updated[existingIndex] = step;
                    return updated;
                  }
                  return [...prev, step];
                });
              },
              onResponse: (response: DataOpsResponse) => {
                console.log('✅ Data Ops API response received:', response);
                responseData = response;
                // Store preview/summary in a way that can be accessed by MessageBubble
                // We'll add these as custom properties to the response
                (responseData as any).preview = response.preview;
                (responseData as any).summary = response.summary;
              },
              onError: (error: Error) => {
                console.error('❌ Data Ops API request failed:', error);
                setThinkingSteps([]);
                setThinkingTargetTimestamp(null);
                reject(error);
              },
              onDone: () => {
                console.log('✅ Data Ops stream completed');
                setThinkingSteps([]);
                setThinkingTargetTimestamp(null);
                if (responseData) {
                  // Convert DataOpsResponse to ChatResponse format
                  const chatResponse: ChatResponse & { preview?: any[]; summary?: any[] } = {
                    answer: responseData.answer,
                    charts: [],
                    insights: [],
                    suggestions: [],
                    preview: responseData.preview,
                    summary: responseData.summary,
                  };
                  resolve(chatResponse as ChatResponse);
                } else {
                  reject(new Error('No response received'));
                }
              },
            },
            abortControllerRef.current.signal,
            targetTimestamp,
            true // dataOpsMode flag for backward compatibility
          ).catch((error: any) => {
            if (error?.name === 'AbortError' || abortControllerRef.current?.signal.aborted) {
              console.log('🚫 Data Ops request was cancelled by user');
              setThinkingSteps([]);
              setThinkingTargetTimestamp(null);
              reject(new Error('Request cancelled'));
            } else {
              setThinkingSteps([]);
              setThinkingTargetTimestamp(null);
              reject(error);
            }
          });
        });
      } else {
        // For 'general', 'analysis', and 'modeling' modes, use regular chat endpoint
        // Only send mode parameter if it's explicitly set (not 'general')
        // For 'general' (auto-detect), don't send mode to let backend auto-detect
        const modeToSend = mode === 'general' ? undefined : (mode === 'modeling' || mode === 'analysis' ? mode : undefined);
        
        return new Promise<ChatResponse>((resolve, reject) => {
        let responseData: ChatResponse | null = null;
        
        streamChatRequest(
          sessionId,
          message,
          chatHistory,
          {
            onThinkingStep: (step: ThinkingStep) => {
              console.log('🧠 Thinking step received:', step);
              setThinkingSteps((prev) => {
                const existingIndex = prev.findIndex(s => s.step === step.step);
                if (existingIndex >= 0) {
                  const updated = [...prev];
                  updated[existingIndex] = step;
                  console.log('🔄 Updated thinking steps:', updated);
                  return updated;
                }
                const newSteps = [...prev, step];
                console.log('➕ Added thinking step. Total steps:', newSteps.length);
                return newSteps;
              });
            },
            onResponse: (response: ChatResponse) => {
              console.log('✅ API response received:', response);
              responseData = response;
            },
            onError: (error: Error) => {
              console.error('❌ API request failed:', error);
              setThinkingSteps([]);
              setThinkingTargetTimestamp(null);
              reject(error);
            },
            onDone: () => {
              console.log('✅ Stream completed');
              setThinkingSteps([]);
              setThinkingTargetTimestamp(null);
              if (responseData) {
                resolve(responseData);
              } else {
                reject(new Error('No response received'));
              }
            },
          },
          abortControllerRef.current.signal,
          targetTimestamp,
          modeToSend
        ).catch((error: any) => {
            if (error?.name === 'AbortError' || abortControllerRef.current?.signal.aborted) {
            console.log('🚫 Request was cancelled by user');
              setThinkingSteps([]);
              setThinkingTargetTimestamp(null);
            reject(new Error('Request cancelled'));
          } else {
            setThinkingSteps([]);
              setThinkingTargetTimestamp(null);
            reject(error);
          }
        });
      });
      }
    },
    onSuccess: (data, variables) => {
      console.log('✅ Chat response received:', data);
      console.log('📝 Answer:', data.answer);
      console.log('📊 Charts:', data.charts?.length || 0);
      console.log('💡 Insights:', data.insights?.length || 0);
      console.log('💬 Suggestions:', data.suggestions?.length || 0);
      
      // Clear pending message ref since request completed successfully
      pendingUserMessageRef.current = null;
      
      if (!data.answer || data.answer.trim().length === 0) {
        console.error('❌ Empty answer received from server!');
        toast({
          title: 'Error',
          description: 'Received empty response from server. Please try again.',
          variant: 'destructive',
        });
        return;
      }
      
      const assistantMessage: Message & { preview?: any[]; summary?: any[] } = {
        role: 'assistant',
        content: data.answer, // Keep markdown formatting for proper rendering
        charts: data.charts,
        insights: data.insights,
        timestamp: Date.now(),
        preview: (data as any).preview,
        summary: (data as any).summary,
      };
      
      console.log('💬 Adding assistant message to chat:', assistantMessage.content.substring(0, 50));
      console.log('📊 Message includes:', {
        hasCharts: !!assistantMessage.charts?.length,
        hasInsights: !!assistantMessage.insights?.length,
        contentLength: assistantMessage.content?.length || 0
      });
      
      setMessages((prev) => {
        // Simply append the message - no deduplication, no cleaning
        // Messages are already saved to CosmosDB by the backend during processing
        const updated = [...prev, assistantMessage];
        console.log('📋 Total messages now:', updated.length);
        return updated;
      });

      // Update suggestions if provided
      if (data.suggestions && setSuggestions) {
        setSuggestions(data.suggestions);
      }
    },
    onError: (error, variables) => {
      // Clear pending message ref
      const pendingMessage = pendingUserMessageRef.current;
      pendingUserMessageRef.current = null;
      
      // Don't show toast for cancelled requests
      if (error instanceof Error && error.message === 'Request cancelled') {
        // Remove the user message that was added when the request was sent
        // since the request was cancelled and won't be saved
        if (pendingMessage) {
          setMessages((prev) => {
            const updated = [...prev];
            // Find and remove the user message that matches the cancelled request
            const indexToRemove = updated.findIndex(
              m => m.role === 'user' && 
              m.content === pendingMessage.content &&
              m.timestamp === pendingMessage.timestamp
            );
            if (indexToRemove >= 0) {
              updated.splice(indexToRemove, 1);
            }
            return updated;
          });
        }
        return;
      }
      toast({
        title: 'Error',
        description: error instanceof Error ? error.message : 'Failed to send message',
        variant: 'destructive',
      });
    },
  });

  // Function to cancel ongoing chat request
  const cancelChatRequest = () => {
    if (abortControllerRef.current) {
      // Get the pending message before aborting
      const pendingMessage = pendingUserMessageRef.current;
      
      abortControllerRef.current.abort();
      abortControllerRef.current = null;
      setThinkingSteps([]);
      setThinkingTargetTimestamp(null);
      
      // Remove the user message that was added when the request was sent
      // since the request was cancelled and won't be saved
      setMessages((prev) => {
        const updated = [...prev];
        
        if (pendingMessage) {
          // Try to find by exact match (content + timestamp)
          let indexToRemove = updated.findIndex(
            m => m.role === 'user' && 
            m.content === pendingMessage.content &&
            m.timestamp === pendingMessage.timestamp
          );
          
          // If not found by exact match, try to find by content only (in case timestamp doesn't match)
          if (indexToRemove < 0) {
            // Find the last user message that matches the content
            for (let i = updated.length - 1; i >= 0; i--) {
              if (updated[i].role === 'user' && updated[i].content === pendingMessage.content) {
                indexToRemove = i;
                break;
              }
            }
          }
          
          if (indexToRemove >= 0) {
            updated.splice(indexToRemove, 1);
            console.log('🗑️ Removed cancelled user message:', pendingMessage.content);
          }
        } else {
          // If no pending message tracked, remove the last user message (most recent)
          // This handles the case where stop is clicked very quickly before tracking is set
          for (let i = updated.length - 1; i >= 0; i--) {
            if (updated[i].role === 'user') {
              // Only remove if there's no assistant response after it
              if (i === updated.length - 1 || updated[i + 1].role !== 'assistant') {
                console.log('🗑️ Removed last user message (no response yet):', updated[i].content);
                updated.splice(i, 1);
              }
              break;
            }
          }
        }
        
        return updated;
      });
      
      pendingUserMessageRef.current = null;
      
      // Reset mutation state to clear loading state
      chatMutation.reset();
    }
  };

  return {
    uploadMutation,
    chatMutation,
    cancelChatRequest,
    thinkingSteps, // Export thinking steps for display
    thinkingTargetTimestamp,
  };
};
