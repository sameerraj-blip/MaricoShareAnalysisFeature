import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Message, UploadResponse, ChatResponse, ThinkingStep } from '@/shared/schema';
import { uploadFile, streamChatRequest, streamDataOpsChatRequest, DataOpsResponse } from '@/lib/api';
import { useToast } from '@/hooks/use-toast';
import { getUserEmail } from '@/utils/userStorage';
import { useRef, useEffect, useState } from 'react';

interface UseHomeMutationsProps {
  sessionId: string | null;
  messages: Message[];
  mode?: 'analysis' | 'dataOps' | 'modeling';
  setSessionId: (id: string | null) => void;
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
}

export const useHomeMutations = ({
  sessionId,
  messages,
  mode = 'analysis',
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

  const sanitizeMarkdown = (text: string) =>
    text
      .replace(/\*\*(.*?)\*\*/g, '$1')
      .replace(/\*(.*?)\*/g, '$1')
      .replace(/__(.*?)__/g, '$1')
      .replace(/_(.*?)_/g, '$1');

  const uploadMutation = useMutation({
    mutationFn: async (file: File) => {
      return await uploadFile<UploadResponse>('/api/upload', file);
    },
    onSuccess: (data) => {
      console.log("upload chart data from the backend",data)
      setSessionId(data.sessionId);
      setInitialCharts(data.charts);
      setInitialInsights(data.insights);
      
      // Store sample rows and columns for data preview
      if (data.sampleRows && data.sampleRows.length > 0) {
        setSampleRows(data.sampleRows);
        setColumns(data.summary.columns.map(c => c.name));
        setNumericColumns(data.summary.numericColumns);
        setDateColumns(data.summary.dateColumns);
        setTotalRows(data.summary.rowCount);
        setTotalColumns(data.summary.columnCount);
      }
      
      // Create initial assistant message with charts and insights - more conversational
      const initialMessage: Message = {
        role: 'assistant',
        content: `Hi! 👋 I've just finished analyzing your data. Here's what I found:\n\n📊 Your dataset has ${data.summary.rowCount} rows and ${data.summary.columnCount} columns\n🔢 ${data.summary.numericColumns.length} numeric columns to work with\n📅 ${data.summary.dateColumns.length} date columns for time-based analysis\n\nI've created ${data.charts.length} visualizations and ${data.insights.length} key insights to get you started. Feel free to ask me anything about your data - I'm here to help! What would you like to explore first?`,
        charts: data.charts,
        insights: data.insights,
        timestamp: Date.now(),
      };
      
      setMessages([initialMessage]);
      
      // Invalidate sessions query to refresh the analysis list
      if (userEmail) {
        queryClient.invalidateQueries({ queryKey: ['sessions', userEmail] });
        console.log('🔄 Invalidated sessions query for user:', userEmail);
      }
      
      toast({
        title: 'Analysis Complete',
        description: 'Your data has been analyzed successfully!',
      });
    },
    onError: (error) => {
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
      const chatHistory = currentMessages.slice(-15).map(msg => ({
        role: msg.role,
        content: msg.content,
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
        // For 'analysis' and 'modeling' modes, use regular chat
        // TODO: Add separate modeling endpoint if needed
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
          targetTimestamp
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
        content: sanitizeMarkdown(data.answer),
        charts: data.charts,
        insights: data.insights,
        timestamp: Date.now(),
        preview: (data as any).preview,
        summary: (data as any).summary,
      };
      
      console.log('💬 Adding assistant message to chat:', assistantMessage.content.substring(0, 50));
      setMessages((prev) => {
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
