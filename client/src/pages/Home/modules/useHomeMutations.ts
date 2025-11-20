import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Message, UploadResponse, ChatResponse, ThinkingStep } from '@shared/schema';
import { uploadFile, streamChatRequest } from '@/lib/api';
import { useToast } from '@/hooks/use-toast';
import { getUserEmail } from '@/utils/userStorage';
import { useRef, useEffect, useState } from 'react';

interface UseHomeMutationsProps {
  sessionId: string | null;
  messages: Message[];
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
      }
      
      // Create new abort controller
      abortControllerRef.current = new AbortController();
      
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
                // Update or add the step
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
          abortControllerRef.current.signal
        ).catch((error: any) => {
          // Check if request was cancelled
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
    },
    onSuccess: (data, message) => {
      console.log('✅ Chat response received:', data);
      console.log('📝 Answer:', data.answer);
      console.log('📊 Charts:', data.charts?.length || 0);
      console.log('💡 Insights:', data.insights?.length || 0);
      console.log('💬 Suggestions:', data.suggestions?.length || 0);
      
      if (!data.answer || data.answer.trim().length === 0) {
        console.error('❌ Empty answer received from server!');
        toast({
          title: 'Error',
          description: 'Received empty response from server. Please try again.',
          variant: 'destructive',
        });
        return;
      }
      
      const assistantMessage: Message = {
        role: 'assistant',
        content: sanitizeMarkdown(data.answer),
        charts: data.charts,
        insights: data.insights,
        timestamp: Date.now(),
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
    onError: (error) => {
      // Don't show toast for cancelled requests
      if (error instanceof Error && error.message === 'Request cancelled') {
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
      abortControllerRef.current.abort();
      abortControllerRef.current = null;
      setThinkingSteps([]);
      setThinkingTargetTimestamp(null);
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
