import { useEffect, useRef } from 'react';
import { Message } from '@/shared/schema';
import { getUserEmail } from '@/utils/userStorage';

interface UseChatMessagesStreamProps {
  sessionId: string | null;
  onNewMessages: (messages: Message[]) => void;
  enabled?: boolean;
}

export const useChatMessagesStream = ({
  sessionId,
  onNewMessages,
  enabled = true,
}: UseChatMessagesStreamProps) => {
  const eventSourceRef = useRef<EventSource | null>(null);
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const reconnectAttemptsRef = useRef(0);
  const maxReconnectAttempts = 5;

  useEffect(() => {
    if (!sessionId || !enabled) {
      // Close existing connection if disabled
      if (eventSourceRef.current) {
        console.log('🚫 SSE stream disabled - closing connection');
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
      return;
    }

    const userEmail = getUserEmail();
    if (!userEmail) {
      return;
    }

    // Get API base URL
    const API_BASE_URL = import.meta.env.VITE_API_URL || 
      (import.meta.env.PROD 
        ? (typeof window !== 'undefined' ? window.location.origin : 'https://marico-insight-safe.vercel.app')
        : 'http://localhost:3002');

    const connectSSE = () => {
      // Close existing connection if any
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }

      // Build SSE URL
      const sseUrl = `${API_BASE_URL}/api/chat/${sessionId}/stream?username=${encodeURIComponent(userEmail)}`;
      
      const eventSource = new EventSource(sseUrl);
      eventSourceRef.current = eventSource;

      eventSource.onopen = () => {
        reconnectAttemptsRef.current = 0;
      };

      // Track if we've received init to prevent duplicate processing
      let initReceived = false;
      
      eventSource.addEventListener('init', (event) => {
        try {
          const data = JSON.parse(event.data);
          // Initial load - send initial messages if available
          // This helps sync messages when SSE first connects
          // Only process init once to avoid duplicates
          if (!initReceived && data.messages && Array.isArray(data.messages) && data.messages.length > 0) {
            initReceived = true;
            onNewMessages(data.messages);
            
            // Check if this is initial analysis - if so, close connection immediately
            const hasInitialAnalysis = data.messages.some((msg: any) => 
              msg.role === 'assistant' && 
              msg.content && 
              (msg.content.toLowerCase().includes('initial analysis for') || 
               msg.charts?.length > 0 || 
               msg.insights?.length > 0)
            );
            
            if (hasInitialAnalysis) {
              console.log('✅ Initial analysis received - closing SSE connection immediately');
              // Close connection immediately
              if (eventSourceRef.current) {
                eventSourceRef.current.close();
                eventSourceRef.current = null;
              }
            }
          }
        } catch (err) {
          console.error('Failed to parse SSE init data:', err);
        }
      });

      eventSource.addEventListener('messages', (event) => {
        try {
          const data = JSON.parse(event.data);
          if (data.messages && Array.isArray(data.messages)) {
            onNewMessages(data.messages);
          }
          reconnectAttemptsRef.current = 0;
        } catch (err) {
          console.error('Failed to parse SSE messages data:', err);
        }
      });

      // Handle 'complete' event - stop listening after initial analysis
      eventSource.addEventListener('complete', () => {
        console.log('✅ Initial analysis complete event received - closing SSE connection');
        if (eventSourceRef.current) {
          eventSourceRef.current.close();
          eventSourceRef.current = null;
        }
      });

      eventSource.addEventListener('error', (event) => {
        console.error('SSE error event:', event);
      });

      eventSource.onerror = (error) => {
        eventSource.close();
        eventSourceRef.current = null;

        // Attempt to reconnect with exponential backoff
        if (reconnectAttemptsRef.current < maxReconnectAttempts) {
          const delay = Math.min(1000 * Math.pow(2, reconnectAttemptsRef.current), 30000);
          reconnectAttemptsRef.current++;
          
          reconnectTimeoutRef.current = setTimeout(() => {
            connectSSE();
          }, delay);
        }
      };
    };

    // Initial connection
    connectSSE();

    // Cleanup function
    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
    };
  }, [sessionId, enabled, onNewMessages]);
};

