import { useEffect, useRef } from 'react';
import { Message } from '@/shared/schema';
import { getUserEmail } from '@/utils/userStorage';
import { logger } from '@/lib/logger';

interface UseChatMessagesStreamProps {
  sessionId: string | null;
  onNewMessages: (messages: Message[]) => void;
  enabled?: boolean;
  onComplete?: () => void;
}

export const useChatMessagesStream = ({
  sessionId,
  onNewMessages,
  enabled = true,
  onComplete,
}: UseChatMessagesStreamProps) => {
  const eventSourceRef = useRef<EventSource | null>(null);
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const reconnectAttemptsRef = useRef(0);
  const maxReconnectAttempts = 5;

  useEffect(() => {
    if (!sessionId || !enabled) {
      // Close existing connection if disabled
      if (eventSourceRef.current) {
        logger.log('🚫 SSE stream disabled - closing connection');
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

      eventSource.addEventListener('init', (event) => {
        try {
          const data = JSON.parse(event.data);
          // Initial load - send initial messages if available
          if (data.messages && Array.isArray(data.messages) && data.messages.length > 0) {
            logger.log(`📥 SSE init: Received ${data.messages.length} initial messages`);
            onNewMessages(data.messages);
          }
        } catch (err) {
          logger.error('Failed to parse SSE init data:', err);
        }
      });

      eventSource.addEventListener('messages', (event) => {
        try {
          const data = JSON.parse(event.data);
          if (data.messages && Array.isArray(data.messages)) {
            logger.log(`📥 SSE messages: Received ${data.messages.length} new messages`);
            onNewMessages(data.messages);
          }
          reconnectAttemptsRef.current = 0;
        } catch (err) {
          logger.error('Failed to parse SSE messages data:', err);
        }
      });

      // Handle 'done' event - close connection after receiving one response
      eventSource.addEventListener('done', () => {
        logger.log('✅ Chat response complete - closing SSE connection');
        if (eventSourceRef.current) {
          eventSourceRef.current.close();
          eventSourceRef.current = null;
        }
      });

      // Handle 'complete' event - close connection after initial analysis
      eventSource.addEventListener('complete', () => {
        logger.log('✅ Initial analysis complete - closing SSE connection');
        if (eventSourceRef.current) {
          eventSourceRef.current.close();
          eventSourceRef.current = null;
        }
        // Call onComplete callback if provided
        if (onComplete) {
          onComplete();
        }
      });

      eventSource.addEventListener('error', (event) => {
        logger.error('SSE error event:', event);
      });

      eventSource.onerror = (error) => {
        // Check if connection was closed normally (readyState 2 = CLOSED)
        if (eventSource.readyState === EventSource.CLOSED) {
          logger.log('✅ SSE connection closed normally');
          eventSourceRef.current = null;
          return;
        }
        
        logger.warn('⚠️ SSE connection error, readyState:', eventSource.readyState);
        eventSource.close();
        eventSourceRef.current = null;

        // Only attempt to reconnect if enabled and we haven't exceeded max attempts
        // Don't reconnect if connection was closed normally (after initial analysis)
        if (enabled && reconnectAttemptsRef.current < maxReconnectAttempts) {
          const delay = Math.min(1000 * Math.pow(2, reconnectAttemptsRef.current), 30000);
          reconnectAttemptsRef.current++;
          logger.log(`🔄 SSE: Attempting reconnect ${reconnectAttemptsRef.current}/${maxReconnectAttempts} in ${delay}ms`);
          
          reconnectTimeoutRef.current = setTimeout(() => {
            if (enabled) { // Check again before reconnecting
              connectSSE();
            }
          }, delay);
        } else {
          logger.log('🚫 SSE: Not reconnecting (disabled or max attempts reached)');
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
  }, [sessionId, enabled, onNewMessages, onComplete]);
};

