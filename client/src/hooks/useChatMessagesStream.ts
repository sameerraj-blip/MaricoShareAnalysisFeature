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
  const enabledRef = useRef(enabled);
  const maxReconnectAttempts = 5;
  
  // Keep enabled ref in sync
  enabledRef.current = enabled;

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

      // Track if connection is closed to prevent processing events after closure
      let connectionClosed = false;

      eventSource.onopen = () => {
        reconnectAttemptsRef.current = 0;
        connectionClosed = false;
      };

      // Track if we've received init to prevent duplicate processing
      let initReceived = false;
      
      eventSource.addEventListener('init', (event) => {
        // Ignore if connection already closed
        if (connectionClosed) {
          return;
        }

        try {
          const data = JSON.parse(event.data);
          // Initial load - send initial messages if available
          // This helps sync messages when SSE first connects
          // Only process init once to avoid duplicates
          if (!initReceived && data.messages && Array.isArray(data.messages)) {
            initReceived = true;
            
            // Process messages if available
            if (data.messages.length > 0) {
              onNewMessages(data.messages);
            }
            
            // CRITICAL: Close connection immediately after receiving init event
            // The backend sends init once for initial analysis and then closes the connection
            // We must close on client side immediately to prevent receiving duplicate events
            // This breaks the SSE connection as soon as we get the initial response
            console.log('✅ Initial response received via init event - closing SSE connection immediately');
            connectionClosed = true;
            if (eventSourceRef.current) {
              eventSourceRef.current.close();
              eventSourceRef.current = null;
            }
          }
        } catch (err) {
          console.error('Failed to parse SSE init data:', err);
        }
      });

      eventSource.addEventListener('messages', (event) => {
        // Ignore if connection already closed
        if (connectionClosed) {
          return;
        }

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

      // Handle 'complete' event - connection should already be closed, but close it if not
      eventSource.addEventListener('complete', () => {
        // Ignore if connection already closed
        if (connectionClosed) {
          return;
        }

        console.log('✅ Initial analysis complete event received - closing SSE connection');
        connectionClosed = true;
        if (eventSourceRef.current) {
          eventSourceRef.current.close();
          eventSourceRef.current = null;
        }
      });

      eventSource.addEventListener('error', (event) => {
        console.error('SSE error event:', event);
      });

      eventSource.onerror = (error) => {
        // Don't reconnect if the connection is disabled
        if (!enabledRef.current) {
          eventSource.close();
          eventSourceRef.current = null;
          return;
        }
        
        eventSource.close();
        eventSourceRef.current = null;

        // Attempt to reconnect with exponential backoff (only if still enabled)
        if (enabledRef.current && reconnectAttemptsRef.current < maxReconnectAttempts) {
          const delay = Math.min(1000 * Math.pow(2, reconnectAttemptsRef.current), 30000);
          reconnectAttemptsRef.current++;
          
          reconnectTimeoutRef.current = setTimeout(() => {
            // Check again if still enabled before reconnecting
            if (enabledRef.current) {
              connectSSE();
            }
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

