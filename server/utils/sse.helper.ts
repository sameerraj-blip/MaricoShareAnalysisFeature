/**
 * SSE (Server-Sent Events) Helper
 * Utility functions for handling SSE connections
 */
import { Response } from "express";

/**
 * Send SSE event to client
 * Safely handles client disconnections
 */
const ENABLE_SSE_LOGGING = process.env.ENABLE_SSE_LOGGING === 'true';

export function sendSSE(res: Response, event: string, data: any): boolean {
  // Check if connection is still writable
  if (res.writableEnded || res.destroyed || !res.writable) {
    return false;
  }

  try {
    const message = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
    res.write(message);
    // Force flush the response (if supported by the platform)
    if (typeof (res as any).flush === 'function') {
      (res as any).flush();
    }
    // Only log in development or when explicitly enabled
    if (ENABLE_SSE_LOGGING || process.env.NODE_ENV === 'development') {
      console.log(`📤 SSE sent: ${event}`, data);
    }
    return true;
  } catch (error: any) {
    // Ignore errors from client disconnections (ECONNRESET, EPIPE are expected)
    if (error.code === 'ECONNRESET' || error.code === 'EPIPE' || error.code === 'ECONNABORTED') {
      // Client disconnected - this is normal, don't log as error
      return false;
    }
    // Log unexpected errors
    console.error('Error sending SSE event:', error);
    return false;
  }
}

/**
 * Set SSE headers for response
 */
export function setSSEHeaders(res: Response): void {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no'); // Disable buffering for nginx
}

