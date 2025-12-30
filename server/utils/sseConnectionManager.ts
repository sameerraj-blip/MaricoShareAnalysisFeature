/**
 * SSE Connection Manager
 * Manages active SSE connections to prevent duplicate queries and optimize resource usage
 */

interface SSEConnection {
  res: Response;
  userEmail: string;
  type: 'sharedAnalyses' | 'sharedDashboards' | 'chatMessages';
  lastUpdate: number;
  intervalId: NodeJS.Timeout;
}

class SSEConnectionManager {
  private connections: Map<string, SSEConnection[]> = new Map();
  private sharedQueries: Map<string, { data: any; timestamp: number }> = new Map();
  private readonly QUERY_CACHE_TTL = 2000; // 2 seconds

  /**
   * Register a new SSE connection
   */
  register(
    connectionId: string,
    res: Response,
    userEmail: string,
    type: 'sharedAnalyses' | 'sharedDashboards' | 'chatMessages',
    intervalId: NodeJS.Timeout
  ): void {
    if (!this.connections.has(connectionId)) {
      this.connections.set(connectionId, []);
    }
    
    this.connections.get(connectionId)!.push({
      res,
      userEmail,
      type,
      lastUpdate: Date.now(),
      intervalId,
    });
  }

  /**
   * Unregister a connection
   */
  unregister(connectionId: string): void {
    const conns = this.connections.get(connectionId);
    if (conns) {
      conns.forEach(conn => {
        clearInterval(conn.intervalId);
      });
      this.connections.delete(connectionId);
    }
  }

  /**
   * Get cached query result or null
   */
  getCachedQuery(key: string): any | null {
    const cached = this.sharedQueries.get(key);
    if (!cached) return null;
    
    if (Date.now() - cached.timestamp > this.QUERY_CACHE_TTL) {
      this.sharedQueries.delete(key);
      return null;
    }
    
    return cached.data;
  }

  /**
   * Cache a query result
   */
  cacheQuery(key: string, data: any): void {
    this.sharedQueries.set(key, {
      data,
      timestamp: Date.now(),
    });
  }

  /**
   * Get active connection count
   */
  getConnectionCount(): number {
    let count = 0;
    for (const conns of this.connections.values()) {
      count += conns.length;
    }
    return count;
  }

  /**
   * Clean up stale connections
   */
  cleanup(): void {
    const now = Date.now();
    const STALE_THRESHOLD = 60000; // 1 minute

    for (const [id, conns] of this.connections.entries()) {
      const active = conns.filter(conn => {
        if (now - conn.lastUpdate > STALE_THRESHOLD) {
          clearInterval(conn.intervalId);
          return false;
        }
        return true;
      });
      
      if (active.length === 0) {
        this.connections.delete(id);
      } else {
        this.connections.set(id, active);
      }
    }

    // Clean expired query cache
    for (const [key, cached] of this.sharedQueries.entries()) {
      if (now - cached.timestamp > this.QUERY_CACHE_TTL) {
        this.sharedQueries.delete(key);
      }
    }
  }
}

export const sseConnectionManager = new SSEConnectionManager();

// Cleanup every 30 seconds
setInterval(() => {
  sseConnectionManager.cleanup();
}, 30000);

