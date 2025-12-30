import crypto from 'crypto';

/**
 * Cache entry interface
 */
interface CacheEntry<T> {
  value: T;
  expiresAt: number;
  createdAt: number;
}

/**
 * Cache key components
 */
interface CacheKeyComponents {
  sessionId: string;
  queryHash: string;
  columnsUsed: string[];
}

/**
 * In-memory cache for query results
 * Key format: sessionId_queryHash_columnsUsed
 */
class QueryCache {
  private cache: Map<string, CacheEntry<any>> = new Map();
  private readonly defaultTTL: number = 60 * 60 * 1000; // 1 hour in milliseconds

  /**
   * Generate cache key from components
   */
  private generateCacheKey(components: CacheKeyComponents): string {
    const columnsStr = components.columnsUsed.sort().join(',');
    return `${components.sessionId}_${components.queryHash}_${columnsStr}`;
  }

  /**
   * Hash query text for cache key
   */
  hashQuery(query: string): string {
    return crypto.createHash('sha256').update(query.toLowerCase().trim()).digest('hex').substring(0, 16);
  }

  /**
   * Get cached result
   */
  get<T>(sessionId: string, query: string, columnsUsed: string[]): T | null {
    const queryHash = this.hashQuery(query);
    const key = this.generateCacheKey({ sessionId, queryHash, columnsUsed });

    const entry = this.cache.get(key);
    if (!entry) {
      return null;
    }

    // Check if expired
    if (Date.now() > entry.expiresAt) {
      this.cache.delete(key);
      return null;
    }

    console.log(`✅ Cache hit for query: ${query.substring(0, 50)}...`);
    return entry.value as T;
  }

  /**
   * Set cached result
   */
  set<T>(sessionId: string, query: string, columnsUsed: string[], value: T, ttl?: number): void {
    const queryHash = this.hashQuery(query);
    const key = this.generateCacheKey({ sessionId, queryHash, columnsUsed });
    const expiresAt = Date.now() + (ttl || this.defaultTTL);

    this.cache.set(key, {
      value,
      expiresAt,
      createdAt: Date.now(),
    });

    console.log(`💾 Cached result for query: ${query.substring(0, 50)}... (TTL: ${(ttl || this.defaultTTL) / 1000}s)`);
  }

  /**
   * Invalidate cache for a session
   * Called when data operations modify the dataset
   */
  invalidateSession(sessionId: string): void {
    let deletedCount = 0;
    for (const key of this.cache.keys()) {
      if (key.startsWith(`${sessionId}_`)) {
        this.cache.delete(key);
        deletedCount++;
      }
    }
    if (deletedCount > 0) {
      console.log(`🗑️ Invalidated ${deletedCount} cache entries for session: ${sessionId}`);
    }
  }

  /**
   * Clear all cache entries
   */
  clear(): void {
    const size = this.cache.size;
    this.cache.clear();
    console.log(`🗑️ Cleared ${size} cache entries`);
  }

  /**
   * Get cache statistics
   */
  getStats(): { size: number; entries: Array<{ key: string; age: number; expiresIn: number }> } {
    const entries = Array.from(this.cache.entries()).map(([key, entry]) => ({
      key,
      age: Date.now() - entry.createdAt,
      expiresIn: entry.expiresAt - Date.now(),
    }));

    return {
      size: this.cache.size,
      entries,
    };
  }

  /**
   * Clean expired entries (can be called periodically)
   */
  cleanExpired(): number {
    const now = Date.now();
    let cleaned = 0;
    for (const [key, entry] of this.cache.entries()) {
      if (now > entry.expiresAt) {
        this.cache.delete(key);
        cleaned++;
      }
    }
    if (cleaned > 0) {
      console.log(`🧹 Cleaned ${cleaned} expired cache entries`);
    }
    return cleaned;
  }
}

// Singleton instance
const queryCache = new QueryCache();

// Clean expired entries every 5 minutes
setInterval(() => {
  queryCache.cleanExpired();
}, 5 * 60 * 1000);

export default queryCache;

interface SharedInviteCacheEntry {
  pending: any[];
  accepted: any[];
  timestamp: number;
}

class SharedInviteCache {
  private cache: Map<string, SharedInviteCacheEntry> = new Map();
  private readonly TTL = 5000; // 5 seconds (less than polling interval)

  get(userEmail: string): SharedInviteCacheEntry | null {
    const entry = this.cache.get(userEmail);
    if (!entry) return null;
    
    if (Date.now() - entry.timestamp > this.TTL) {
      this.cache.delete(userEmail);
      return null;
    }
    
    return entry;
  }

  set(userEmail: string, pending: any[], accepted: any[]): void {
    this.cache.set(userEmail, {
      pending,
      accepted,
      timestamp: Date.now(),
    });
  }

  invalidate(userEmail: string): void {
    this.cache.delete(userEmail);
  }

  clear(): void {
    this.cache.clear();
  }
}

export const sharedInviteCache = new SharedInviteCache();



