/**
 * Redis Caching Utilities
 * Provides caching for query results, correlation calculations, and analysis
 */

import { getRedisClient, isRedisAvailable } from './redisClient.js';
import crypto from 'crypto';

/**
 * Generate cache key from session ID and query parameters
 */
function generateCacheKey(
  sessionId: string,
  prefix: string,
  ...params: (string | number | undefined)[]
): string {
  const paramStr = params
    .filter(p => p !== undefined && p !== null)
    .map(p => String(p))
    .join(':');
  
  // Create hash of parameters if too long
  const hash = crypto.createHash('md5').update(paramStr).digest('hex').substring(0, 8);
  
  return `marico:${prefix}:${sessionId}:${hash}`;
}

/**
 * Cache TTL (Time To Live) in seconds
 */
const CACHE_TTL = {
  QUERY_RESULT: 3600,        // 1 hour for query results
  CORRELATION: 7200,        // 2 hours for correlation calculations
  ANALYSIS: 1800,           // 30 minutes for analysis results
  METADATA: 86400,          // 24 hours for metadata
  DATA_SUMMARY: 86400,      // 24 hours for data summaries
} as const;

export interface CacheOptions {
  ttl?: number;
  prefix?: string;
}

/**
 * Get cached value
 */
export async function getCached<T>(
  sessionId: string,
  prefix: string,
  params: (string | number | undefined)[],
  options: CacheOptions = {}
): Promise<T | null> {
  if (!isRedisAvailable()) {
    return null;
  }

  try {
    const client = await getRedisClient();
    if (!client) {
      return null;
    }

    const key = generateCacheKey(sessionId, prefix, ...params);
    const cached = await client.get(key);
    
    if (cached) {
      console.log(`✅ Cache hit: ${prefix} for session ${sessionId}`);
      return JSON.parse(cached) as T;
    }
    
    return null;
  } catch (error) {
    console.error('⚠️ Redis cache get error:', error);
    return null;
  }
}

/**
 * Set cached value
 */
export async function setCached<T>(
  sessionId: string,
  prefix: string,
  params: (string | number | undefined)[],
  value: T,
  options: CacheOptions = {}
): Promise<void> {
  if (!isRedisAvailable()) {
    return;
  }

  try {
    const client = await getRedisClient();
    if (!client) {
      return;
    }

    const key = generateCacheKey(sessionId, prefix, ...params);
    const ttl = options.ttl || CACHE_TTL[prefix as keyof typeof CACHE_TTL] || 3600;
    
    await client.setEx(key, ttl, JSON.stringify(value));
    console.log(`💾 Cached: ${prefix} for session ${sessionId} (TTL: ${ttl}s)`);
  } catch (error) {
    console.error('⚠️ Redis cache set error:', error);
  }
}

/**
 * Invalidate cache for a session
 */
export async function invalidateSessionCache(sessionId: string): Promise<void> {
  if (!isRedisAvailable()) {
    return;
  }

  try {
    const client = await getRedisClient();
    if (!client) {
      return;
    }

    const pattern = `marico:*:${sessionId}:*`;
    const keys = await client.keys(pattern);
    
    if (keys.length > 0) {
      await client.del(keys);
      console.log(`🗑️ Invalidated ${keys.length} cache entries for session ${sessionId}`);
    }
  } catch (error) {
    console.error('⚠️ Redis cache invalidation error:', error);
  }
}

/**
 * Invalidate specific cache prefix for a session
 */
export async function invalidateCache(
  sessionId: string,
  prefix: string
): Promise<void> {
  if (!isRedisAvailable()) {
    return;
  }

  try {
    const client = await getRedisClient();
    if (!client) {
      return;
    }

    const pattern = `marico:${prefix}:${sessionId}:*`;
    const keys = await client.keys(pattern);
    
    if (keys.length > 0) {
      await client.del(keys);
      console.log(`🗑️ Invalidated ${keys.length} ${prefix} cache entries for session ${sessionId}`);
    }
  } catch (error) {
    console.error('⚠️ Redis cache invalidation error:', error);
  }
}

/**
 * Cache query result
 */
export async function cacheQueryResult<T>(
  sessionId: string,
  requiredColumns: string[],
  result: T
): Promise<void> {
  const columnsKey = requiredColumns.sort().join(',');
  await setCached(
    sessionId,
    'query_result',
    [columnsKey],
    result,
    { ttl: CACHE_TTL.QUERY_RESULT }
  );
}

/**
 * Get cached query result
 */
export async function getCachedQueryResult<T>(
  sessionId: string,
  requiredColumns: string[]
): Promise<T | null> {
  const columnsKey = requiredColumns.sort().join(',');
  return getCached<T>(
    sessionId,
    'query_result',
    [columnsKey],
    { ttl: CACHE_TTL.QUERY_RESULT }
  );
}

/**
 * Cache correlation result
 */
export async function cacheCorrelation<T>(
  sessionId: string,
  targetVariable: string,
  comparisonVariables: string[],
  result: T
): Promise<void> {
  const varsKey = [targetVariable, ...comparisonVariables.sort()].join(':');
  await setCached(
    sessionId,
    'correlation',
    [varsKey],
    result,
    { ttl: CACHE_TTL.CORRELATION }
  );
}

/**
 * Get cached correlation result
 */
export async function getCachedCorrelation<T>(
  sessionId: string,
  targetVariable: string,
  comparisonVariables: string[]
): Promise<T | null> {
  const varsKey = [targetVariable, ...comparisonVariables.sort()].join(':');
  return getCached<T>(
    sessionId,
    'correlation',
    [varsKey],
    { ttl: CACHE_TTL.CORRELATION }
  );
}

/**
 * Cache analysis result
 */
export async function cacheAnalysis<T>(
  sessionId: string,
  question: string,
  result: T
): Promise<void> {
  await setCached(
    sessionId,
    'analysis',
    [question],
    result,
    { ttl: CACHE_TTL.ANALYSIS }
  );
}

/**
 * Get cached analysis result
 */
export async function getCachedAnalysis<T>(
  sessionId: string,
  question: string
): Promise<T | null> {
  return getCached<T>(
    sessionId,
    'analysis',
    [question],
    { ttl: CACHE_TTL.ANALYSIS }
  );
}

/**
 * Clear all cache (use with caution)
 */
export async function clearAllCache(): Promise<void> {
  if (!isRedisAvailable()) {
    return;
  }

  try {
    const client = await getRedisClient();
    if (!client) {
      return;
    }

    const keys = await client.keys('marico:*');
    if (keys.length > 0) {
      await client.del(keys);
      console.log(`🗑️ Cleared ${keys.length} cache entries`);
    }
  } catch (error) {
    console.error('⚠️ Redis cache clear error:', error);
  }
}

