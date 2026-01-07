/**
 * Redis Client Configuration
 * Provides caching for query results, correlation calculations, and analysis results
 */

// Dynamic import for Redis to handle optional dependency
let redisModule: any = null;
let RedisClientType: any = null;
let createClient: any = null;

async function loadRedis() {
  if (redisModule) {
    return { createClient, RedisClientType };
  }
  
  try {
    redisModule = await import('redis');
    createClient = redisModule.createClient || redisModule.default?.createClient;
    RedisClientType = redisModule.RedisClientType || redisModule.default?.RedisClientType;
    
    if (!createClient) {
      throw new Error('Redis createClient not found');
    }
    
    return { createClient, RedisClientType };
  } catch (error) {
    console.warn('⚠️ Redis module not available. Caching will be disabled.');
    return null;
  }
}

let redisClient: any = null;
let redisAvailable: boolean = false;

/**
 * Initialize Redis client with lazy loading
 */
export async function getRedisClient(): Promise<any | null> {
  // Return existing client if available
  if (redisClient && redisAvailable) {
    return redisClient;
  }

  // Try to load Redis module
  const redis = await loadRedis();
  if (!redis) {
    redisAvailable = false;
    return null;
  }

  // Check if Redis URL is configured
  const redisUrl = process.env.REDIS_URL || process.env.REDIS_HOST;
  
  if (!redisUrl) {
    console.warn('⚠️ Redis not configured. Caching will be disabled. Set REDIS_URL or REDIS_HOST environment variable.');
    redisAvailable = false;
    return null;
  }

  try {
    // Create Redis client
    const client = redis.createClient({
      url: redisUrl,
      socket: {
        reconnectStrategy: (retries) => {
          if (retries > 10) {
            console.error('❌ Redis connection failed after 10 retries. Caching disabled.');
            redisAvailable = false;
            return new Error('Redis connection failed');
          }
          return Math.min(retries * 100, 3000);
        },
      },
    });

    // Handle connection events
    client.on('error', (err) => {
      console.error('❌ Redis Client Error:', err);
      redisAvailable = false;
    });

    client.on('connect', () => {
      console.log('🔌 Redis connecting...');
    });

    client.on('ready', () => {
      console.log('✅ Redis connected and ready');
      redisAvailable = true;
    });

    client.on('reconnecting', () => {
      console.log('🔄 Redis reconnecting...');
    });

    // Connect to Redis
    await client.connect();
    
    redisClient = client;
    redisAvailable = true;
    
    return redisClient;
  } catch (error) {
    console.error('❌ Failed to initialize Redis:', error);
    console.warn('⚠️ Continuing without Redis caching. Some features may be slower.');
    redisAvailable = false;
    return null;
  }
}

/**
 * Check if Redis is available
 */
export function isRedisAvailable(): boolean {
  return redisAvailable && redisClient !== null;
}

/**
 * Close Redis connection
 */
export async function closeRedisClient(): Promise<void> {
  if (redisClient) {
    try {
      await redisClient.quit();
      redisClient = null;
      redisAvailable = false;
      console.log('✅ Redis connection closed');
    } catch (error) {
      console.error('❌ Error closing Redis connection:', error);
    }
  }
}

/**
 * Get Redis client (synchronous check)
 */
export function getRedisClientSync(): any | null {
  return redisClient;
}

