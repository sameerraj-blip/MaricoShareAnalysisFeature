/**
 * Database Configuration
 * Handles CosmosDB client initialization and container access
 */
import { CosmosClient, Database, Container } from "@azure/cosmos";

// CosmosDB configuration
const COSMOS_ENDPOINT = process.env.COSMOS_ENDPOINT || "";
const COSMOS_KEY = process.env.COSMOS_KEY || "";
const COSMOS_DATABASE_ID = process.env.COSMOS_DATABASE_ID || "marico-insights";
const COSMOS_CONTAINER_ID = process.env.COSMOS_CONTAINER_ID || "chats";
const COSMOS_DASHBOARDS_CONTAINER_ID = process.env.COSMOS_DASHBOARDS_CONTAINER_ID || "dashboards";
const COSMOS_SHARED_ANALYSES_CONTAINER_ID = process.env.COSMOS_SHARED_ANALYSES_CONTAINER_ID || "shared-analyses";
const COSMOS_SHARED_DASHBOARDS_CONTAINER_ID = process.env.COSMOS_SHARED_DASHBOARDS_CONTAINER_ID || "shared-dashboards";

// Initialize CosmosDB client
// Note: Connection retries are handled at the application level via retryOnConnectionError
const client = new CosmosClient({
  endpoint: COSMOS_ENDPOINT,
  key: COSMOS_KEY,
});

let database: Database;
let container: Container;
let dashboardsContainer: Container;
let sharedAnalysesContainer: Container;
let sharedDashboardsContainer: Container;
let initializationInProgress = false;
let initializationPromise: Promise<void> | null = null;

/**
 * Helper function to safely create a container with fallback if throughput limit is reached
 * Handles both provisioned and serverless Cosmos DB accounts
 */
const createContainerSafely = async (
  database: Database,
  containerId: string,
  partitionKey: string,
  throughput: number = 400
): Promise<Container> => {
  try {
    // Try to create with explicit throughput (for provisioned accounts)
    const { container } = await database.containers.createIfNotExists({
      id: containerId,
      partitionKey,
      throughput,
    });
    return container;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    
    // If serverless account error, retry without throughput
    if (errorMessage.includes("serverless") || errorMessage.includes("not supported for serverless")) {
      console.log(`ℹ️ Serverless account detected for container ${containerId}, creating without throughput...`);
      try {
        const { container } = await database.containers.createIfNotExists({
          id: containerId,
          partitionKey,
          // No throughput parameter for serverless accounts
        });
        return container;
      } catch (retryError) {
        // If container already exists, try to read it
        try {
          const { container } = database.container(containerId);
          await container.read();
          console.log(`✅ Using existing container: ${containerId}`);
          return container;
        } catch (readError) {
          throw retryError;
        }
      }
    }
    
    // If throughput limit error, try to read existing container
    if (errorMessage.includes("throughput limit") || errorMessage.includes("RU/s")) {
      console.warn(`⚠️ Throughput limit reached for container ${containerId}, attempting to use existing container...`);
      try {
        const { container } = database.container(containerId);
        // Try to read container to verify it exists
        await container.read();
        console.log(`✅ Using existing container: ${containerId}`);
        return container;
      } catch (readError) {
        // Container doesn't exist, re-throw original error
        throw error;
      }
    }
    throw error;
  }
};

/**
 * Initialize CosmosDB database and containers
 * Can be called multiple times safely - will reuse existing promise if already initializing
 */
export const initializeCosmosDB = async (): Promise<void> => {
  // If already initialized, return immediately
  if (container && dashboardsContainer && sharedAnalysesContainer && sharedDashboardsContainer) {
    return;
  }

  // If initialization is in progress, wait for it
  if (initializationInProgress && initializationPromise) {
    return initializationPromise;
  }

  // Start new initialization
  initializationInProgress = true;
  initializationPromise = (async () => {
    try {
      if (!COSMOS_ENDPOINT || !COSMOS_KEY) {
        throw new Error("CosmosDB endpoint or key not configured. Please set COSMOS_ENDPOINT and COSMOS_KEY environment variables.");
      }

      console.log("🔄 Initializing CosmosDB...");

      // Create database if it doesn't exist
      const { database: db } = await client.databases.createIfNotExists({
        id: COSMOS_DATABASE_ID,
      });
      database = db;
      console.log(`✅ Database ready: ${COSMOS_DATABASE_ID}`);

      // Create containers with explicit throughput (400 RU/s minimum) to avoid exceeding account limits
      // The helper function will fallback to using existing containers if throughput limit is reached
      container = await createContainerSafely(
        database,
        COSMOS_CONTAINER_ID,
        "/fsmrora", // Partition by username for better performance
        400 // Minimum throughput: 400 RU/s
      );
      console.log(`✅ Chats container ready: ${COSMOS_CONTAINER_ID}`);

      dashboardsContainer = await createContainerSafely(
        database,
        COSMOS_DASHBOARDS_CONTAINER_ID,
        "/username",
        400 // Minimum throughput: 400 RU/s
      );
      console.log(`✅ Dashboards container ready: ${COSMOS_DASHBOARDS_CONTAINER_ID}`);

      sharedAnalysesContainer = await createContainerSafely(
        database,
        COSMOS_SHARED_ANALYSES_CONTAINER_ID,
        "/targetEmail",
        400 // Minimum throughput: 400 RU/s
      );
      console.log(`✅ Shared analyses container ready: ${COSMOS_SHARED_ANALYSES_CONTAINER_ID}`);

      sharedDashboardsContainer = await createContainerSafely(
        database,
        COSMOS_SHARED_DASHBOARDS_CONTAINER_ID,
        "/targetEmail",
        400 // Minimum throughput: 400 RU/s
      );
      console.log(`✅ Shared dashboards container ready: ${COSMOS_SHARED_DASHBOARDS_CONTAINER_ID}`);

      console.log("✅ CosmosDB initialized successfully");
    } catch (error) {
      console.error("❌ Failed to initialize CosmosDB:", error);
      const errorMessage = error instanceof Error ? error.message : String(error);
      
      // Check if error is related to serverless accounts (should be handled by createContainerSafely, but just in case)
      if (errorMessage.includes("serverless") || errorMessage.includes("not supported for serverless")) {
        // This should have been handled by createContainerSafely, but if it reaches here, re-throw with context
        throw new Error(`CosmosDB serverless account detected but initialization failed: ${errorMessage}`);
      }
      
      // Check if error is related to throughput limits
      if (errorMessage.includes("throughput limit") || errorMessage.includes("RU/s")) {
        const helpfulMessage = `
CosmosDB throughput limit exceeded. Your account has a limit of 20,000 RU/s.

Possible solutions:
1. If containers already exist, they may have high throughput configured. 
   Check your Azure Portal and reduce throughput on existing containers.
2. Delete unused containers to free up throughput.
3. Request a throughput limit increase from Azure support.
4. Consider using database-level shared throughput instead of container-level.

Error details: ${errorMessage}
        `.trim();
        throw new Error(helpfulMessage);
      }
      
      throw new Error(`CosmosDB initialization failed: ${errorMessage}`);
    } finally {
      initializationInProgress = false;
    }
  })();

  return initializationPromise;
};

/**
 * Wait for chat container to be initialized
 * Will attempt to initialize if not already done
 */
export const waitForContainer = async (maxRetries: number = 60, retryDelay: number = 500): Promise<Container> => {
  // Try to initialize if not already done
  if (!container) {
    try {
      await initializeCosmosDB();
    } catch (error) {
      // If initialization fails, continue to retry loop
      console.warn("⚠️ Initialization attempt failed, will retry:", error);
    }
  }

  let retries = 0;
  
  while (!container && retries < maxRetries) {
    await new Promise(resolve => setTimeout(resolve, retryDelay));
    // Try to initialize again every 5 retries
    if (retries % 5 === 0 && !container) {
      try {
        await initializeCosmosDB();
      } catch (error) {
        // Continue retrying
      }
    }
    retries++;
  }
  
  if (!container) {
    throw new Error("CosmosDB container not initialized. Please check your COSMOS_ENDPOINT and COSMOS_KEY environment variables and ensure CosmosDB is accessible.");
  }
  
  return container;
};

/**
 * Wait for dashboards container to be initialized
 * Will attempt to initialize if not already done
 */
export const waitForDashboardsContainer = async (
  maxRetries: number = 60,
  retryDelay: number = 500
): Promise<Container> => {
  // Try to initialize if not already done
  if (!dashboardsContainer) {
    try {
      await initializeCosmosDB();
    } catch (error) {
      console.warn("⚠️ Initialization attempt failed, will retry:", error);
    }
  }

  let retries = 0;

  while (!dashboardsContainer && retries < maxRetries) {
    await new Promise(resolve => setTimeout(resolve, retryDelay));
    // Try to initialize again every 5 retries
    if (retries % 5 === 0 && !dashboardsContainer) {
      try {
        await initializeCosmosDB();
      } catch (error) {
        // Continue retrying
      }
    }
    retries++;
  }

  if (!dashboardsContainer) {
    throw new Error("CosmosDB dashboards container not initialized. Please check your COSMOS_ENDPOINT and COSMOS_KEY environment variables and ensure CosmosDB is accessible.");
  }

  return dashboardsContainer;
};

/**
 * Wait for shared analyses container to be initialized
 * Will attempt to initialize if not already done
 */
export const waitForSharedAnalysesContainer = async (
  maxRetries: number = 60,
  retryDelay: number = 500
): Promise<Container> => {
  // Try to initialize if not already done
  if (!sharedAnalysesContainer) {
    try {
      await initializeCosmosDB();
    } catch (error) {
      console.warn("⚠️ Initialization attempt failed, will retry:", error);
    }
  }

  let retries = 0;

  while (!sharedAnalysesContainer && retries < maxRetries) {
    await new Promise(resolve => setTimeout(resolve, retryDelay));
    // Try to initialize again every 5 retries
    if (retries % 5 === 0 && !sharedAnalysesContainer) {
      try {
        await initializeCosmosDB();
      } catch (error) {
        // Continue retrying
      }
    }
    retries++;
  }

  if (!sharedAnalysesContainer) {
    throw new Error("CosmosDB shared analyses container not initialized. Please check your COSMOS_ENDPOINT and COSMOS_KEY environment variables and ensure CosmosDB is accessible.");
  }

  return sharedAnalysesContainer;
};

/**
 * Get the CosmosDB client instance
 */
export const getCosmosClient = () => client;

/**
 * Wait for shared dashboards container to be initialized
 * Will attempt to initialize if not already done
 */
export const waitForSharedDashboardsContainer = async (
  maxRetries: number = 60,
  retryDelay: number = 500
): Promise<Container> => {
  // Try to initialize if not already done
  if (!sharedDashboardsContainer) {
    try {
      await initializeCosmosDB();
    } catch (error) {
      console.warn("⚠️ Initialization attempt failed, will retry:", error);
    }
  }

  let retries = 0;

  while (!sharedDashboardsContainer && retries < maxRetries) {
    await new Promise(resolve => setTimeout(resolve, retryDelay));
    // Try to initialize again every 5 retries
    if (retries % 5 === 0 && !sharedDashboardsContainer) {
      try {
        await initializeCosmosDB();
      } catch (error) {
        // Continue retrying
      }
    }
    retries++;
  }

  if (!sharedDashboardsContainer) {
    throw new Error("CosmosDB shared dashboards container not initialized. Please check your COSMOS_ENDPOINT and COSMOS_KEY environment variables and ensure CosmosDB is accessible.");
  }

  return sharedDashboardsContainer;
};

/**
 * Get the database instance
 */
export const getDatabase = () => database;

