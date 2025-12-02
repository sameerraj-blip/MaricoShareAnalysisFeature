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

// Initialize CosmosDB client
const client = new CosmosClient({
  endpoint: COSMOS_ENDPOINT,
  key: COSMOS_KEY,
});

let database: Database;
let container: Container;
let dashboardsContainer: Container;
let sharedAnalysesContainer: Container;
let initializationInProgress = false;
let initializationPromise: Promise<void> | null = null;

/**
 * Initialize CosmosDB database and containers
 * Can be called multiple times safely - will reuse existing promise if already initializing
 */
export const initializeCosmosDB = async (): Promise<void> => {
  // If already initialized, return immediately
  if (container && dashboardsContainer && sharedAnalysesContainer) {
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

      // Create container if it doesn't exist
      const { container: cont } = await database.containers.createIfNotExists({
        id: COSMOS_CONTAINER_ID,
        partitionKey: "/fsmrora", // Partition by username for better performance
      });
      container = cont;
      console.log(`✅ Chats container ready: ${COSMOS_CONTAINER_ID}`);

      // Create dashboards container if it doesn't exist
      const { container: dashCont } = await database.containers.createIfNotExists({
        id: COSMOS_DASHBOARDS_CONTAINER_ID,
        partitionKey: "/username",
      });
      dashboardsContainer = dashCont;
      console.log(`✅ Dashboards container ready: ${COSMOS_DASHBOARDS_CONTAINER_ID}`);

      // Create shared analyses container if it doesn't exist
      const { container: sharedCont } = await database.containers.createIfNotExists({
        id: COSMOS_SHARED_ANALYSES_CONTAINER_ID,
        partitionKey: "/targetEmail",
      });
      sharedAnalysesContainer = sharedCont;
      console.log(`✅ Shared analyses container ready: ${COSMOS_SHARED_ANALYSES_CONTAINER_ID}`);

      console.log("✅ CosmosDB initialized successfully");
    } catch (error) {
      console.error("❌ Failed to initialize CosmosDB:", error);
      const errorMessage = error instanceof Error ? error.message : String(error);
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
 * Get the database instance
 */
export const getDatabase = () => database;

