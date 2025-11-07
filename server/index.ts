// Main server file
import 'dotenv/config';
import express from "express";
import { corsConfig } from "./middleware/index.js";
import { registerRoutes } from "./routes/index.js";
import { initializeCosmosDB } from "./lib/cosmosDB.js";
import { initializeBlobStorage } from "./lib/blobStorage.js";

const app = express();

// Middleware (increase payload limits for chat history and chart data)
app.use(express.json({ limit: '20mb' }));
app.use(express.urlencoded({ extended: false, limit: '20mb' }));

// Handle preflight requests explicitly
app.options('*', corsConfig);

app.use(corsConfig);

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.json({ status: 'OK', message: 'Server is running' });
});

// Initialize services (for both local and Vercel)
let initialized = false;
async function initializeServices() {
  if (initialized) return;
  
  try {
    // Initialize CosmosDB (optional)
    try {
      await initializeCosmosDB();
    } catch (cosmosError) {
      const errorMessage = cosmosError instanceof Error ? cosmosError.message : String(cosmosError);
      console.warn("⚠️ CosmosDB initialization failed, continuing without it:", errorMessage);
    }
    
    // Initialize Azure Blob Storage (optional)
    try {
      await initializeBlobStorage();
    } catch (blobError) {
      const errorMessage = blobError instanceof Error ? blobError.message : String(blobError);
      console.warn("⚠️ Azure Blob Storage initialization failed, continuing without it:", errorMessage);
    }
    
    await registerRoutes(app);
    initialized = true;
  } catch (error) {
    console.error("Failed to initialize services:", error);
    throw error;
  }
}

// For Vercel: export the app and initialize on first request
// For local: run the server normally
if (process.env.VERCEL) {
  // Vercel serverless mode - initialize services
  initializeServices().catch(console.error);
}

// Local development mode
if (!process.env.VERCEL) {
  (async () => {
    try {
      await initializeServices();
      const { createServer } = await import("http");
      const server = createServer(app);
      const port = process.env.PORT || 3003;
      server.listen(port, () => {
        console.log(`Server running on port ${port}`);
      });
    } catch (error) {
      console.error("Failed to start server:", error);
      process.exit(1);
    }
  })();
}

// Export app for Vercel (only exported when imported by api/index.ts)
export default app;