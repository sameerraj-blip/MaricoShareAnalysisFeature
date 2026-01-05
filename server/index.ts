// Main server file
import 'dotenv/config';
import express from "express";
import { corsConfig } from "./middleware/index.js";
import { registerRoutes } from "./routes/index.js";

// Factory function to create the Express app
export function createApp() {
  const app = express();

  // Middleware (increase payload limits for large file uploads and chat history)
  // Set to 1GB to support large CSV files (50MB+)
  app.use(express.json({ limit: '1gb' }));
  app.use(express.urlencoded({ extended: false, limit: '1gb' }));

  // Handle preflight requests explicitly
  app.options('*', corsConfig);

  app.use(corsConfig);

  // Health check endpoint
  app.get('/api/health', (req, res) => {
    res.json({ status: 'OK', message: 'Server is running' });
  });

  // Register all routes (synchronous)
  registerRoutes(app);

  // Initialize optional services in background (non-blocking)
  // These are optional, so we don't wait for them
  // Use dynamic imports to avoid breaking if packages aren't available
  Promise.all([
    import("./models/index.js").then(m => m.initializeCosmosDB()).catch((error) => {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.warn("⚠️ CosmosDB initialization failed on startup, will retry on first use:", errorMessage);
      console.warn("   Make sure COSMOS_ENDPOINT and COSMOS_KEY are set in your environment variables");
    }),
    import("./lib/blobStorage.js").then(m => m.initializeBlobStorage()).catch((error) => {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.warn("⚠️ Azure Blob Storage initialization failed, continuing without it:", errorMessage);
    })
  ]).catch(() => {
    // Ignore - services are optional
  });

  return app;
}

// For local: create and start server
if (!process.env.VERCEL) {
  (async () => {
    try {
      const app = createApp();
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

// No default export needed - createApp is used directly by api/index.ts