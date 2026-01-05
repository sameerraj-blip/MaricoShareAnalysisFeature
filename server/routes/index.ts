import { Express } from "express";
import { createServer, type Server } from "http";
import uploadRoutes from "./upload.js";
import chatRoutes from "./chat.js";
import chatManagementRoutes from "./chatManagement.js";
import blobStorageRoutes from "./blobStorage.js";
import sessionRoutes from "./sessions.js";
import dataRetrievalRoutes from "./dataRetrieval.js";
import dashboardRoutes from "./dashboards.js";
import sharedAnalysisRoutes from "./sharedAnalyses.js";
import sharedDashboardRoutes from "./sharedDashboards.js";
import dataOpsRoutes from "./dataOps.js";
import dataApiRoutes from "./dataApi.js";

export function registerRoutes(app: Express): Server | void {
  // Register route modules
  app.use('/api', uploadRoutes);
  app.use('/api', chatRoutes);
  app.use('/api', chatManagementRoutes);
  app.use('/api', blobStorageRoutes);
  app.use('/api', sessionRoutes);
  app.use('/api/data', dataRetrievalRoutes);
  app.use('/api', dashboardRoutes);
  app.use('/api', sharedAnalysisRoutes);
  app.use('/api', sharedDashboardRoutes);
  app.use('/api', dataOpsRoutes);
  app.use('/api/data', dataApiRoutes);

  // For Vercel, we don't need to create HTTP server
  if (process.env.VERCEL) {
    return;
  }
  
  // For local development, create HTTP server
  const httpServer = createServer(app);
  return httpServer;
}
