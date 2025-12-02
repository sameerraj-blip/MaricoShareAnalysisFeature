/**
 * Models Index
 * Central export point for all database models
 */

// Database configuration
export { initializeCosmosDB } from "./database.config.js";

// Chat model
export * from "./chat.model.js";
export type { ChatDocument } from "./chat.model.js";

// Dashboard model
export * from "./dashboard.model.js";

// Shared Analysis model
export * from "./sharedAnalysis.model.js";

