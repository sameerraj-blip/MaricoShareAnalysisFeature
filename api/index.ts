// Vercel serverless function entry point
// This wraps the Express app for Vercel deployment
import 'dotenv/config';

// Set VERCEL env so server knows it's running on Vercel
process.env.VERCEL = '1';

// Import the app factory function
import { createApp } from '../server/index.js';

// Initialize app immediately (Vercel will cache this)
// @vercel/node automatically handles Express apps
const appPromise = createApp();

// Export the app promise - Vercel's @vercel/node will handle it
// For Express apps, Vercel expects the app directly, not a handler
export default appPromise;
