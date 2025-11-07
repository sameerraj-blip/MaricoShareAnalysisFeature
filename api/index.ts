// Vercel serverless function entry point
// This wraps the Express app for Vercel deployment
import 'dotenv/config';
import type { Request, Response } from 'express';

// Set VERCEL env so server knows it's running on Vercel
process.env.VERCEL = '1';

// Import the app factory function
import { createApp } from '../server/index.js';

// Cache the app promise (Vercel will reuse this across invocations)
let appPromise: Promise<any> | null = null;

function getApp() {
  if (!appPromise) {
    appPromise = createApp();
  }
  return appPromise;
}

// Vercel serverless function handler
// @vercel/node can handle Express apps, but we need to await initialization
export default async (req: Request, res: Response) => {
  try {
    const app = await getApp();
    // Delegate to Express app
    app(req, res);
  } catch (error) {
    console.error('Error initializing app:', error);
    if (!res.headersSent) {
      res.status(500).json({ 
        error: 'Internal server error',
        message: error instanceof Error ? error.message : 'Unknown error'
      });
    }
  }
};
