// Vercel serverless function entry point
// This wraps the Express app for Vercel deployment
import 'dotenv/config';

// Set VERCEL env so server knows it's running on Vercel
process.env.VERCEL = '1';

// Import and re-export the Express app
// The server/index.ts will initialize services when VERCEL is set
import app from '../server/index.js';

export default app;
