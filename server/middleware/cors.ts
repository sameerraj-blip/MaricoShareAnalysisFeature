/**
 * CORS Middleware Configuration
 * Handles cross-origin resource sharing for the API
 */
import cors from "cors";

/**
 * Get allowed origins from environment variables
 */
const getAllowedOrigins = (): string[] => {
  const origins: string[] = [
    'http://localhost:3000', 
    'http://localhost:3001', 
    'http://localhost:3002', 
    'http://localhost:3003', 
    'http://localhost:3004'
  ];
  
  // Add production frontend URL from environment variable
  if (process.env.FRONTEND_URL) {
    origins.push(process.env.FRONTEND_URL);
  }
  
  return origins;
};

/**
 * CORS configuration
 */
export const corsConfig = cors({
  origin: (origin: string | undefined, callback: (err: Error | null, allow?: boolean) => void) => {
    const allowedOrigins = getAllowedOrigins();
    
    // Debug logging
    if (process.env.NODE_ENV === 'production') {
      console.log('CORS Origin check:', origin);
      console.log('NODE_ENV:', process.env.NODE_ENV);
    }
    
    // Allow requests with no origin (mobile apps, curl, Postman, etc.)
    if (!origin) {
      return callback(null, true);
    }
    
    // Check if origin is in allowed list
    if (allowedOrigins.includes(origin)) {
      return callback(null, true);
    }
    
    // Allow Vercel domains (works in both production and when NODE_ENV might not be set)
    if (origin && origin.includes('.vercel.app')) {
      console.log('Allowing Vercel domain:', origin);
      return callback(null, true);
    }
    
    // Allow Render domains (for backend-to-backend communication)
    if (origin && origin.includes('.onrender.com')) {
      console.log('Allowing Render domain:', origin);
      return callback(null, true);
    }
    
    // Allow localhost in development
    if (origin && origin.includes('localhost')) {
      console.log('Allowing localhost:', origin);
      return callback(null, true);
    }
    
    // Block origin not in allowed list
    console.warn('CORS blocked origin:', origin);
    console.warn('Allowed origins:', allowedOrigins);
    callback(new Error('Not allowed by CORS'));
  },
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH'],
  allowedHeaders: [
    'Content-Type', 
    'Authorization', 
    'X-Requested-With',
    'Accept',
    'Origin',
    'Access-Control-Request-Method',
    'Access-Control-Request-Headers',
    'X-User-Email',
    'x-user-email',
    'X-User-Name',
    'x-user-name'
  ],
  exposedHeaders: ['Content-Length'],
  optionsSuccessStatus: 200,
  preflightContinue: false
});
