/**
 * Authentication Helper
 * Utility functions for extracting user information from requests
 */
import { Request } from "express";

/**
 * Extract username/email from request
 * Checks both body and headers
 */
export function extractUsername(req: Request): string | null {
  const username = (req.body.username as string) || (req.headers['x-user-email'] as string);
  return username || null;
}

/**
 * Extract username/email from request or throw error
 */
export function requireUsername(req: Request): string {
  const username = extractUsername(req);
  if (!username) {
    throw new Error('Missing authenticated user email');
  }
  return username;
}

