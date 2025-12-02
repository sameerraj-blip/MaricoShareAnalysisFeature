/**
 * Response Formatter
 * Utility functions for formatting API responses
 */
import { Response } from "express";

/**
 * Send success response
 */
export function sendSuccess(res: Response, data: any, statusCode: number = 200): void {
  res.status(statusCode).json(data);
}

/**
 * Send error response
 */
export function sendError(res: Response, error: string | Error, statusCode: number = 500): void {
  const errorMessage = error instanceof Error ? error.message : error;
  res.status(statusCode).json({ error: errorMessage });
}

/**
 * Send validation error response
 */
export function sendValidationError(res: Response, error: string): void {
  sendError(res, error, 400);
}

/**
 * Send unauthorized error response
 */
export function sendUnauthorized(res: Response, error: string = 'Unauthorized'): void {
  sendError(res, error, 401);
}

/**
 * Send not found error response
 */
export function sendNotFound(res: Response, error: string = 'Resource not found'): void {
  sendError(res, error, 404);
}

/**
 * Send conflict error response
 */
export function sendConflict(res: Response, error: string): void {
  sendError(res, error, 409);
}

