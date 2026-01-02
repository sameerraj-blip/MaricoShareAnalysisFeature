import { Request, Response } from "express";
import multer from "multer";
import { uploadFileToBlob } from "../lib/blobStorage.js";
import { uploadQueue } from "../utils/uploadQueue.js";
import { createPlaceholderSession } from "../models/chat.model.js";

/**
 * Upload file endpoint - now uses async queue processing
 * Returns immediately with jobId for status tracking
 */
export const uploadFile = async (
  req: Request & { file?: Express.Multer.File },
  res: Response
) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    // Get username from request body or headers
    const username = req.body.username || req.headers['x-user-email'] || 'anonymous@example.com';

    // Generate a unique session ID for this upload
    const sessionId = `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

    // Upload file to Azure Blob Storage first (this is fast)
    let blobInfo;
    try {
      blobInfo = await uploadFileToBlob(
        req.file.buffer,
        req.file.originalname,
        username,
        req.file.mimetype
      );
      console.log(`✅ File uploaded to blob storage: ${blobInfo.blobName}`);
    } catch (blobError) {
      console.error("Failed to upload file to blob storage:", blobError);
      // Continue without failing the upload - blob storage is optional
    }

    // Create placeholder session immediately so it exists in the database
    // This prevents 404 errors when frontend tries to fetch session details
    try {
      const placeholder = await createPlaceholderSession(
        username,
        req.file.originalname,
        sessionId,
        req.file.size,
        blobInfo
      );
      console.log(`✅ Placeholder session created: ${sessionId} (chatId: ${placeholder.id})`);
    } catch (placeholderError: any) {
      // Log the full error details for debugging
      console.error("❌ Failed to create placeholder session:", {
        error: placeholderError?.message || placeholderError,
        code: placeholderError?.code,
        statusCode: placeholderError?.statusCode,
        sessionId,
        username
      });
      // Don't fail the upload - the session will be created during processing
      // But log this as a warning since it means the frontend will get 404s initially
      console.warn("⚠️ Upload will continue, but frontend may get 404 errors until processing completes");
    }

    // Enqueue the processing job
    const jobId = await uploadQueue.enqueue(
      sessionId,
      username,
      req.file.originalname,
      req.file.buffer,
      req.file.mimetype,
      blobInfo
    );

    console.log(`📤 Upload job enqueued: ${jobId} for session ${sessionId}`);

    // Return immediately with job ID and session ID
    res.status(202).json({
      jobId,
      sessionId,
      fileName: req.file.originalname, // Include fileName so frontend can show it immediately
      status: 'processing',
      message: 'File upload accepted. Processing in background. Use /api/upload/status/:jobId to check progress.',
    });
  } catch (error) {
    console.error('Upload error:', error);
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Failed to process file',
    });
  }
};

/**
 * Get upload job status
 */
export const getUploadStatus = async (req: Request, res: Response) => {
  try {
    const { jobId } = req.params;
    
    if (!jobId) {
      return res.status(400).json({ error: 'Job ID is required' });
    }

    const job = uploadQueue.getJob(jobId);
    
    if (!job) {
      return res.status(404).json({ error: 'Job not found' });
    }

    const response: any = {
      jobId: job.jobId,
      sessionId: job.sessionId,
      status: job.status,
      progress: job.progress,
      createdAt: job.createdAt,
    };

    if (job.startedAt) {
      response.startedAt = job.startedAt;
    }

    if (job.completedAt) {
      response.completedAt = job.completedAt;
    }

    if (job.error) {
      response.error = job.error;
    }

    if (job.status === 'completed' && job.result) {
      response.result = job.result;
    }

    res.json(response);
  } catch (error) {
    console.error('Get upload status error:', error);
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Failed to get upload status',
    });
  }
};

/**
 * Get queue statistics (admin endpoint)
 */
export const getQueueStats = async (req: Request, res: Response) => {
  try {
    const stats = uploadQueue.getStats();
    res.json(stats);
  } catch (error) {
    console.error('Get queue stats error:', error);
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Failed to get queue stats',
    });
  }
};
