import { Router } from "express";
import multer from "multer";
import express from "express";
import { uploadFile, getUploadStatus, getQueueStats } from "../controllers/uploadController.js";

// Configure multer for file uploads (in-memory storage)
// For very large files, consider using disk storage with streaming
// Increased to 1GB to support large CSV files (50MB+)
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 1024 * 1024 * 1024, // 1GB (1024MB) - increased for large file support
  },
  fileFilter: (req: express.Request, file: Express.Multer.File, cb: multer.FileFilterCallback) => {
    const allowedTypes = [
      'text/csv',
      'application/vnd.ms-excel',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    ];
    
    if (allowedTypes.includes(file.mimetype) || 
        file.originalname.match(/\.(csv|xls|xlsx)$/i)) {
      cb(null, true);
    } else {
      cb(new Error('Invalid file type. Please upload CSV or Excel files.'));
    }
  },
});

const router = Router();

// File upload endpoint - now returns jobId immediately
router.post('/upload', upload.single('file'), (err: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
  // Handle multer errors
  if (err instanceof multer.MulterError) {
    if (err.code === 'LIMIT_FILE_SIZE') {
      return res.status(400).json({ 
        error: 'File too large', 
        message: `File size exceeds the maximum limit of 1GB. Your file is too large to upload.`,
        maxSize: '1GB',
        receivedSize: req.headers['content-length'] ? `${(parseInt(req.headers['content-length']) / 1024 / 1024).toFixed(2)}MB` : 'unknown'
      });
    }
    return res.status(400).json({ 
      error: 'Upload error', 
      message: err.message 
    });
  }
  // Handle other errors
  if (err) {
    return res.status(400).json({ 
      error: 'Upload error', 
      message: err.message 
    });
  }
  next();
}, uploadFile);

// Upload status endpoint
router.get('/upload/status/:jobId', getUploadStatus);

// Queue statistics endpoint (for monitoring)
router.get('/upload/queue/stats', getQueueStats);

export default router;
