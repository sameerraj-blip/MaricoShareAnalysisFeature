import { Router } from "express";
import { dataOpsChatWithAI, dataOpsChatWithAIStream, downloadModifiedDataset } from "../controllers/dataOpsController.js";

const router = Router();

// Health check for Data Ops routes
router.get('/data-ops/health', (req, res) => {
  console.log('✅ Data Ops health check endpoint hit');
  res.json({ status: 'ok', service: 'data-ops' });
});

// Data Ops chat endpoint
router.post('/data-ops/chat', dataOpsChatWithAI);

// Streaming Data Ops chat endpoint (SSE)
router.post('/data-ops/chat/stream', dataOpsChatWithAIStream);

// Download modified dataset endpoint
router.get('/data-ops/download/:sessionId', downloadModifiedDataset);

console.log('✅ Data Ops routes registered: /api/data-ops/chat, /api/data-ops/chat/stream, /api/data-ops/download/:sessionId');

export default router;

