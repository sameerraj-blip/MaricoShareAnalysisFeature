import { Router } from "express";
import { dataOpsChatWithAI, dataOpsChatWithAIStream } from "../controllers/dataOpsController.js";

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

console.log('✅ Data Ops routes registered: /api/data-ops/chat, /api/data-ops/chat/stream');

export default router;

