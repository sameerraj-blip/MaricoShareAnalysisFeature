import { Router } from "express";
import { 
  getAllSessionsEndpoint,
  getSessionsPaginatedEndpoint,
  getSessionsFilteredEndpoint,
  getSessionStatisticsEndpoint,
  getSessionDetailsEndpoint,
  getSessionsByUserEndpoint,
  deleteSessionEndpoint
} from "../controllers/sessionController.js";

const router = Router();

// Get all sessions
router.get('/sessions', getAllSessionsEndpoint);

// Get sessions with pagination
router.get('/sessions/paginated', getSessionsPaginatedEndpoint);

// Get sessions with filters
router.get('/sessions/filtered', getSessionsFilteredEndpoint);

// Get session statistics
router.get('/sessions/statistics', getSessionStatisticsEndpoint);

// Get detailed session by session ID
router.get('/sessions/details/:sessionId', getSessionDetailsEndpoint);

// Get sessions by user
router.get('/sessions/user/:username', getSessionsByUserEndpoint);

// Delete session by session ID
router.delete('/sessions/:sessionId', deleteSessionEndpoint);

export default router;
