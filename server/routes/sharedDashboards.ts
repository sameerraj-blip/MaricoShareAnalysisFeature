import { Router } from "express";
import {
  acceptSharedDashboardController,
  declineSharedDashboardController,
  getIncomingSharedDashboardsController,
  getSentSharedDashboardsController,
  getSharedDashboardInviteController,
  shareDashboardController,
  streamIncomingSharedDashboardsController,
} from "../controllers/sharedDashboardController.js";

const router = Router();

router.post("/shared-dashboards", shareDashboardController);
router.get("/shared-dashboards/incoming", getIncomingSharedDashboardsController);
router.get("/shared-dashboards/incoming/stream", streamIncomingSharedDashboardsController);
router.get("/shared-dashboards/sent", getSentSharedDashboardsController);
router.get("/shared-dashboards/:inviteId", getSharedDashboardInviteController);
router.post("/shared-dashboards/:inviteId/accept", acceptSharedDashboardController);
router.post("/shared-dashboards/:inviteId/decline", declineSharedDashboardController);

export default router;
