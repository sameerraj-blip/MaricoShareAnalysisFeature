/**
 * Shared Dashboard Model
 * Handles all database operations for shared dashboard invites
 */
import { SharedDashboardInvite } from "../shared/schema.js";
import { waitForSharedDashboardsContainer } from "./database.config.js";
import { getDashboardById, Dashboard } from "./dashboard.model.js";

const normalizeEmail = (value: string) => value?.trim().toLowerCase();

/**
 * Build preview object for shared dashboard
 */
const buildSharedDashboardPreview = (dashboard: Dashboard) => ({
  name: dashboard.name,
  createdAt: dashboard.createdAt,
  updatedAt: dashboard.updatedAt,
  sheetsCount: dashboard.sheets?.length || 1,
  chartsCount: dashboard.charts?.length || 0,
});

/**
 * Create a shared dashboard invite
 */
export const createSharedDashboardInvite = async ({
  ownerEmail,
  targetEmail,
  sourceDashboardId,
  permission,
  note,
}: {
  ownerEmail: string;
  targetEmail: string;
  sourceDashboardId: string;
  permission: "view" | "edit";
  note?: string;
}): Promise<SharedDashboardInvite> => {
  if (!ownerEmail || !targetEmail) {
    const error = new Error("Both owner and target emails are required to share a dashboard.");
    (error as any).statusCode = 400;
    throw error;
  }

  const normalizedOwner = normalizeEmail(ownerEmail) || ownerEmail;
  const normalizedTarget = normalizeEmail(targetEmail) || targetEmail;

  if (normalizedOwner === normalizedTarget) {
    const error = new Error("You cannot share a dashboard with yourself.");
    (error as any).statusCode = 400;
    throw error;
  }

  const sharedContainer = await waitForSharedDashboardsContainer();
  const sourceDashboard = await getDashboardById(sourceDashboardId, normalizedOwner);

  if (!sourceDashboard) {
    throw new Error("Unable to find the source dashboard to share.");
  }

  if (normalizeEmail(sourceDashboard.username) !== normalizedOwner) {
    const error = new Error("You can only share dashboards that you own.");
    (error as any).statusCode = 403;
    throw error;
  }

  // Check if user already has access (check for existing accepted invite or collaborator)
  const { resources: existingInvites } = await sharedContainer.items
    .query({
      query: "SELECT * FROM c WHERE c.sourceDashboardId = @dashboardId AND c.targetEmail = @targetEmail",
      parameters: [
        { name: "@dashboardId", value: sourceDashboardId },
        { name: "@targetEmail", value: normalizedTarget },
      ],
    })
    .fetchAll();

  const hasAcceptedInvite = existingInvites.some(
    (invite: any) => invite.status === "accepted" || invite.status === "pending"
  );

  // Also check if user is already in dashboard collaborators
  const isAlreadyCollaborator = sourceDashboard.collaborators?.some(
    (c) => c.userId.toLowerCase() === normalizedTarget
  );

  if (hasAcceptedInvite || isAlreadyCollaborator) {
    const error = new Error("This teammate already has access to the shared dashboard.");
    (error as any).statusCode = 409;
    throw error;
  }

  const timestamp = Date.now();
  const invite: SharedDashboardInvite = {
    id: `shared_dashboard_${sourceDashboard.id}_${timestamp}`,
    sourceDashboardId,
    ownerEmail: normalizedOwner,
    targetEmail: normalizedTarget,
    permission,
    status: "pending",
    createdAt: timestamp,
    note,
    preview: buildSharedDashboardPreview(sourceDashboard),
  };

  const { resource } = await sharedContainer.items.create(invite);
  return resource as SharedDashboardInvite;
};

/**
 * List shared dashboards for a user (incoming invites)
 */
export const listSharedDashboardsForUser = async (targetEmail: string): Promise<SharedDashboardInvite[]> => {
  const sharedContainer = await waitForSharedDashboardsContainer();
  const normalizedTarget = normalizeEmail(targetEmail) || targetEmail;
  const { resources } = await sharedContainer.items.query({
    query: "SELECT * FROM c WHERE c.targetEmail = @targetEmail ORDER BY c.createdAt DESC",
    parameters: [{ name: "@targetEmail", value: normalizedTarget }],
  }).fetchAll();

  return resources as SharedDashboardInvite[];
};

/**
 * List shared dashboards for owner (sent invites)
 */
export const listSharedDashboardsForOwner = async (ownerEmail: string): Promise<SharedDashboardInvite[]> => {
  const sharedContainer = await waitForSharedDashboardsContainer();
  const normalizedOwner = normalizeEmail(ownerEmail) || ownerEmail;
  const { resources } = await sharedContainer.items
    .query(
      {
        query: "SELECT * FROM c WHERE c.ownerEmail = @ownerEmail ORDER BY c.createdAt DESC",
        parameters: [{ name: "@ownerEmail", value: normalizedOwner }],
      },
      { enableCrossPartitionQuery: true }
    )
    .fetchAll();

  return resources as SharedDashboardInvite[];
};

/**
 * Get shared dashboard invite by ID
 */
export const getSharedDashboardInviteById = async (
  id: string,
  targetEmail: string
): Promise<SharedDashboardInvite | null> => {
  try {
    const sharedContainer = await waitForSharedDashboardsContainer();
    const normalizedTarget = normalizeEmail(targetEmail) || targetEmail;
    const { resource } = await sharedContainer.item(id, normalizedTarget).read();
    return resource as SharedDashboardInvite;
  } catch (error: any) {
    if (error.code === 404) {
      return null;
    }
    throw error;
  }
};

/**
 * Accept a shared dashboard invite
 */
export const acceptSharedDashboardInvite = async (
  id: string,
  targetEmail: string
): Promise<{ invite: SharedDashboardInvite; dashboard: Dashboard }> => {
  const sharedContainer = await waitForSharedDashboardsContainer();
  const normalizedTarget = normalizeEmail(targetEmail) || targetEmail;
  const invite = await getSharedDashboardInviteById(id, normalizedTarget);

  if (!invite) {
    const error = new Error("Shared dashboard invite not found.");
    (error as any).statusCode = 404;
    throw error;
  }

  if (invite.status === "declined") {
    const error = new Error("This shared dashboard invite has already been declined.");
    (error as any).statusCode = 400;
    throw error;
  }

  const sourceDashboard = await getDashboardById(invite.sourceDashboardId, invite.ownerEmail);
  if (!sourceDashboard) {
    const error = new Error("The original dashboard is no longer available.");
    (error as any).statusCode = 404;
    throw error;
  }

  // Add user to dashboard collaborators with their permission
  const collaborators = sourceDashboard.collaborators || [];
  const existingCollaboratorIndex = collaborators.findIndex(
    (c) => c.userId.toLowerCase() === normalizedTarget
  );

  if (existingCollaboratorIndex >= 0) {
    // Update existing collaborator's permission
    collaborators[existingCollaboratorIndex] = {
      userId: normalizedTarget,
      permission: invite.permission,
    };
  } else {
    // Add new collaborator
    collaborators.push({
      userId: normalizedTarget,
      permission: invite.permission,
    });
  }

  // Update the dashboard with new collaborators
  const updatedDashboard = {
    ...sourceDashboard,
    collaborators,
    updatedAt: Date.now(),
  };

  // Save updated dashboard
  const { updateDashboard } = await import("./dashboard.model.js");
  const savedDashboard = await updateDashboard(updatedDashboard);

  const updatedInvite: SharedDashboardInvite = {
    ...invite,
    status: "accepted",
    acceptedAt: Date.now(),
  };

  const { resource } = await sharedContainer.items.upsert(updatedInvite);

  return {
    invite: resource as SharedDashboardInvite,
    dashboard: savedDashboard,
  };
};

/**
 * Decline a shared dashboard invite
 */
export const declineSharedDashboardInvite = async (
  id: string,
  targetEmail: string
): Promise<SharedDashboardInvite> => {
  const sharedContainer = await waitForSharedDashboardsContainer();
  const normalizedTarget = normalizeEmail(targetEmail) || targetEmail;
  const invite = await getSharedDashboardInviteById(id, normalizedTarget);

  if (!invite) {
    const error = new Error("Shared dashboard invite not found.");
    (error as any).statusCode = 404;
    throw error;
  }

  if (invite.status !== "pending") {
    return invite;
  }

  const updatedInvite: SharedDashboardInvite = {
    ...invite,
    status: "declined",
    declinedAt: Date.now(),
  };

  const { resource } = await sharedContainer.items.upsert(updatedInvite);
  return resource as SharedDashboardInvite;
};
