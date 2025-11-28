/**
 * Shared Analysis Model
 * Handles all database operations for shared analysis invites
 */
import { SharedAnalysisInvite } from "../shared/schema.js";
import { waitForSharedAnalysesContainer } from "./database.config.js";
import { getChatBySessionIdEfficient, updateChatDocument, ChatDocument } from "./chat.model.js";

const normalizeEmail = (value: string) => value?.trim().toLowerCase();

const ensureCollaborators = (chatDocument: ChatDocument): string[] => {
  const owner = normalizeEmail(chatDocument.username);
  const collaborators = Array.from(
    new Set(
      (chatDocument.collaborators || [])
        .map(normalizeEmail)
        .filter((email): email is string => Boolean(email))
    )
  );

  if (!collaborators.includes(owner)) {
    collaborators.unshift(owner);
  }

  chatDocument.collaborators = collaborators;
  return collaborators;
};

/**
 * Build preview object for shared analysis
 */
const buildSharedAnalysisPreview = (chatDocument: ChatDocument) => ({
  fileName: chatDocument.fileName,
  uploadedAt: chatDocument.uploadedAt,
  createdAt: chatDocument.createdAt,
  lastUpdatedAt: chatDocument.lastUpdatedAt,
  chartsCount: chatDocument.charts?.length || 0,
  insightsCount: chatDocument.insights?.length || 0,
  messagesCount: chatDocument.messages?.length || 0,
});

/**
 * Create a shared analysis invite
 */
export const createSharedAnalysisInvite = async ({
  ownerEmail,
  targetEmail,
  sourceSessionId,
  note,
}: {
  ownerEmail: string;
  targetEmail: string;
  sourceSessionId: string;
  note?: string;
}): Promise<SharedAnalysisInvite> => {
  if (!ownerEmail || !targetEmail) {
    const error = new Error("Both owner and target emails are required to share an analysis.");
    (error as any).statusCode = 400;
    throw error;
  }

  const normalizedOwner = normalizeEmail(ownerEmail) || ownerEmail;
  const normalizedTarget = normalizeEmail(targetEmail) || targetEmail;

  if (normalizedOwner === normalizedTarget) {
    const error = new Error("You cannot share an analysis with yourself.");
    (error as any).statusCode = 400;
    throw error;
  }

  const sharedContainer = await waitForSharedAnalysesContainer();
  const sourceChat = await getChatBySessionIdEfficient(sourceSessionId);

  if (!sourceChat) {
    throw new Error("Unable to find the source analysis to share.");
  }

  if (normalizeEmail(sourceChat.username) !== normalizedOwner) {
    const error = new Error("You can only share analyses that you own.");
    (error as any).statusCode = 403;
    throw error;
  }

  const collaborators = ensureCollaborators(sourceChat);
  if (collaborators.includes(normalizedTarget)) {
    const error = new Error("This teammate already has access to the shared analysis.");
    (error as any).statusCode = 409;
    throw error;
  }

  const timestamp = Date.now();
  const invite: SharedAnalysisInvite = {
    id: `shared_${sourceChat.id}_${timestamp}`,
    sourceSessionId,
    sourceChatId: sourceChat.id,
    ownerEmail: normalizedOwner,
    targetEmail: normalizedTarget,
    status: "pending",
    createdAt: timestamp,
    note,
    preview: buildSharedAnalysisPreview(sourceChat),
  };

  const { resource } = await sharedContainer.items.create(invite);
  return resource as SharedAnalysisInvite;
};

/**
 * List shared analyses for a user (incoming invites)
 */
export const listSharedAnalysesForUser = async (targetEmail: string): Promise<SharedAnalysisInvite[]> => {
  const sharedContainer = await waitForSharedAnalysesContainer();
  const normalizedTarget = normalizeEmail(targetEmail) || targetEmail;
  const { resources } = await sharedContainer.items.query({
    query: "SELECT * FROM c WHERE c.targetEmail = @targetEmail ORDER BY c.createdAt DESC",
    parameters: [{ name: "@targetEmail", value: normalizedTarget }],
  }).fetchAll();

  return resources as SharedAnalysisInvite[];
};

/**
 * List shared analyses for owner (sent invites)
 */
export const listSharedAnalysesForOwner = async (ownerEmail: string): Promise<SharedAnalysisInvite[]> => {
  const sharedContainer = await waitForSharedAnalysesContainer();
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

  return resources as SharedAnalysisInvite[];
};

/**
 * Get shared analysis invite by ID
 */
export const getSharedAnalysisInviteById = async (
  id: string,
  targetEmail: string
): Promise<SharedAnalysisInvite | null> => {
  try {
    const sharedContainer = await waitForSharedAnalysesContainer();
    const normalizedTarget = normalizeEmail(targetEmail) || targetEmail;
    const { resource } = await sharedContainer.item(id, normalizedTarget).read();
    return resource as SharedAnalysisInvite;
  } catch (error: any) {
    if (error.code === 404) {
      return null;
    }
    throw error;
  }
};

/**
 * Accept a shared analysis invite
 */
export const acceptSharedAnalysisInvite = async (
  id: string,
  targetEmail: string
): Promise<{ invite: SharedAnalysisInvite; newSession: ChatDocument }> => {
  const sharedContainer = await waitForSharedAnalysesContainer();
  const normalizedTarget = normalizeEmail(targetEmail) || targetEmail;
  const invite = await getSharedAnalysisInviteById(id, normalizedTarget);

  if (!invite) {
    const error = new Error("Shared analysis invite not found.");
    (error as any).statusCode = 404;
    throw error;
  }

  if (invite.status === "declined") {
    const error = new Error("This shared analysis invite has already been declined.");
    (error as any).statusCode = 400;
    throw error;
  }

  const sourceChat = await getChatBySessionIdEfficient(invite.sourceSessionId);
  if (!sourceChat) {
    const error = new Error("The original analysis is no longer available.");
    (error as any).statusCode = 404;
    throw error;
  }

  const collaborators = ensureCollaborators(sourceChat);
  if (!collaborators.includes(normalizedTarget)) {
    sourceChat.collaborators = [...collaborators, normalizedTarget];
    await updateChatDocument(sourceChat);
  }

  const updatedInvite: SharedAnalysisInvite = {
    ...invite,
    status: "accepted",
    acceptedAt: Date.now(),
    acceptedSessionId: sourceChat.sessionId,
  };

  const { resource } = await sharedContainer.items.upsert(updatedInvite);

  return {
    invite: resource as SharedAnalysisInvite,
    newSession: sourceChat,
  };
};

/**
 * Decline a shared analysis invite
 */
export const declineSharedAnalysisInvite = async (
  id: string,
  targetEmail: string
): Promise<SharedAnalysisInvite> => {
  const sharedContainer = await waitForSharedAnalysesContainer();
  const normalizedTarget = normalizeEmail(targetEmail) || targetEmail;
  const invite = await getSharedAnalysisInviteById(id, normalizedTarget);

  if (!invite) {
    const error = new Error("Shared analysis invite not found.");
    (error as any).statusCode = 404;
    throw error;
  }

  if (invite.status !== "pending") {
    return invite;
  }

  const updatedInvite: SharedAnalysisInvite = {
    ...invite,
    status: "declined",
    declinedAt: Date.now(),
  };

  const { resource } = await sharedContainer.items.upsert(updatedInvite);
  return resource as SharedAnalysisInvite;
};

