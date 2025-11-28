import { CosmosClient, Database, Container } from "@azure/cosmos";
import { ChartSpec, Message, DataSummary, Insight, Dashboard, SharedAnalysisInvite } from "../shared/schema.js";

// CosmosDB configuration
const COSMOS_ENDPOINT = process.env.COSMOS_ENDPOINT || "";
const COSMOS_KEY = process.env.COSMOS_KEY || "";
const COSMOS_DATABASE_ID = process.env.COSMOS_DATABASE_ID || "marico-insights";
const COSMOS_CONTAINER_ID = process.env.COSMOS_CONTAINER_ID || "chats";
const COSMOS_DASHBOARDS_CONTAINER_ID = process.env.COSMOS_DASHBOARDS_CONTAINER_ID || "dashboards";
const COSMOS_SHARED_ANALYSES_CONTAINER_ID = process.env.COSMOS_SHARED_ANALYSES_CONTAINER_ID || "shared-analyses";

// Initialize CosmosDB client
const client = new CosmosClient({
  endpoint: COSMOS_ENDPOINT,
  key: COSMOS_KEY,
});

let database: Database;
let container: Container;
let dashboardsContainer: Container;
let sharedAnalysesContainer: Container;

// Initialize database and container
export const initializeCosmosDB = async () => {
  try {
    if (!COSMOS_ENDPOINT || !COSMOS_KEY) {
      throw new Error("CosmosDB endpoint or key not configured");
    }

    // Create database if it doesn't exist
    const { database: db } = await client.databases.createIfNotExists({
      id: COSMOS_DATABASE_ID,
    });
    database = db;

    // Create container if it doesn't exist
    const { container: cont } = await database.containers.createIfNotExists({
      id: COSMOS_CONTAINER_ID,
      partitionKey: "/fsmrora", // Partition by username for better performance
    });
    container = cont;

    // Create dashboards container if it doesn't exist
    const { container: dashCont } = await database.containers.createIfNotExists({
      id: COSMOS_DASHBOARDS_CONTAINER_ID,
      partitionKey: "/username",
    });
    dashboardsContainer = dashCont;

    // Create shared analyses container if it doesn't exist
    const { container: sharedCont } = await database.containers.createIfNotExists({
      id: COSMOS_SHARED_ANALYSES_CONTAINER_ID,
      partitionKey: "/targetEmail",
    });
    sharedAnalysesContainer = sharedCont;

    console.log("CosmosDB initialized successfully");
  } catch (error) {
    console.error("Failed to initialize CosmosDB:", error);
    throw error;
  }
};

// Helper function to wait for container initialization
// This prevents race conditions where functions try to use container before it's initialized
const waitForContainer = async (maxRetries: number = 10, retryDelay: number = 500): Promise<Container> => {
  let retries = 0;
  
  while (!container && retries < maxRetries) {
    await new Promise(resolve => setTimeout(resolve, retryDelay));
    retries++;
  }
  
  if (!container) {
    throw new Error("CosmosDB container not initialized. Please wait for initialization to complete.");
  }
  
  return container;
};

const waitForSharedAnalysesContainer = async (
  maxRetries: number = 10,
  retryDelay: number = 500
): Promise<Container> => {
  let retries = 0;

  while (!sharedAnalysesContainer && retries < maxRetries) {
    await new Promise(resolve => setTimeout(resolve, retryDelay));
    retries++;
  }

  if (!sharedAnalysesContainer) {
    throw new Error("CosmosDB shared analyses container not initialized.");
  }

  return sharedAnalysesContainer;
};

// Chat document interface
export interface ChatDocument {
  id: string; // Unique chat ID (fileName + timestamp)
  username: string; // User email
  fileName: string; // Original uploaded file name
  uploadedAt: number; // Upload timestamp
  createdAt: number; // Chat creation timestamp
  lastUpdatedAt: number; // Last update timestamp
  collaborators?: string[]; // Emails with access (always includes owner)
  dataSummary: DataSummary; // Data summary from file upload
  messages: Message[]; // Chat messages with charts and insights
  charts: ChartSpec[]; // All charts generated for this chat
  insights: Insight[]; // AI-generated insights from data analysis
  sessionId: string; // Original session ID
  // Enhanced analysis data storage
  rawData: Record<string, any>[]; // Complete raw data from uploaded file
  sampleRows: Record<string, any>[]; // Sample rows for preview (first 50)
  columnStatistics: Record<string, any>; // Statistical analysis of numeric columns
  blobInfo?: { // Azure Blob Storage information
    blobUrl: string;
    blobName: string;
  };
  currentDataBlob?: { // Current processed data blob (for data operations)
    blobUrl: string;
    blobName: string;
    version: number;
    lastUpdated: number;
  };
  dataVersions?: Array<{ // Version history for data operations
    versionId: string;
    blobName: string;
    operation: string;
    description: string;
    timestamp: number;
    parameters?: any;
    affectedRows?: number;
    affectedColumns?: string[];
    rowsBefore?: number;
    rowsAfter?: number;
  }>;
  dataOpsContext?: any; // Context for data operations (pending operations, filters, etc.)
  analysisMetadata: { // Additional metadata about the analysis
    totalProcessingTime: number; // Time taken to process the file
    aiModelUsed: string; // AI model used for analysis
    fileSize: number; // Original file size in bytes
    analysisVersion: string; // Version of analysis algorithm
  };
  dataOpsMode?: boolean; // Whether Data Ops mode is enabled for this session
}

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

// Helper function to generate unique filename with number suffix
const generateUniqueFileName = async (baseFileName: string, username: string): Promise<string> => {
  try {
    // Get all sessions for this user
    const allSessions = await getAllSessions(username);
    
    // Extract base name without extension and any existing number suffix
    const baseNameMatch = baseFileName.match(/^(.+?)(\s*\(\d+\))?(\.[^.]+)?$/);
    const baseNameWithoutExt = baseNameMatch ? baseNameMatch[1] : baseFileName;
    const extension = baseNameMatch && baseNameMatch[3] ? baseNameMatch[3] : '';
    
    // Find all sessions with matching base filename (with or without number suffix)
    const matchingSessions = allSessions.filter(session => {
      const sessionBaseMatch = session.fileName.match(/^(.+?)(\s*\(\d+\))?(\.[^.]+)?$/);
      const sessionBaseName = sessionBaseMatch ? sessionBaseMatch[1] : session.fileName;
      const sessionExt = sessionBaseMatch && sessionBaseMatch[3] ? sessionBaseMatch[3] : '';
      
      // Match if base name and extension are the same
      return sessionBaseName === baseNameWithoutExt && sessionExt === extension;
    });
    
    // If no matches, return original filename
    if (matchingSessions.length === 0) {
      return baseFileName;
    }
    
    // Extract numbers from existing filenames
    // If a filename has no number suffix, it's the first upload (count as 1)
    const existingNumbers = matchingSessions
      .map(session => {
        const match = session.fileName.match(/\((\d+)\)/);
        return match ? parseInt(match[1], 10) : 1; // If no number, treat as (1)
      })
      .sort((a, b) => b - a); // Sort descending
    
    // Find the next available number
    // If we have matching sessions, the next number is max + 1
    // If max is 1 and we have 1 session, next is 2
    // If max is 2 and we have 2 sessions, next is 3, etc.
    const maxNumber = existingNumbers.length > 0 ? existingNumbers[0] : 0;
    const nextNumber = maxNumber + 1;
    
    // Return filename with number suffix
    return `${baseNameWithoutExt} (${nextNumber})${extension}`;
  } catch (error) {
    console.error('Error generating unique filename, using original:', error);
    return baseFileName; // Fallback to original filename on error
  }
};

const deepClone = <T>(value: T): T => JSON.parse(JSON.stringify(value));

const cloneChatDocumentForUser = async (
  source: ChatDocument,
  targetEmail: string
): Promise<ChatDocument> => {
  const containerInstance = await waitForContainer();
  const timestamp = Date.now();
  const clonedFileName = await generateUniqueFileName(source.fileName, targetEmail);
  const newSessionId = `${source.sessionId}_shared_${timestamp}`;

  const clonedDocument: ChatDocument & { fsmrora?: string } = {
    id: `${clonedFileName.replace(/[^a-zA-Z0-9]/g, '_')}_${timestamp}`,
    username: targetEmail,
    fsmrora: targetEmail,
    fileName: clonedFileName,
    uploadedAt: timestamp,
    createdAt: timestamp,
    lastUpdatedAt: timestamp,
    dataSummary: deepClone(source.dataSummary),
    messages: deepClone(source.messages || []),
    charts: deepClone(source.charts || []),
    insights: deepClone(source.insights || []),
    sessionId: newSessionId,
    rawData: deepClone(source.rawData || []),
    sampleRows: deepClone(source.sampleRows || []),
    columnStatistics: deepClone(source.columnStatistics || {}),
    blobInfo: source.blobInfo ? { ...source.blobInfo } : undefined,
    analysisMetadata: source.analysisMetadata ? { ...source.analysisMetadata } : {
      totalProcessingTime: 0,
      aiModelUsed: "gpt-4o",
      fileSize: 0,
      analysisVersion: "1.0.0",
    },
  };

  const { resource } = await containerInstance.items.create(clonedDocument);
  return resource as ChatDocument;
};

// Create a new chat document
export const createChatDocument = async (
  username: string,
  fileName: string,
  sessionId: string,
  dataSummary: DataSummary,
  initialCharts: ChartSpec[] = [],
  rawData: Record<string, any>[] = [],
  sampleRows: Record<string, any>[] = [],
  columnStatistics: Record<string, any> = {},
  blobInfo?: { blobUrl: string; blobName: string },
  analysisMetadata?: {
    totalProcessingTime: number;
    aiModelUsed: string;
    fileSize: number;
    analysisVersion: string;
  },
  insights: Insight[] = []
): Promise<ChatDocument> => {
  const timestamp = Date.now();
  const normalizedUsername = normalizeEmail(username) || username;
  
  // Generate unique filename with number suffix if needed
  const uniqueFileName = await generateUniqueFileName(fileName, normalizedUsername);
  console.log(`📝 Generated unique filename: "${fileName}" -> "${uniqueFileName}"`);
  
  const chatId = `${uniqueFileName.replace(/[^a-zA-Z0-9]/g, '_')}_${timestamp}`;
  
  const chatDocument: ChatDocument & { fsmrora?: string } = {
    id: chatId,
    username: normalizedUsername,
    fsmrora: normalizedUsername, // Add partition key field to match partition key path /fsmrora
    fileName: uniqueFileName,
    uploadedAt: timestamp,
    createdAt: timestamp,
    lastUpdatedAt: timestamp,
    dataSummary,
    messages: [],
    charts: initialCharts,
    insights: insights,
    sessionId,
    rawData,
    sampleRows,
    columnStatistics,
    blobInfo,
    collaborators: [normalizedUsername],
    analysisMetadata: analysisMetadata || {
      totalProcessingTime: 0,
      aiModelUsed: 'gpt-4o',
      fileSize: 0,
      analysisVersion: '1.0.0'
    }
  };

  try {
    // Wait for container to be initialized (with timeout)
    const containerInstance = await waitForContainer();
    
    const { resource } = await containerInstance.items.create(chatDocument);
    return resource as ChatDocument;
  } catch (error) {
    console.error("Failed to create chat document:", error);
    throw error;
  }
};

// Get chat document by ID
export const getChatDocument = async (
  chatId: string,
  requesterEmail: string
): Promise<ChatDocument | null> => {
  try {
    const containerInstance = await waitForContainer();
    const { resources } = await containerInstance.items
      .query(
        {
          query: "SELECT * FROM c WHERE c.id = @chatId",
          parameters: [{ name: "@chatId", value: chatId }],
        },
        { enableCrossPartitionQuery: true }
      )
      .fetchAll();

    if (!resources.length) {
      return null;
    }

    const chatDocument = resources[0] as ChatDocument;
    const collaborators = ensureCollaborators(chatDocument);
    const normalizedRequester = normalizeEmail(requesterEmail);

    if (!normalizedRequester || !collaborators.includes(normalizedRequester)) {
      const error = new Error("Unauthorized to access this analysis");
      (error as any).statusCode = 403;
      throw error;
    }

    return chatDocument;
  } catch (error: any) {
    if (error.code === 404) {
      return null;
    }
    console.error("Failed to get chat document:", error);
    throw error;
  }
};

// Update chat document
export const updateChatDocument = async (chatDocument: ChatDocument): Promise<ChatDocument> => {
  try {
    // Wait for container to be initialized (with timeout)
    const containerInstance = await waitForContainer();
    chatDocument.username = normalizeEmail(chatDocument.username) || chatDocument.username;
    (chatDocument as any).fsmrora = chatDocument.username;
    ensureCollaborators(chatDocument);

    chatDocument.lastUpdatedAt = Date.now();
    const { resource } = await containerInstance.items.upsert(chatDocument);
    console.log(`✅ Updated chat document: ${chatDocument.id}`);
    return resource as unknown as ChatDocument;
  } catch (error) {
    console.error("❌ Failed to update chat document:", error);
    throw error;
  }
};

// Add message to chat
export const addMessageToChat = async (
  chatId: string,
  username: string,
  message: Message
): Promise<ChatDocument> => {
  try {
    const chatDocument = await getChatDocument(chatId, username);
    if (!chatDocument) {
      throw new Error("Chat document not found");
    }

    chatDocument.messages.push(message);
    
    // Add any new charts from the message to the main charts array
    if (message.charts) {
      message.charts.forEach(chart => {
        const existingChart = chatDocument.charts.find(c => 
          c.title === chart.title && c.type === chart.type
        );
        if (!existingChart) {
          chatDocument.charts.push(chart);
        }
      });
    }

    return await updateChatDocument(chatDocument);
  } catch (error) {
    console.error("❌ Failed to add message to chat:", error);
    throw error;
  }
};

// Add one or more messages by sessionId (avoids relying on partition key at callsite)
export const addMessagesBySessionId = async (
  sessionId: string,
  messages: Message[]
): Promise<ChatDocument> => {
  try {
    console.log("📝 addMessagesBySessionId - sessionId:", sessionId, "messages:", messages.map(m => m.role));
    const chatDocumentAny = await getChatBySessionIdEfficient(sessionId as any);
    const chatDocument = chatDocumentAny as unknown as ChatDocument | null;
    if (!chatDocument) {
      throw new Error("Chat document not found for sessionId");
    }

    console.log("🗂️ Appending to doc:", chatDocument.id, "partition:", chatDocument.username, "existing messages:", chatDocument.messages?.length || 0);
    chatDocument.messages.push(...messages);

    // Collect any charts from assistant messages into top-level charts
    messages.forEach((msg) => {
      if (msg.charts && msg.charts.length > 0) {
        msg.charts.forEach((chart) => {
          const exists = chatDocument.charts.find(
            (c) => c.title === chart.title && c.type === chart.type
          );
          if (!exists) {
            chatDocument.charts.push(chart);
          }
        });
      }
    });

    const updated = await updateChatDocument(chatDocument);
    console.log("✅ Upserted chat doc:", updated.id, "messages now:", updated.messages?.length || 0);
    return updated;
  } catch (error) {
    console.error("❌ Failed to add messages by sessionId:", error);
    throw error;
  }
};

// Update a message and truncate all messages after it (used when editing a message)
export const updateMessageAndTruncate = async (
  sessionId: string,
  targetTimestamp: number,
  updatedContent: string
): Promise<ChatDocument> => {
  try {
    console.log("✏️ updateMessageAndTruncate - sessionId:", sessionId, "targetTimestamp:", targetTimestamp);
    const chatDocumentAny = await getChatBySessionIdEfficient(sessionId as any);
    const chatDocument = chatDocumentAny as unknown as ChatDocument | null;
    if (!chatDocument) {
      throw new Error("Chat document not found for sessionId");
    }

    if (!chatDocument.messages || chatDocument.messages.length === 0) {
      throw new Error("No messages found in chat document");
    }

    // Find the message to update by timestamp
    const messageIndex = chatDocument.messages.findIndex(
      (msg) => msg.timestamp === targetTimestamp && msg.role === 'user'
    );

    if (messageIndex === -1) {
      throw new Error(`Message with timestamp ${targetTimestamp} not found`);
    }

    console.log(`🗂️ Found message at index ${messageIndex}, truncating all messages after it`);
    console.log(`📊 Messages before truncation: ${chatDocument.messages.length}`);

    // Update the message content
    chatDocument.messages[messageIndex] = {
      ...chatDocument.messages[messageIndex],
      content: updatedContent,
    };

    // Remove all messages after the edited message
    const messagesToRemove = chatDocument.messages.length - messageIndex - 1;
    if (messagesToRemove > 0) {
      chatDocument.messages.splice(messageIndex + 1);
      console.log(`🗑️ Removed ${messagesToRemove} messages after the edited message`);
    }

    console.log(`📊 Messages after truncation: ${chatDocument.messages.length}`);

    const updated = await updateChatDocument(chatDocument);
    console.log("✅ Updated message and truncated chat doc:", updated.id, "messages now:", updated.messages?.length || 0);
    return updated;
  } catch (error) {
    console.error("❌ Failed to update message and truncate:", error);
    throw error;
  }
};

// Get all chats for a user
export const getUserChats = async (username: string): Promise<ChatDocument[]> => {
  try {
    // Wait for container to be initialized (with timeout)
    const containerInstance = await waitForContainer();
    const normalizedUsername = normalizeEmail(username) || username;

    const query =
      "SELECT * FROM c WHERE (ARRAY_CONTAINS(c.collaborators, @username) OR c.username = @username) ORDER BY c.createdAt DESC";
    const { resources } = await containerInstance.items
      .query(
        {
          query,
          parameters: [{ name: "@username", value: normalizedUsername }],
        },
        { enableCrossPartitionQuery: true }
      )
      .fetchAll();

    const chats = resources.map((doc) => {
      const typed = doc as ChatDocument;
      ensureCollaborators(typed);
      return typed;
    });
    
    return chats;
  } catch (error) {
    console.error("❌ Failed to get user chats:", error);
    throw error;
  }
};

// Get chat by session ID (more efficient)
export const getChatBySessionIdEfficient = async (sessionId: string): Promise<ChatDocument | null> => {
  try {
    // Wait for container to be initialized (with timeout)
    const containerInstance = await waitForContainer();
    
    const query = "SELECT * FROM c WHERE c.sessionId = @sessionId";
    const { resources } = await containerInstance.items.query({
      query,
      parameters: [{ name: "@sessionId", value: sessionId }]
    }).fetchAll();
    const doc = (resources && resources.length > 0) ? resources[0] : null;
    if (!doc) {
      console.warn("⚠️ No chat document found for sessionId:", sessionId);
    } else {
      console.log("🔎 Found chat document by sessionId:", doc.id, "username:", doc.username);
      ensureCollaborators(doc as ChatDocument);
    }
    return doc as unknown as ChatDocument | null;
  } catch (error) {
    console.error("❌ Failed to get chat by session ID:", error);
    throw error;
  }
};

export const getChatBySessionIdForUser = async (
  sessionId: string,
  requesterEmail: string
): Promise<ChatDocument | null> => {
  const chatDocument = await getChatBySessionIdEfficient(sessionId);
  if (!chatDocument) {
    return null;
  }

  const collaborators = ensureCollaborators(chatDocument);
  const normalizedRequester = normalizeEmail(requesterEmail);
  if (!normalizedRequester || !collaborators.includes(normalizedRequester)) {
    const error = new Error("Unauthorized to access this session");
    (error as any).statusCode = 403;
    throw error;
  }

  return chatDocument;
};

// Delete chat document
export const deleteChatDocument = async (chatId: string, username: string): Promise<void> => {
  try {
    // Wait for container to be initialized (with timeout)
    const containerInstance = await waitForContainer();
    
    await containerInstance.item(chatId, username).delete();
    console.log(`✅ Deleted chat document: ${chatId}`);
  } catch (error) {
    console.error("❌ Failed to delete chat document:", error);
    throw error;
  }
};

// Update session fileName by session ID
export const updateSessionFileName = async (
  sessionId: string,
  username: string,
  newFileName: string
): Promise<ChatDocument> => {
  try {
    // Get the chat document by sessionId
    const chatDocument = await getChatBySessionIdEfficient(sessionId);
    
    if (!chatDocument) {
      throw new Error(`Session not found for sessionId: ${sessionId}`);
    }
    
    // Verify the username matches
    if (chatDocument.username !== username) {
      throw new Error('Unauthorized: Session does not belong to this user');
    }
    
    // Update the fileName
    chatDocument.fileName = newFileName.trim();
    
    // Update the document
    const updated = await updateChatDocument(chatDocument);
    console.log(`✅ Updated session fileName: ${sessionId} -> ${newFileName}`);
    return updated;
  } catch (error) {
    console.error("❌ Failed to update session fileName:", error);
    throw error;
  }
};

// =================== Shared Analyses ===================

const buildSharedAnalysisPreview = (chatDocument: ChatDocument) => ({
  fileName: chatDocument.fileName,
  uploadedAt: chatDocument.uploadedAt,
  createdAt: chatDocument.createdAt,
  lastUpdatedAt: chatDocument.lastUpdatedAt,
  chartsCount: chatDocument.charts?.length || 0,
  insightsCount: chatDocument.insights?.length || 0,
  messagesCount: chatDocument.messages?.length || 0,
});

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

export const listSharedAnalysesForUser = async (targetEmail: string): Promise<SharedAnalysisInvite[]> => {
  const sharedContainer = await waitForSharedAnalysesContainer();
  const normalizedTarget = normalizeEmail(targetEmail) || targetEmail;
  const { resources } = await sharedContainer.items.query({
    query: "SELECT * FROM c WHERE c.targetEmail = @targetEmail ORDER BY c.createdAt DESC",
    parameters: [{ name: "@targetEmail", value: normalizedTarget }],
  }).fetchAll();

  return resources as SharedAnalysisInvite[];
};

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

// Delete chat document by session ID
export const deleteSessionBySessionId = async (sessionId: string, username: string): Promise<void> => {
  try {
    // Wait for container to be initialized (with timeout)
    const containerInstance = await waitForContainer();
    
    // First, get the chat document by sessionId to find the chatId
    const chatDocument = await getChatBySessionIdEfficient(sessionId);
    
    if (!chatDocument) {
      throw new Error(`Session not found for sessionId: ${sessionId}`);
    }
    
    const chatId = chatDocument.id;
    
    console.log(`🗑️ Attempting to delete session: ${sessionId}`);
    console.log(`   Chat ID: ${chatId}`);
    console.log(`   Username from doc: ${chatDocument.username}`);
    console.log(`   fsmrora from doc: ${(chatDocument as any).fsmrora || 'not found'}`);
    
    // Try to delete using SQL query since we know the document exists but partition key might be wrong
    // This approach doesn't require the exact partition key value
    try {
      // First, try to read the document to verify the partition key
      // Try with fsmrora field value if it exists
      let partitionKeyUsed: string | undefined;
      let deleteSuccess = false;
      
      // Try different partition key values
      const possiblePartitionKeys = [
        (chatDocument as any).fsmrora,
        chatDocument.username,
        username
      ].filter(Boolean) as string[];
      
      console.log(`   Trying partition keys: ${possiblePartitionKeys.join(', ')}`);
      
      // Try each possible partition key value
      for (const pkValue of possiblePartitionKeys) {
        try {
          // Try to read with this partition key to verify it works
          const testRead = await containerInstance.item(chatId, pkValue).read();
          console.log(`   ✅ Verified partition key: ${pkValue}`);
          
          // Now delete with the verified partition key
          await containerInstance.item(chatId, pkValue).delete();
          console.log(`✅ Successfully deleted session: ${sessionId} (chatId: ${chatId}, partitionKey: ${pkValue})`);
          deleteSuccess = true;
          partitionKeyUsed = pkValue;
          break;
        } catch (pkError: any) {
          if (pkError.code === 404) {
            console.log(`   ⚠️ Partition key ${pkValue} didn't work (404), trying next...`);
            continue;
          }
          // For other errors, re-throw
          throw pkError;
        }
      }
      
      if (!deleteSuccess) {
        // If direct delete failed, the document might have been stored with undefined/null partition key
        // Try using the document's actual partition key from the query result
        console.log(`   ⚠️ Direct delete failed, trying to get actual partition key from document...`);
        
        // Query the document by id to get its actual partition key value
        const queryResult = await containerInstance.items.query({
          query: "SELECT * FROM c WHERE c.id = @id",
          parameters: [{ name: "@id", value: chatId }]
        }).fetchAll();
        
        if (queryResult.resources.length > 0) {
          const doc = queryResult.resources[0];
          // Try to get the actual partition key value from the document
          // CosmosDB might have stored it differently
          const actualPartitionKey = doc.fsmrora || doc.username || chatDocument.username;
          
          console.log(`   Document from query - fsmrora: ${doc.fsmrora || 'not found'}, username: ${doc.username}`);
          console.log(`   Attempting delete with partition key: ${actualPartitionKey}`);
          
          try {
            await containerInstance.item(chatId, actualPartitionKey).delete();
            console.log(`✅ Successfully deleted using partition key from query`);
            deleteSuccess = true;
            partitionKeyUsed = actualPartitionKey;
          } catch (queryDeleteError: any) {
            // If that fails, the document might have been stored without the fsmrora field
            // Try updating the document first to add the fsmrora field, then delete
            console.log(`   ⚠️ Delete with query partition key failed: ${queryDeleteError.code}`);
            console.log(`   Attempting to update document with fsmrora field, then delete...`);
            
            try {
              // Update the document to add the fsmrora field with the username value
              // First, we need to read it with the correct partition key (which we don't know)
              // So we'll use a workaround: update via query result
              const docToUpdate = queryResult.resources[0];
              docToUpdate.fsmrora = docToUpdate.username || chatDocument.username;
              
              // Try to replace the document - this requires the correct partition key
              // Since we don't know it, we'll try with username
              const partitionKeyForUpdate = docToUpdate.fsmrora || docToUpdate.username || chatDocument.username;
              
              try {
                await containerInstance.item(chatId, partitionKeyForUpdate).replace(docToUpdate);
                console.log(`   ✅ Updated document with fsmrora field`);
                
                // Now try to delete with the updated partition key
                await containerInstance.item(chatId, partitionKeyForUpdate).delete();
                console.log(`✅ Successfully deleted after updating fsmrora field`);
                deleteSuccess = true;
                partitionKeyUsed = partitionKeyForUpdate;
              } catch (updateError: any) {
                console.log(`   ⚠️ Update failed: ${updateError.code} - ${updateError.message}`);
                throw new Error(`Cannot delete document - partition key mismatch. The document may need to be manually updated in CosmosDB to include the fsmrora field.`);
              }
            } catch (updateDeleteError: any) {
              throw new Error(`Cannot delete document - partition key mismatch. Error: ${updateDeleteError.message}`);
            }
          }
        } else {
          throw new Error(`Document not found in query - may have already been deleted`);
        }
      }
      
      if (!deleteSuccess) {
        throw new Error(`Could not delete document with any partition key value`);
      }
      
      // Verify deletion using query (more reliable than reading by partition key)
      // This avoids partition key mismatch issues
      try {
        const verifyQuery = await containerInstance.items.query({
          query: "SELECT * FROM c WHERE c.id = @id",
          parameters: [{ name: "@id", value: chatId }]
        }).fetchAll();
        
        if (verifyQuery.resources.length > 0) {
          console.warn(`⚠️ Warning: Document still exists after deletion attempt. ChatId: ${chatId}`);
          // Try one more time with the partition key we used
          if (partitionKeyUsed) {
            try {
              await containerInstance.item(chatId, partitionKeyUsed).delete();
              console.log(`✅ Retry deletion successful with partition key: ${partitionKeyUsed}`);
              return; // Success
            } catch (retryError: any) {
              if (retryError.code === 404) {
                console.log(`   ✅ Document was actually deleted (404 on retry)`);
                return; // Success
              }
              throw new Error(`Deletion failed - document still exists in database after retry`);
            }
          } else {
            throw new Error(`Deletion failed - document still exists in database`);
          }
        } else {
          console.log(`   ✅ Verified: Document no longer exists - deletion successful`);
          return; // Success
        }
      } catch (verifyError: any) {
        // If query fails, assume deletion was successful (document might have been deleted)
        console.log(`   ✅ Deletion completed (verification query had issues, but deletion was attempted)`);
        return; // Success
      }
    } catch (deleteError: any) {
      // If all methods fail, throw the error
      throw deleteError;
    }
  } catch (error: any) {
    console.error("❌ Failed to delete session by sessionId:", error);
    console.error("   Error code:", error.code);
    console.error("   Error statusCode:", error.statusCode);
    console.error("   Error message:", error.message);
    throw error;
  }
};
// =================== Dashboards CRUD ===================

export const createDashboard = async (
  username: string,
  name: string,
  charts: ChartSpec[] = []
): Promise<Dashboard> => {
  if (!dashboardsContainer) {
    throw new Error("CosmosDB dashboards container not initialized.");
  }
  
  // Check if a dashboard with the same name already exists for this username
  const existingDashboards = await getUserDashboards(username);
  const duplicateDashboard = existingDashboards.find(
    d => d.name.toLowerCase().trim() === name.toLowerCase().trim()
  );
  
  if (duplicateDashboard) {
    throw new Error(`A dashboard with the name "${name}" already exists. Please enter a different name.`);
  }
  
  const timestamp = Date.now();
  const id = `${name.replace(/[^a-zA-Z0-9]/g, '_')}_${timestamp}`;
  
  // Create default sheet with charts
  const defaultSheet = {
    id: 'default',
    name: 'Overview',
    charts,
    order: 0,
  };
  
  const dashboard: Dashboard = {
    id,
    username,
    name,
    createdAt: timestamp,
    updatedAt: timestamp,
    charts, // Keep for backward compatibility
    sheets: [defaultSheet],
  };
  const { resource } = await dashboardsContainer.items.create(dashboard);
  return resource as unknown as Dashboard;
};

export const getUserDashboards = async (username: string): Promise<Dashboard[]> => {
  if (!dashboardsContainer) {
    return [];
  }
  const { resources } = await dashboardsContainer.items.query({
    query: "SELECT * FROM c WHERE c.username = @username ORDER BY c.createdAt DESC",
    parameters: [{ name: "@username", value: username }],
  }).fetchAll();
  return resources as unknown as Dashboard[];
};

export const getDashboardById = async (id: string, username: string): Promise<Dashboard | null> => {
  try {
    const { resource } = await dashboardsContainer.item(id, username).read();
    const dashboard = resource as unknown as Dashboard;
    
    // Update lastOpenedAt when dashboard is accessed
    if (dashboard) {
      dashboard.lastOpenedAt = Date.now();
      return await updateDashboard(dashboard);
    }
    
    return dashboard;
  } catch (error: any) {
    if (error.code === 404) return null;
    throw error;
  }
};

export const renameDashboard = async (
  id: string,
  username: string,
  newName: string
): Promise<Dashboard> => {
  const dashboard = await getDashboardById(id, username);
  if (!dashboard) throw new Error("Dashboard not found");
  
  // Check if a dashboard with the same name already exists for this username (excluding current dashboard)
  const existingDashboards = await getUserDashboards(username);
  const duplicateDashboard = existingDashboards.find(
    d => d.id !== id && d.name.toLowerCase().trim() === newName.toLowerCase().trim()
  );
  
  if (duplicateDashboard) {
    throw new Error(`A dashboard with the name "${newName}" already exists. Please enter a different name.`);
  }
  
  dashboard.name = newName;
  dashboard.updatedAt = Date.now();
  return updateDashboard(dashboard);
};

export const updateDashboard = async (dashboard: Dashboard): Promise<Dashboard> => {
  dashboard.updatedAt = Date.now();
  const { resource } = await dashboardsContainer.items.upsert(dashboard);
  return resource as unknown as Dashboard;
};

export const deleteDashboard = async (id: string, username: string): Promise<void> => {
  await dashboardsContainer.item(id, username).delete();
};

export const addChartToDashboard = async (
  id: string,
  username: string,
  chart: ChartSpec,
  sheetId?: string
): Promise<Dashboard> => {
  const dashboard = await getDashboardById(id, username);
  if (!dashboard) throw new Error("Dashboard not found");
  
  // Initialize sheets if not present (backward compatibility)
  if (!dashboard.sheets || dashboard.sheets.length === 0) {
    dashboard.sheets = [{
      id: 'default',
      name: 'Overview',
      charts: [...dashboard.charts],
      order: 0,
    }];
  }
  
  // If sheetId is provided, add to that sheet; otherwise add to first sheet
  const targetSheetId = sheetId || dashboard.sheets[0].id;
  const targetSheet = dashboard.sheets.find(s => s.id === targetSheetId);
  
  if (!targetSheet) {
    throw new Error(`Sheet with id ${targetSheetId} not found`);
  }
  
  targetSheet.charts.push(chart);
  
  // Also update the legacy charts array for backward compatibility
  dashboard.charts.push(chart);
  
  return updateDashboard(dashboard);
};

export const addSheetToDashboard = async (
  id: string,
  username: string,
  sheetName: string
): Promise<Dashboard> => {
  const dashboard = await getDashboardById(id, username);
  if (!dashboard) throw new Error("Dashboard not found");
  
  // Initialize sheets if not present
  if (!dashboard.sheets || dashboard.sheets.length === 0) {
    dashboard.sheets = [{
      id: 'default',
      name: 'Overview',
      charts: [...dashboard.charts],
      order: 0,
    }];
  }
  
  const trimmedName = sheetName.trim();
  
  // Check for duplicate sheet names (case-insensitive)
  const duplicateSheet = dashboard.sheets.find(s => 
    s.name.toLowerCase().trim() === trimmedName.toLowerCase()
  );
  
  if (duplicateSheet) {
    throw new Error(`A sheet with the name "${trimmedName}" already exists. Please enter a different name.`);
  }
  
  const newSheet = {
    id: `sheet-${Date.now()}`,
    name: trimmedName,
    charts: [],
    order: dashboard.sheets.length,
  };
  
  dashboard.sheets.push(newSheet);
  return updateDashboard(dashboard);
};

export const removeSheetFromDashboard = async (
  id: string,
  username: string,
  sheetId: string
): Promise<Dashboard> => {
  const dashboard = await getDashboardById(id, username);
  if (!dashboard) throw new Error("Dashboard not found");
  
  if (!dashboard.sheets || dashboard.sheets.length <= 1) {
    throw new Error("Cannot remove the last sheet");
  }
  
  dashboard.sheets = dashboard.sheets.filter(s => s.id !== sheetId);
  return updateDashboard(dashboard);
};

export const renameSheet = async (
  id: string,
  username: string,
  sheetId: string,
  newName: string
): Promise<Dashboard> => {
  const dashboard = await getDashboardById(id, username);
  if (!dashboard) throw new Error("Dashboard not found");
  
  if (!dashboard.sheets) {
    throw new Error("No sheets found");
  }
  
  const sheet = dashboard.sheets.find(s => s.id === sheetId);
  if (!sheet) throw new Error("Sheet not found");
  
  const trimmedName = newName.trim();
  
  // Check for duplicate sheet names (case-insensitive, excluding current sheet)
  const duplicateSheet = dashboard.sheets.find(s => 
    s.id !== sheetId && s.name.toLowerCase().trim() === trimmedName.toLowerCase()
  );
  
  if (duplicateSheet) {
    throw new Error(`A sheet with the name "${trimmedName}" already exists. Please enter a different name.`);
  }
  
  sheet.name = trimmedName;
  return updateDashboard(dashboard);
};

export const removeChartFromDashboard = async (
  id: string,
  username: string,
  predicate: { index?: number; title?: string; type?: ChartSpec["type"]; sheetId?: string }
): Promise<Dashboard> => {
  const dashboard = await getDashboardById(id, username);
  if (!dashboard) throw new Error("Dashboard not found");

  console.log('Removing chart from dashboard:', { 
    dashboardId: id, 
    predicate, 
    hasSheets: !!dashboard.sheets, 
    sheetsCount: dashboard.sheets?.length || 0 
  });

  // If sheetId is provided, remove from that specific sheet
  if (predicate.sheetId && dashboard.sheets && dashboard.sheets.length > 0) {
    const sheet = dashboard.sheets.find(s => s.id === predicate.sheetId);
    if (!sheet) {
      // If sheet not found, check if it's a default sheet (backward compatibility)
      if (predicate.sheetId === 'default' && dashboard.charts.length > 0) {
        // For default sheet, remove from main charts array
        if (typeof predicate.index === 'number' && predicate.index >= 0 && predicate.index < dashboard.charts.length) {
          dashboard.charts.splice(predicate.index, 1);
        }
        return updateDashboard(dashboard);
      }
      throw new Error(`Sheet with id "${predicate.sheetId}" not found`);
    }

    if (typeof predicate.index === 'number') {
      if (predicate.index >= 0 && predicate.index < sheet.charts.length) {
        // Get the chart BEFORE removing it
        const chartToRemove = sheet.charts[predicate.index];
        
        // Remove from the specific sheet
        sheet.charts.splice(predicate.index, 1);
        
        // Check if this chart exists in other sheets
        const existsInOtherSheets = dashboard.sheets.some(s => 
          s.id !== sheet.id && s.charts.some(c => 
            c.title === chartToRemove.title && c.type === chartToRemove.type
          )
        );
        
        // Only remove from main charts array if it doesn't exist in other sheets
        if (!existsInOtherSheets) {
          const mainIndex = dashboard.charts.findIndex(c => 
            c.title === chartToRemove.title && c.type === chartToRemove.type
          );
          if (mainIndex >= 0) {
            dashboard.charts.splice(mainIndex, 1);
          }
        }
      }
    } else if (predicate.title || predicate.type) {
      // Filter sheet charts
      const removedCharts = sheet.charts.filter(c => {
        const titleMatch = predicate.title ? c.title === predicate.title : false;
        const typeMatch = predicate.type ? c.type === predicate.type : false;
        return titleMatch || typeMatch;
      });
      
      sheet.charts = sheet.charts.filter(c => {
        const titleMatch = predicate.title ? c.title !== predicate.title : true;
        const typeMatch = predicate.type ? c.type !== predicate.type : true;
        return titleMatch || typeMatch;
      });
      
      // Remove from main charts array only if not in other sheets
      removedCharts.forEach(removedChart => {
        const existsInOtherSheets = dashboard.sheets && dashboard.sheets.some(s => 
          s.id !== sheet.id && s.charts.some(c => 
            c.title === removedChart.title && c.type === removedChart.type
          )
        );
        
        if (!existsInOtherSheets) {
          const mainIndex = dashboard.charts.findIndex(c => 
            c.title === removedChart.title && c.type === removedChart.type
          );
          if (mainIndex >= 0) {
            dashboard.charts.splice(mainIndex, 1);
          }
        }
      });
    }
  } else {
    // Legacy behavior: remove from main charts array
    if (typeof predicate.index === 'number') {
      dashboard.charts.splice(predicate.index, 1);
      // Also remove from all sheets
      if (dashboard.sheets) {
        dashboard.sheets.forEach(sheet => {
          if (predicate.index! < sheet.charts.length) {
            sheet.charts.splice(predicate.index!, 1);
          }
        });
      }
    } else if (predicate.title || predicate.type) {
      dashboard.charts = dashboard.charts.filter(c => {
        const titleMatch = predicate.title ? c.title !== predicate.title : true;
        const typeMatch = predicate.type ? c.type !== predicate.type : true;
        return titleMatch || typeMatch;
      });
      // Also remove from all sheets
      if (dashboard.sheets) {
        dashboard.sheets.forEach(sheet => {
          sheet.charts = sheet.charts.filter(c => {
            const titleMatch = predicate.title ? c.title !== predicate.title : true;
            const typeMatch = predicate.type ? c.type !== predicate.type : true;
            return titleMatch || typeMatch;
          });
        });
      }
    }
  }

  return updateDashboard(dashboard);
};

export const updateChartInsightOrRecommendation = async (
  id: string,
  username: string,
  chartIndex: number,
  sheetId: string | undefined,
  updates: { keyInsight?: string }
): Promise<Dashboard> => {
  const dashboard = await getDashboardById(id, username);
  if (!dashboard) throw new Error("Dashboard not found");

  // Initialize sheets if not present (backward compatibility)
  if (!dashboard.sheets || dashboard.sheets.length === 0) {
    dashboard.sheets = [{
      id: 'default',
      name: 'Overview',
      charts: [...dashboard.charts],
      order: 0,
    }];
  }

  // Find the target sheet
  const targetSheetId = sheetId || dashboard.sheets[0].id;
  const targetSheet = dashboard.sheets.find(s => s.id === targetSheetId);

  if (!targetSheet) {
    throw new Error(`Sheet with id ${targetSheetId} not found`);
  }

  if (chartIndex < 0 || chartIndex >= targetSheet.charts.length) {
    throw new Error(`Chart index ${chartIndex} is out of range`);
  }

  const chart = targetSheet.charts[chartIndex];

  // Update the chart's keyInsight
  if (updates.keyInsight !== undefined) {
    // If empty string, set to undefined to remove it
    chart.keyInsight = updates.keyInsight === '' ? undefined : updates.keyInsight;
  }

  // Also update in the legacy charts array for backward compatibility
  // Find the matching chart in the main charts array
  const mainChartIndex = dashboard.charts.findIndex(c => 
    c.title === chart.title && c.type === chart.type
  );
  if (mainChartIndex >= 0) {
    if (updates.keyInsight !== undefined) {
      // If empty string, set to undefined to remove it
      dashboard.charts[mainChartIndex].keyInsight = updates.keyInsight === '' ? undefined : updates.keyInsight;
    }
  }

  return updateDashboard(dashboard);
};

// Generate column statistics for numeric columns
export const generateColumnStatistics = (data: Record<string, any>[], numericColumns: string[]): Record<string, any> => {
  const stats: Record<string, any> = {};
  
  for (const column of numericColumns) {
    const values = data.map(row => Number(row[column])).filter(v => !isNaN(v));
    
    if (values.length > 0) {
      const sortedValues = [...values].sort((a, b) => a - b);
      const sum = values.reduce((a, b) => a + b, 0);
      const mean = sum / values.length;
      
      // Calculate median
      const mid = Math.floor(sortedValues.length / 2);
      const median = sortedValues.length % 2 === 0 
        ? (sortedValues[mid - 1] + sortedValues[mid]) / 2 
        : sortedValues[mid];
      
      // Calculate standard deviation
      const variance = values.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / values.length;
      const standardDeviation = Math.sqrt(variance);
      
      // Calculate quartiles
      const q1Index = Math.floor(sortedValues.length * 0.25);
      const q3Index = Math.floor(sortedValues.length * 0.75);
      const q1 = sortedValues[q1Index];
      const q3 = sortedValues[q3Index];
      
      stats[column] = {
        count: values.length,
        min: Math.min(...values),
        max: Math.max(...values),
        sum: sum,
        mean: Number(mean.toFixed(2)),
        median: Number(median.toFixed(2)),
        standardDeviation: Number(standardDeviation.toFixed(2)),
        q1: Number(q1.toFixed(2)),
        q3: Number(q3.toFixed(2)),
        range: Math.max(...values) - Math.min(...values),
        variance: Number(variance.toFixed(2))
      };
    }
  }
  
  return stats;
};

// Get all sessions from CosmosDB container (optionally filtered by username)
export const getAllSessions = async (username?: string): Promise<ChatDocument[]> => {
  try {
    // Wait for container to be initialized (with timeout)
    const containerInstance = await waitForContainer();
    
    let query = "SELECT * FROM c";
    const parameters: Array<{ name: string; value: any }> = [];
    
    // Add username filter if provided
    if (username) {
      query += " WHERE (ARRAY_CONTAINS(c.collaborators, @username) OR c.username = @username)";
      parameters.push({ name: "@username", value: normalizeEmail(username) || username });
    }
    
    query += " ORDER BY c.createdAt DESC";
    
    const queryOptions = parameters.length > 0 ? { parameters } : {};
    const { resources } = await containerInstance.items
      .query(
        {
          query,
          ...queryOptions,
        },
        { enableCrossPartitionQuery: true }
      )
      .fetchAll();
    
    console.log(`✅ Retrieved ${resources.length} sessions from CosmosDB${username ? ` for user: ${username}` : ''}`);
    return resources.map((doc) => {
      const typed = doc as ChatDocument;
      ensureCollaborators(typed);
      return typed;
    });
  } catch (error) {
    console.error("❌ Failed to get all sessions:", error);
    throw error;
  }
};

// Get all sessions with pagination (optionally filtered by username)
export const getAllSessionsPaginated = async (
  pageSize: number = 10,
  continuationToken?: string,
  username?: string
): Promise<{
  sessions: ChatDocument[];
  continuationToken?: string;
  hasMoreResults: boolean;
}> => {
  try {
    // Wait for container to be initialized (with timeout)
    const containerInstance = await waitForContainer();
    
    let query = "SELECT * FROM c";
    const parameters: Array<{ name: string; value: any }> = [];
    
    // Add username filter if provided
    if (username) {
      query += " WHERE (ARRAY_CONTAINS(c.collaborators, @username) OR c.username = @username)";
      parameters.push({ name: "@username", value: normalizeEmail(username) || username });
    }
    
    query += " ORDER BY c.createdAt DESC";
    
    const queryOptions = {
      maxItemCount: pageSize,
      continuationToken,
      ...(parameters.length > 0 && { parameters }),
    };
    
    const { resources, continuationToken: nextToken, hasMoreResults } = await containerInstance.items
      .query(
        {
          query,
          parameters,
        },
        queryOptions
      )
      .fetchNext();
    
    console.log(`✅ Retrieved ${resources.length} sessions (page size: ${pageSize})${username ? ` for user: ${username}` : ''}`);
    
    const sessions = resources.map((doc) => {
      const typed = doc as ChatDocument;
      ensureCollaborators(typed);
      return typed;
    });

    return {
      sessions,
      continuationToken: nextToken,
      hasMoreResults: hasMoreResults || false,
    };
  } catch (error) {
    console.error("❌ Failed to get paginated sessions:", error);
    throw error;
  }
};

// Get sessions with filtering options
export const getSessionsWithFilters = async (options: {
  username?: string;
  fileName?: string;
  dateFrom?: number;
  dateTo?: number;
  limit?: number;
  orderBy?: 'createdAt' | 'lastUpdatedAt' | 'uploadedAt';
  orderDirection?: 'ASC' | 'DESC';
}): Promise<ChatDocument[]> => {
  try {
    // Wait for container to be initialized (with timeout)
    const containerInstance = await waitForContainer();
    
    let query = "SELECT * FROM c WHERE 1=1";
    const parameters: Array<{ name: string; value: any }> = [];
    
    // Add filters based on options
    if (options.username) {
      query += " AND (ARRAY_CONTAINS(c.collaborators, @username) OR c.username = @username)";
      parameters.push({ name: "@username", value: normalizeEmail(options.username) || options.username });
    }
    
    if (options.fileName) {
      query += " AND CONTAINS(c.fileName, @fileName)";
      parameters.push({ name: "@fileName", value: options.fileName });
    }
    
    if (options.dateFrom) {
      query += " AND c.createdAt >= @dateFrom";
      parameters.push({ name: "@dateFrom", value: options.dateFrom });
    }
    
    if (options.dateTo) {
      query += " AND c.createdAt <= @dateTo";
      parameters.push({ name: "@dateTo", value: options.dateTo });
    }
    
    // Add ordering
    const orderBy = options.orderBy || 'createdAt';
    const orderDirection = options.orderDirection || 'DESC';
    query += ` ORDER BY c.${orderBy} ${orderDirection}`;
    
    // Add limit if specified
    if (options.limit) {
      query += ` OFFSET 0 LIMIT ${options.limit}`;
    }
    
    const queryOptions = options.limit ? { maxItemCount: options.limit } : {};
    
    const { resources } = await containerInstance.items
      .query(
        {
          query,
          parameters,
        },
        { ...queryOptions, enableCrossPartitionQuery: true }
      )
      .fetchAll();
    
    console.log(`✅ Retrieved ${resources.length} sessions with filters`);
    return resources.map((doc) => {
      const typed = doc as ChatDocument;
      ensureCollaborators(typed);
      return typed;
    });
  } catch (error) {
    console.error("❌ Failed to get filtered sessions:", error);
    throw error;
  }
};

// Get session statistics
export const getSessionStatistics = async (): Promise<{
  totalSessions: number;
  totalUsers: number;
  totalMessages: number;
  totalCharts: number;
  sessionsByUser: Record<string, number>;
  sessionsByDate: Record<string, number>;
}> => {
  try {
    // Get all sessions
    const allSessions = await getAllSessions();
    
    // Calculate statistics
    const totalSessions = allSessions.length;
    const uniqueUsers = new Set(allSessions.map(s => s.username));
    const totalUsers = uniqueUsers.size;
    
    const totalMessages = allSessions.reduce((sum, session) => sum + session.messages.length, 0);
    const totalCharts = allSessions.reduce((sum, session) => sum + session.charts.length, 0);
    
    // Sessions by user
    const sessionsByUser: Record<string, number> = {};
    allSessions.forEach(session => {
      sessionsByUser[session.username] = (sessionsByUser[session.username] || 0) + 1;
    });
    
    // Sessions by date (grouped by day)
    const sessionsByDate: Record<string, number> = {};
    allSessions.forEach(session => {
      const date = new Date(session.createdAt).toISOString().split('T')[0];
      sessionsByDate[date] = (sessionsByDate[date] || 0) + 1;
    });
    
    console.log(`✅ Generated session statistics: ${totalSessions} sessions, ${totalUsers} users`);
    
    return {
      totalSessions,
      totalUsers,
      totalMessages,
      totalCharts,
      sessionsByUser,
      sessionsByDate,
    };
  } catch (error) {
    console.error("❌ Failed to get session statistics:", error);
    throw error;
  }
};
