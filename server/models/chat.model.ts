/**
 * Chat Model
 * Handles all database operations for chat documents and sessions
 */
import { ChartSpec, Message, DataSummary, Insight } from "../shared/schema.js";
import { waitForContainer } from "./database.config.js";

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
  rawData: Record<string, any>[]; // Complete raw data from uploaded file (updated after each data operation)
  sampleRows: Record<string, any>[]; // Sample rows for preview (first 100)
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

// Helper functions
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

const deepClone = <T>(value: T): T => JSON.parse(JSON.stringify(value));

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
    const maxNumber = existingNumbers.length > 0 ? existingNumbers[0] : 0;
    const nextNumber = maxNumber + 1;
    
    // Return filename with number suffix
    return `${baseNameWithoutExt} (${nextNumber})${extension}`;
  } catch (error) {
    console.error('Error generating unique filename, using original:', error);
    return baseFileName; // Fallback to original filename on error
  }
};

/**
 * Create a new chat document
 */
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
    const containerInstance = await waitForContainer();
    const { resource } = await containerInstance.items.create(chatDocument);
    return resource as ChatDocument;
  } catch (error) {
    console.error("Failed to create chat document:", error);
    throw error;
  }
};

/**
 * Get chat document by ID
 */
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

/**
 * Update chat document
 */
export const updateChatDocument = async (chatDocument: ChatDocument): Promise<ChatDocument> => {
  try {
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

/**
 * Add message to chat
 */
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

/**
 * Add one or more messages by sessionId (avoids relying on partition key at callsite)
 */
export const addMessagesBySessionId = async (
  sessionId: string,
  messages: Message[]
): Promise<ChatDocument> => {
  try {
    console.log("📝 addMessagesBySessionId - sessionId:", sessionId, "messages:", messages.map(m => m.role));
    const chatDocument = await getChatBySessionIdEfficient(sessionId);
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

/**
 * Update a message and truncate all messages after it (used when editing a message)
 */
export const updateMessageAndTruncate = async (
  sessionId: string,
  targetTimestamp: number,
  updatedContent: string
): Promise<ChatDocument> => {
  try {
    console.log("✏️ updateMessageAndTruncate - sessionId:", sessionId, "targetTimestamp:", targetTimestamp);
    const chatDocument = await getChatBySessionIdEfficient(sessionId);
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

/**
 * Get all chats for a user
 */
export const getUserChats = async (username: string): Promise<ChatDocument[]> => {
  try {
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

/**
 * Get chat by session ID (more efficient)
 */
export const getChatBySessionIdEfficient = async (sessionId: string): Promise<ChatDocument | null> => {
  try {
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

/**
 * Get chat by session ID for a specific user (with authorization check)
 */
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

/**
 * Delete chat document
 */
export const deleteChatDocument = async (chatId: string, username: string): Promise<void> => {
  try {
    const containerInstance = await waitForContainer();
    await containerInstance.item(chatId, username).delete();
    console.log(`✅ Deleted chat document: ${chatId}`);
  } catch (error) {
    console.error("❌ Failed to delete chat document:", error);
    throw error;
  }
};

/**
 * Update session fileName by session ID
 */
export const updateSessionFileName = async (
  sessionId: string,
  username: string,
  newFileName: string
): Promise<ChatDocument> => {
  try {
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

/**
 * Delete chat document by session ID
 */
export const deleteSessionBySessionId = async (sessionId: string, username: string): Promise<void> => {
  try {
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
        await containerInstance.item(chatId, pkValue).delete();
        console.log(`✅ Successfully deleted session: ${sessionId} (chatId: ${chatId}, partitionKey: ${pkValue})`);
        return;
      } catch (pkError: any) {
        if (pkError.code === 404) {
          console.log(`   ⚠️ Partition key ${pkValue} didn't work (404), trying next...`);
          continue;
        }
        throw pkError;
      }
    }
    
    throw new Error(`Could not delete document with any partition key value`);
  } catch (error: any) {
    console.error("❌ Failed to delete session by sessionId:", error);
    throw error;
  }
};

/**
 * Get all sessions from CosmosDB container (optionally filtered by username)
 */
export const getAllSessions = async (username?: string): Promise<ChatDocument[]> => {
  try {
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

/**
 * Get all sessions with pagination (optionally filtered by username)
 */
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

/**
 * Get sessions with filtering options
 */
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

/**
 * Get session statistics
 */
export const getSessionStatistics = async (): Promise<{
  totalSessions: number;
  totalUsers: number;
  totalMessages: number;
  totalCharts: number;
  sessionsByUser: Record<string, number>;
  sessionsByDate: Record<string, number>;
}> => {
  try {
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

/**
 * Generate column statistics for numeric columns
 */
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

