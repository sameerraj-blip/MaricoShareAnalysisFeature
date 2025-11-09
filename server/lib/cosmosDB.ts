import { CosmosClient, Database, Container } from "@azure/cosmos";
import { ChartSpec, Message, DataSummary, Insight, Dashboard } from "../../shared/schema.js";

// CosmosDB configuration
const COSMOS_ENDPOINT = process.env.COSMOS_ENDPOINT || "";
const COSMOS_KEY = process.env.COSMOS_KEY || "";
const COSMOS_DATABASE_ID = process.env.COSMOS_DATABASE_ID || "marico-insights";
const COSMOS_CONTAINER_ID = process.env.COSMOS_CONTAINER_ID || "chats";
const COSMOS_DASHBOARDS_CONTAINER_ID = process.env.COSMOS_DASHBOARDS_CONTAINER_ID || "dashboards";

// Initialize CosmosDB client
const client = new CosmosClient({
  endpoint: COSMOS_ENDPOINT,
  key: COSMOS_KEY,
});

let database: Database;
let container: Container;
let dashboardsContainer: Container;

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

    console.log("CosmosDB initialized successfully");
  } catch (error) {
    console.error("Failed to initialize CosmosDB:", error);
    throw error;
  }
};

// Chat document interface
export interface ChatDocument {
  id: string; // Unique chat ID (fileName + timestamp)
  username: string; // User email
  fileName: string; // Original uploaded file name
  uploadedAt: number; // Upload timestamp
  createdAt: number; // Chat creation timestamp
  lastUpdatedAt: number; // Last update timestamp
  dataSummary: DataSummary; // Data summary from file upload
  messages: Message[]; // Chat messages with charts and insights
  charts: ChartSpec[]; // All charts generated for this chat
  insights: Insight[]; // AI-generated insights from data analysis
  sessionId: string; // Original session ID
  // Enhanced analysis data storage
  rawData: Record<string, any>[]; // Complete raw data from uploaded file
  sampleRows: Record<string, any>[]; // Sample rows for preview (first 10)
  columnStatistics: Record<string, any>; // Statistical analysis of numeric columns
  blobInfo?: { // Azure Blob Storage information
    blobUrl: string;
    blobName: string;
  };
  analysisMetadata: { // Additional metadata about the analysis
    totalProcessingTime: number; // Time taken to process the file
    aiModelUsed: string; // AI model used for analysis
    fileSize: number; // Original file size in bytes
    analysisVersion: string; // Version of analysis algorithm
  };
}

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
  const chatId = `${fileName.replace(/[^a-zA-Z0-9]/g, '_')}_${timestamp}`;
  
  const chatDocument: ChatDocument & { fsmrora?: string } = {
    id: chatId,
    username,
    fsmrora: username, // Add partition key field to match partition key path /fsmrora
    fileName,
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
    analysisMetadata: analysisMetadata || {
      totalProcessingTime: 0,
      aiModelUsed: 'gpt-4o',
      fileSize: 0,
      analysisVersion: '1.0.0'
    }
  };

  try {
    if (!container) {
      throw new Error("CosmosDB container not initialized. Make sure initializeCosmosDB() was called successfully.");
    }
    
    const { resource } = await container.items.create(chatDocument);
    return resource as ChatDocument;
  } catch (error) {
    console.error("Failed to create chat document:", error);
    throw error;
  }
};

// Get chat document by ID
export const getChatDocument = async (chatId: string, username: string): Promise<ChatDocument | null> => {
  try {
    if (!container) {
      return null;
    }
    
    const { resource } = await container.item(chatId, username).read();
    return resource;
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
    chatDocument.lastUpdatedAt = Date.now();
    const { resource } = await container.items.upsert(chatDocument);
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

// Get all chats for a user
export const getUserChats = async (username: string): Promise<ChatDocument[]> => {
  try {
    const query = "SELECT * FROM c WHERE c.username = @username ORDER BY c.createdAt DESC";
    const { resources } = await container.items.query({
      query,
      parameters: [{ name: "@username", value: username }]
    }).fetchAll();
    
    return resources;
  } catch (error) {
    console.error("❌ Failed to get user chats:", error);
    throw error;
  }
};

// Get chat by session ID (more efficient)
export const getChatBySessionIdEfficient = async (sessionId: string): Promise<ChatDocument | null> => {
  try {
    if (!container) {
      throw new Error("CosmosDB container not initialized. Please wait for initialization to complete.");
    }
    
    const query = "SELECT * FROM c WHERE c.sessionId = @sessionId";
    const { resources } = await container.items.query({
      query,
      parameters: [{ name: "@sessionId", value: sessionId }]
    }).fetchAll();
    const doc = (resources && resources.length > 0) ? resources[0] : null;
    if (!doc) {
      console.warn("⚠️ No chat document found for sessionId:", sessionId);
    } else {
      console.log("🔎 Found chat document by sessionId:", doc.id, "username:", doc.username);
    }
    return doc as unknown as ChatDocument | null;
  } catch (error) {
    console.error("❌ Failed to get chat by session ID:", error);
    throw error;
  }
};

// Delete chat document
export const deleteChatDocument = async (chatId: string, username: string): Promise<void> => {
  try {
    if (!container) {
      throw new Error("CosmosDB container not initialized. Please wait for initialization to complete.");
    }
    
    await container.item(chatId, username).delete();
    console.log(`✅ Deleted chat document: ${chatId}`);
  } catch (error) {
    console.error("❌ Failed to delete chat document:", error);
    throw error;
  }
};

// Delete chat document by session ID
export const deleteSessionBySessionId = async (sessionId: string, username: string): Promise<void> => {
  try {
    // Wait for container to be initialized (with timeout)
    let retries = 0;
    const maxRetries = 10;
    const retryDelay = 500; // 500ms
    
    while (!container && retries < maxRetries) {
      await new Promise(resolve => setTimeout(resolve, retryDelay));
      retries++;
    }
    
    if (!container) {
      throw new Error("CosmosDB container not initialized. Please wait for initialization to complete.");
    }
    
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
      let partitionKeyValue: string | undefined;
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
          const testRead = await container.item(chatId, pkValue).read();
          console.log(`   ✅ Verified partition key: ${pkValue}`);
          
          // Now delete with the verified partition key
          await container.item(chatId, pkValue).delete();
          console.log(`✅ Successfully deleted session: ${sessionId} (chatId: ${chatId}, partitionKey: ${pkValue})`);
          deleteSuccess = true;
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
        const queryResult = await container.items.query({
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
            await container.item(chatId, actualPartitionKey).delete();
            console.log(`✅ Successfully deleted using partition key from query`);
            deleteSuccess = true;
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
                await container.item(chatId, partitionKeyForUpdate).replace(docToUpdate);
                console.log(`   ✅ Updated document with fsmrora field`);
                
                // Now try to delete with the updated partition key
                await container.item(chatId, partitionKeyForUpdate).delete();
                console.log(`✅ Successfully deleted after updating fsmrora field`);
                deleteSuccess = true;
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
      
      // Verify deletion
      try {
        const verifyDoc = await container.item(chatId, possiblePartitionKeys[0]).read();
        console.warn(`⚠️ Warning: Document still exists after deletion attempt. ChatId: ${chatId}`);
        throw new Error(`Deletion failed - document still exists in database`);
      } catch (verifyError: any) {
        if (verifyError.code === 404 || verifyError.statusCode === 404) {
          console.log(`   ✅ Verified: Document no longer exists - deletion successful`);
          return; // Success
        }
        throw verifyError;
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
  const timestamp = Date.now();
  const id = `${name.replace(/[^a-zA-Z0-9]/g, '_')}_${timestamp}`;
  const dashboard: Dashboard = {
    id,
    username,
    name,
    createdAt: timestamp,
    updatedAt: timestamp,
    charts,
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
    return resource as unknown as Dashboard;
  } catch (error: any) {
    if (error.code === 404) return null;
    throw error;
  }
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
  chart: ChartSpec
): Promise<Dashboard> => {
  const dashboard = await getDashboardById(id, username);
  if (!dashboard) throw new Error("Dashboard not found");
  dashboard.charts.push(chart);
  return updateDashboard(dashboard);
};

export const removeChartFromDashboard = async (
  id: string,
  username: string,
  predicate: { index?: number; title?: string; type?: ChartSpec["type"] }
): Promise<Dashboard> => {
  const dashboard = await getDashboardById(id, username);
  if (!dashboard) throw new Error("Dashboard not found");

  if (typeof predicate.index === 'number') {
    dashboard.charts.splice(predicate.index, 1);
  } else if (predicate.title || predicate.type) {
    dashboard.charts = dashboard.charts.filter(c => {
      const titleMatch = predicate.title ? c.title !== predicate.title : true;
      const typeMatch = predicate.type ? c.type !== predicate.type : true;
      return titleMatch || typeMatch;
    });
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
    let query = "SELECT * FROM c";
    const parameters: Array<{ name: string; value: any }> = [];
    
    // Add username filter if provided
    if (username) {
      query += " WHERE c.username = @username";
      parameters.push({ name: "@username", value: username });
    }
    
    query += " ORDER BY c.createdAt DESC";
    
    const queryOptions = parameters.length > 0 ? { parameters } : {};
    const { resources } = await container.items.query({
      query,
      ...queryOptions,
    }).fetchAll();
    
    console.log(`✅ Retrieved ${resources.length} sessions from CosmosDB${username ? ` for user: ${username}` : ''}`);
    return resources;
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
    let query = "SELECT * FROM c";
    const parameters: Array<{ name: string; value: any }> = [];
    
    // Add username filter if provided
    if (username) {
      query += " WHERE c.username = @username";
      parameters.push({ name: "@username", value: username });
    }
    
    query += " ORDER BY c.createdAt DESC";
    
    const queryOptions = {
      maxItemCount: pageSize,
      continuationToken,
      ...(parameters.length > 0 && { parameters }),
    };
    
    const { resources, continuationToken: nextToken, hasMoreResults } = await container.items.query({
      query,
    }, queryOptions).fetchNext();
    
    console.log(`✅ Retrieved ${resources.length} sessions (page size: ${pageSize})${username ? ` for user: ${username}` : ''}`);
    
    return {
      sessions: resources,
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
    let query = "SELECT * FROM c WHERE 1=1";
    const parameters: Array<{ name: string; value: any }> = [];
    
    // Add filters based on options
    if (options.username) {
      query += " AND c.username = @username";
      parameters.push({ name: "@username", value: options.username });
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
    
    const { resources } = await container.items.query({
      query,
      parameters,
    }, queryOptions).fetchAll();
    
    console.log(`✅ Retrieved ${resources.length} sessions with filters`);
    return resources;
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
