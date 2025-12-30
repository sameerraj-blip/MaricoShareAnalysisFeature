import { BlobServiceClient, BlockBlobClient, StorageSharedKeyCredential, BlobSASPermissions } from "@azure/storage-blob";

// Azure Blob Storage configuration
const AZURE_STORAGE_ACCOUNT_NAME = process.env.AZURE_STORAGE_ACCOUNT_NAME || "";
const AZURE_STORAGE_ACCOUNT_KEY = process.env.AZURE_STORAGE_ACCOUNT_KEY || "";
const AZURE_STORAGE_CONTAINER_NAME = process.env.AZURE_STORAGE_CONTAINER_NAME || "maricoinsight";

// Create blob service client
const sharedKeyCredential = new StorageSharedKeyCredential(
  AZURE_STORAGE_ACCOUNT_NAME,
  AZURE_STORAGE_ACCOUNT_KEY
);

const blobServiceClient = new BlobServiceClient(
  `https://${AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net`,
  sharedKeyCredential
);

// Get container client
const containerClient = blobServiceClient.getContainerClient(AZURE_STORAGE_CONTAINER_NAME);

// Initialize blob storage
export const initializeBlobStorage = async () => {
  try {
    // Create container if it doesn't exist
    await containerClient.createIfNotExists();
    
    console.log("✅ Azure Blob Storage initialized successfully");
    console.log(`📁 Container: ${AZURE_STORAGE_CONTAINER_NAME}`);
  } catch (error) {
    console.error("❌ Failed to initialize Azure Blob Storage:", error);
    throw error;
  }
};

// Upload file to blob storage
export const uploadFileToBlob = async (
  fileBuffer: Buffer,
  fileName: string,
  username: string,
  contentType?: string
): Promise<{ blobUrl: string; blobName: string }> => {
  try {
    // Create unique blob name with user folder structure
    const timestamp = Date.now();
    const sanitizedUsername = username.replace(/[^a-zA-Z0-9]/g, '_');
    const sanitizedFileName = fileName.replace(/[^a-zA-Z0-9.-]/g, '_');
    const blobName = `${sanitizedUsername}/${timestamp}/${sanitizedFileName}`;
    
    // Get block blob client
    const blockBlobClient = containerClient.getBlockBlobClient(blobName);
    
    // Upload options
    const uploadOptions = {
      blobHTTPHeaders: {
        blobContentType: contentType || getContentTypeFromFileName(fileName),
      },
      metadata: {
        originalFileName: fileName,
        uploadedBy: username,
        uploadedAt: new Date().toISOString(),
      },
    };
    
    // Upload the file
    const uploadResult = await blockBlobClient.upload(
      fileBuffer,
      fileBuffer.length,
      uploadOptions
    );
    
    // Generate the blob URL
    const blobUrl = blockBlobClient.url;
    
    console.log(`✅ File uploaded to blob storage: ${blobName}`);
    console.log(`🔗 Blob URL: ${blobUrl}`);
    
    return {
      blobUrl,
      blobName,
    };
  } catch (error) {
    console.error("❌ Failed to upload file to blob storage:", error);
    throw error;
  }
};

// Get file from blob storage
export const getFileFromBlob = async (blobName: string): Promise<Buffer> => {
  try {
    const blockBlobClient = containerClient.getBlockBlobClient(blobName);
    const downloadResponse = await blockBlobClient.download();
    
    if (!downloadResponse.readableStreamBody) {
      throw new Error("No readable stream body found");
    }
    
    // Convert stream to buffer
    const chunks: Buffer[] = [];
    for await (const chunk of downloadResponse.readableStreamBody) {
      chunks.push(Buffer.from(chunk));
    }
    
    return Buffer.concat(chunks);
  } catch (error) {
    console.error("❌ Failed to get file from blob storage:", error);
    throw error;
  }
};

// Delete file from blob storage
export const deleteFileFromBlob = async (blobName: string): Promise<void> => {
  try {
    const blockBlobClient = containerClient.getBlockBlobClient(blobName);
    await blockBlobClient.delete();
    
    console.log(`✅ File deleted from blob storage: ${blobName}`);
  } catch (error) {
    console.error("❌ Failed to delete file from blob storage:", error);
    throw error;
  }
};

// List files for a user
export const listUserFiles = async (username: string): Promise<Array<{
  blobName: string;
  blobUrl: string;
  lastModified: Date;
  size: number;
  metadata: Record<string, string>;
}>> => {
  try {
    const sanitizedUsername = username.replace(/[^a-zA-Z0-9]/g, '_');
    const files: Array<{
      blobName: string;
      blobUrl: string;
      lastModified: Date;
      size: number;
      metadata: Record<string, string>;
    }> = [];
    
    // List blobs with prefix (user folder)
    for await (const blob of containerClient.listBlobsFlat({
      prefix: `${sanitizedUsername}/`,
    })) {
      const blockBlobClient = containerClient.getBlockBlobClient(blob.name);
      
      files.push({
        blobName: blob.name,
        blobUrl: blockBlobClient.url,
        lastModified: blob.properties.lastModified || new Date(),
        size: blob.properties.contentLength || 0,
        metadata: blob.metadata || {},
      });
    }
    
    return files;
  } catch (error) {
    console.error("❌ Failed to list user files:", error);
    throw error;
  }
};

// Helper function to get content type from file name
const getContentTypeFromFileName = (fileName: string): string => {
  const extension = fileName.toLowerCase().split('.').pop();
  
  switch (extension) {
    case 'csv':
      return 'text/csv';
    case 'xlsx':
      return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
    case 'xls':
      return 'application/vnd.ms-excel';
    case 'json':
      return 'application/json';
    case 'txt':
      return 'text/plain';
    default:
      return 'application/octet-stream';
  }
};

// Generate SAS URL for temporary access (optional)
export const generateSasUrl = async (
  blobName: string,
  expiresInMinutes: number = 60
): Promise<string> => {
  try {
    const blockBlobClient = containerClient.getBlockBlobClient(blobName);
    
    // Generate SAS token
    const sasUrl = await blockBlobClient.generateSasUrl({
      permissions: BlobSASPermissions.parse('r'), // Read permission
      expiresOn: new Date(Date.now() + expiresInMinutes * 60 * 1000),
    });
    
    return sasUrl;
  } catch (error) {
    console.error("❌ Failed to generate SAS URL:", error);
    throw error;
  }
};

// Update processed data blob (for data operations)
export const updateProcessedDataBlob = async (
  sessionId: string,
  data: Record<string, any>[] | Buffer,
  version: number,
  username: string
): Promise<{ blobUrl: string; blobName: string }> => {
  try {
    let buffer: Buffer;
    let rowCount: number;
    
    // Handle both data array and pre-serialized buffer
    if (Buffer.isBuffer(data)) {
      buffer = data;
      // Try to extract row count from buffer if possible, otherwise estimate
      try {
        const parsed = JSON.parse(buffer.toString('utf-8'));
        rowCount = Array.isArray(parsed) ? parsed.length : 0;
      } catch {
        // If we can't parse, estimate based on size (rough estimate)
        rowCount = Math.floor(buffer.length / 1000); // Rough estimate
      }
    } else {
      // Convert data to JSON buffer
      const isLargeDataset = data.length > 50000;
      
      if (isLargeDataset) {
        console.log(`📦 Large dataset detected (${data.length} rows). Serializing in chunks for blob upload...`);
        // For very large datasets, serialize in chunks to avoid memory issues
        const chunks: string[] = [];
        chunks.push('[');
        
        for (let i = 0; i < data.length; i++) {
          if (i > 0) chunks.push(',');
          chunks.push(JSON.stringify(data[i]));
          
          // Log progress for very large datasets
          if ((i + 1) % 10000 === 0) {
            console.log(`  Serialized ${i + 1} / ${data.length} rows...`);
          }
        }
        
        chunks.push(']');
        const jsonData = chunks.join('');
        buffer = Buffer.from(jsonData, 'utf-8');
        rowCount = data.length;
      } else {
        // Small dataset - serialize normally
        const jsonData = JSON.stringify(data);
        buffer = Buffer.from(jsonData, 'utf-8');
        rowCount = data.length;
      }
    }
    
    // Create blob name for processed data
    const sanitizedUsername = username.replace(/[^a-zA-Z0-9]/g, '_');
    const blobName = `${sanitizedUsername}/processed/${sessionId}/v${version}.json`;
    
    // Get block blob client
    const blockBlobClient = containerClient.getBlockBlobClient(blobName);
    
    // Upload options
    const uploadOptions = {
      blobHTTPHeaders: {
        blobContentType: 'application/json',
      },
      metadata: {
        sessionId,
        version: version.toString(),
        processedBy: username,
        processedAt: new Date().toISOString(),
        rowCount: rowCount.toString(),
        sizeBytes: buffer.length.toString(),
      },
    };
    
    // Upload the data
    // Azure Blob Storage handles large files efficiently, but we log progress for very large uploads
    const sizeMB = buffer.length / (1024 * 1024);
    if (sizeMB > 50) {
      console.log(`📤 Uploading large blob (${sizeMB.toFixed(2)} MB) to Azure Blob Storage...`);
    }
    
    await blockBlobClient.upload(buffer, buffer.length, uploadOptions);
    
    // Generate the blob URL
    const blobUrl = blockBlobClient.url;
    
    console.log(`✅ Processed data uploaded to blob storage: ${blobName} (${rowCount} rows, ${sizeMB.toFixed(2)} MB)`);
    console.log(`🔗 Blob URL: ${blobUrl}`);
    
    return {
      blobUrl,
      blobName,
    };
  } catch (error) {
    console.error("❌ Failed to update processed data blob:", error);
    throw error;
  }
};

// Chart storage interface
export interface ChartReference {
  chartId: string;
  blobName: string;
  blobUrl: string;
  title: string;
  type: string;
  createdAt: number;
}

// Save charts to blob storage
export const saveChartsToBlob = async (
  sessionId: string,
  charts: any[],
  username: string
): Promise<ChartReference[]> => {
  try {
    if (!charts || charts.length === 0) {
      return [];
    }

    const sanitizedUsername = username.replace(/[^a-zA-Z0-9]/g, '_');
    const chartReferences: ChartReference[] = [];
    const timestamp = Date.now();

    // Save each chart individually to blob storage
    for (let i = 0; i < charts.length; i++) {
      const chart = charts[i];
      const chartId = `chart_${timestamp}_${i}`;
      const blobName = `${sanitizedUsername}/charts/${sessionId}/${chartId}.json`;

      // Serialize chart data
      const chartData = JSON.stringify(chart);
      const buffer = Buffer.from(chartData, 'utf-8');

      // Get block blob client
      const blockBlobClient = containerClient.getBlockBlobClient(blobName);

      // Upload options
      const uploadOptions = {
        blobHTTPHeaders: {
          blobContentType: 'application/json',
        },
        metadata: {
          sessionId,
          chartId,
          chartTitle: chart.title || 'Untitled Chart',
          chartType: chart.type || 'unknown',
          savedBy: username,
          savedAt: new Date().toISOString(),
        },
      };

      // Upload the chart
      await blockBlobClient.upload(buffer, buffer.length, uploadOptions);
      const blobUrl = blockBlobClient.url;

      chartReferences.push({
        chartId,
        blobName,
        blobUrl,
        title: chart.title || 'Untitled Chart',
        type: chart.type || 'unknown',
        createdAt: timestamp,
      });
    }

    console.log(`✅ Saved ${chartReferences.length} charts to blob storage for session ${sessionId}`);
    return chartReferences;
  } catch (error) {
    console.error("❌ Failed to save charts to blob storage:", error);
    throw error;
  }
};

// Load charts from blob storage
export const loadChartsFromBlob = async (
  chartReferences: ChartReference[]
): Promise<any[]> => {
  try {
    if (!chartReferences || chartReferences.length === 0) {
      return [];
    }

    const charts: any[] = [];

    // Load each chart from blob storage
    for (const ref of chartReferences) {
      try {
        const blobBuffer = await getFileFromBlob(ref.blobName);
        const chartData = JSON.parse(blobBuffer.toString('utf-8'));
        charts.push(chartData);
      } catch (error) {
        console.error(`⚠️ Failed to load chart ${ref.chartId} from blob:`, error);
        // Continue loading other charts even if one fails
      }
    }

    console.log(`✅ Loaded ${charts.length} charts from blob storage`);
    return charts;
  } catch (error) {
    console.error("❌ Failed to load charts from blob storage:", error);
    throw error;
  }
};

// Delete charts from blob storage
export const deleteChartsFromBlob = async (
  chartReferences: ChartReference[]
): Promise<void> => {
  try {
    if (!chartReferences || chartReferences.length === 0) {
      return;
    }

    for (const ref of chartReferences) {
      try {
        await deleteFileFromBlob(ref.blobName);
      } catch (error) {
        console.error(`⚠️ Failed to delete chart ${ref.chartId} from blob:`, error);
        // Continue deleting other charts even if one fails
      }
    }

    console.log(`✅ Deleted ${chartReferences.length} charts from blob storage`);
  } catch (error) {
    console.error("❌ Failed to delete charts from blob storage:", error);
    throw error;
  }
};
