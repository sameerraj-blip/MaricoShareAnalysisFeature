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
  data: Record<string, any>[],
  version: number,
  username: string
): Promise<{ blobUrl: string; blobName: string }> => {
  try {
    // Convert data to JSON buffer
    const jsonData = JSON.stringify(data);
    const buffer = Buffer.from(jsonData, 'utf-8');
    
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
        rowCount: data.length.toString(),
      },
    };
    
    // Upload the data
    await blockBlobClient.upload(buffer, buffer.length, uploadOptions);
    
    // Generate the blob URL
    const blobUrl = blockBlobClient.url;
    
    console.log(`✅ Processed data uploaded to blob storage: ${blobName}`);
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
