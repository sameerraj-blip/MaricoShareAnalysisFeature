/**
 * Data Loader Utility
 * Shared function to load the latest data for a chat document
 * Ensures analysis uses the most up-to-date data including data operation changes
 */
import { ChatDocument } from "../models/chat.model.js";
import { getFileFromBlob } from "../lib/blobStorage.js";
import { parseFile } from "../lib/fileParser.js";

/**
 * Load the latest data for a chat document
 * This function ensures that data operations performed by any user are reflected in analysis
 * Priority:
 * 1. currentDataBlob (modified data from data operations)
 * 2. rawData from document (if no blob exists)
 * 3. original blob (if rawData is not available)
 * 4. sampleRows (fallback)
 */
export async function loadLatestData(chatDocument: ChatDocument): Promise<Record<string, any>[]> {
  let fullData: Record<string, any>[] = [];
  
  console.log(`🔍 Loading latest data for session ${chatDocument.sessionId}`);
  console.log(`   - rawData: ${chatDocument.rawData?.length || 0} rows`);
  console.log(`   - sampleRows: ${chatDocument.sampleRows?.length || 0} rows`);
  console.log(`   - currentDataBlob: ${chatDocument.currentDataBlob?.blobName || 'none'}`);
  console.log(`   - original blob: ${chatDocument.blobInfo?.blobName || 'none'}`);
  
  // Priority 1: Try to load from currentDataBlob (modified data from data operations)
  // This ensures we get the latest data including any transformations
  if (chatDocument.currentDataBlob?.blobName) {
    try {
      console.log(`📦 Attempting to load from currentDataBlob: ${chatDocument.currentDataBlob.blobName}`);
      const blobBuffer = await getFileFromBlob(chatDocument.currentDataBlob.blobName);
      
      // Try parsing as JSON first (processed data from data operations)
      try {
        const blobData = JSON.parse(blobBuffer.toString('utf-8'));
        if (Array.isArray(blobData) && blobData.length > 0) {
          fullData = blobData;
          console.log(`✅ Loaded ${fullData.length} rows from currentDataBlob (modified data)`);
          return fullData;
        }
      } catch {
        // If not JSON, try parsing as CSV/Excel
        const parsedData = await parseFile(blobBuffer, chatDocument.fileName);
        if (parsedData && parsedData.length > 0) {
          fullData = parsedData;
          console.log(`✅ Loaded ${fullData.length} rows from currentDataBlob (parsed file)`);
          return fullData;
        }
      }
    } catch (error) {
      console.error('⚠️ Failed to load from currentDataBlob, trying other sources:', error);
    }
  }
  
  // Priority 2: Use rawData from document (should be up-to-date after data operations)
  if (chatDocument.rawData && Array.isArray(chatDocument.rawData) && chatDocument.rawData.length > 0) {
    fullData = chatDocument.rawData;
    console.log(`✅ Using rawData from document: ${fullData.length} rows`);
    return fullData;
  }
  
  // Priority 3: Try to load from original blob storage
  if (chatDocument.blobInfo?.blobName) {
    try {
      console.log(`📦 Attempting to load from original blob: ${chatDocument.blobInfo.blobName}`);
      const blobBuffer = await getFileFromBlob(chatDocument.blobInfo.blobName);
      
      // Try parsing as JSON first
      try {
        const blobData = JSON.parse(blobBuffer.toString('utf-8'));
        if (Array.isArray(blobData) && blobData.length > 0) {
          fullData = blobData;
          console.log(`✅ Loaded ${fullData.length} rows from original blob (JSON)`);
          return fullData;
        }
      } catch {
        // If not JSON, try parsing as CSV/Excel
        const parsedData = await parseFile(blobBuffer, chatDocument.fileName);
        if (parsedData && parsedData.length > 0) {
          fullData = parsedData;
          console.log(`✅ Loaded ${fullData.length} rows from original blob (parsed file)`);
          return fullData;
        }
      }
    } catch (error) {
      console.error('⚠️ Failed to load from original blob:', error);
    }
  }
  
  // Priority 4: Fallback to sampleRows (limited data)
  if (chatDocument.sampleRows && Array.isArray(chatDocument.sampleRows) && chatDocument.sampleRows.length > 0) {
    fullData = chatDocument.sampleRows;
    console.log(`⚠️ Using sampleRows as fallback: ${fullData.length} rows (limited data)`);
    return fullData;
  }
  
  throw new Error('No data found. Please upload your file again.');
}

