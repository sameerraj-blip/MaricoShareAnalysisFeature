/**
 * Data Loader Utility
 * Shared function to load the latest data for a chat document
 * Ensures analysis uses the most up-to-date data including data operation changes
 */
import { ChatDocument } from "../models/chat.model.js";
import { getFileFromBlob } from "../lib/blobStorage.js";
import { parseFile, createDataSummary, convertDashToZeroForNumericColumns } from "../lib/fileParser.js";

/**
 * Normalize data by converting string numbers to actual numbers
 * This ensures numeric columns are properly typed even if stored as strings
 */
function normalizeNumericColumns(data: Record<string, any>[]): Record<string, any>[] {
  if (!data || data.length === 0) return data;
  
  const columns = Object.keys(data[0]);
  const normalizedData = data.map(row => {
    const normalizedRow: Record<string, any> = {};
    for (const [key, value] of Object.entries(row)) {
      // If already a number, keep it
      if (typeof value === 'number' && !isNaN(value) && isFinite(value)) {
        normalizedRow[key] = value;
        continue;
      }
      
      // If null/undefined/empty, keep as is
      if (value === null || value === undefined || value === '') {
        normalizedRow[key] = value;
        continue;
      }
      
      // Try to convert string numbers
      if (typeof value === 'string') {
        const trimmed = value.trim();
        // Skip if it looks like a date (has month names or date separators)
        const lowerStr = trimmed.toLowerCase();
        const monthNames = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
        const hasMonthName = monthNames.some(month => lowerStr.includes(month));
        const hasDateSeparators = /[\/\-]/.test(trimmed) && /\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}/.test(trimmed);
        
        if (hasMonthName || hasDateSeparators) {
          normalizedRow[key] = value; // Keep as string if it's a date
          continue;
        }
        
        // Strip formatting characters
        const cleaned = trimmed.replace(/[%,$€£¥₹\s\u2013\u2014\u2015]/g, '').trim();
        
        if (cleaned === '') {
          normalizedRow[key] = value;
          continue;
        }
        
        // Try to convert to number
        const num = Number(cleaned);
        if (!isNaN(num) && isFinite(num) && cleaned !== '') {
          // Check if it's a pure number (digits with optional decimal and minus)
          if (/^-?\d+\.?\d*$/.test(cleaned)) {
            normalizedRow[key] = num;
          } else {
            normalizedRow[key] = value; // Keep as string if not a pure number
          }
        } else {
          normalizedRow[key] = value; // Keep as string if conversion failed
        }
      } else {
        normalizedRow[key] = value;
      }
    }
    return normalizedRow;
  });
  
  return normalizedData;
}

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
          fullData = normalizeNumericColumns(blobData);
          // Convert "-" to 0 for numeric columns
          const numericColumns = chatDocument.dataSummary?.numericColumns || [];
          fullData = convertDashToZeroForNumericColumns(fullData, numericColumns);
          console.log(`✅ Loaded ${fullData.length} rows from currentDataBlob (modified data)`);
          return fullData;
        }
      } catch {
        // If not JSON, try parsing as CSV/Excel
        const parsedData = await parseFile(blobBuffer, chatDocument.fileName);
        if (parsedData && parsedData.length > 0) {
          fullData = normalizeNumericColumns(parsedData);
          // Convert "-" to 0 for numeric columns
          const numericColumns = chatDocument.dataSummary?.numericColumns || [];
          fullData = convertDashToZeroForNumericColumns(fullData, numericColumns);
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
    fullData = normalizeNumericColumns(chatDocument.rawData);
    // Convert "-" to 0 for numeric columns
    const numericColumns = chatDocument.dataSummary?.numericColumns || [];
    fullData = convertDashToZeroForNumericColumns(fullData, numericColumns);
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
          fullData = normalizeNumericColumns(blobData);
          // Convert "-" to 0 for numeric columns
          const numericColumns = chatDocument.dataSummary?.numericColumns || [];
          fullData = convertDashToZeroForNumericColumns(fullData, numericColumns);
          console.log(`✅ Loaded ${fullData.length} rows from original blob (JSON)`);
          return fullData;
        }
      } catch {
        // If not JSON, try parsing as CSV/Excel
        const parsedData = await parseFile(blobBuffer, chatDocument.fileName);
        if (parsedData && parsedData.length > 0) {
          fullData = normalizeNumericColumns(parsedData);
          // Convert "-" to 0 for numeric columns
          const numericColumns = chatDocument.dataSummary?.numericColumns || [];
          fullData = convertDashToZeroForNumericColumns(fullData, numericColumns);
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

