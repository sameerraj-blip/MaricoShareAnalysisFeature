/**
 * Data Loader Utility
 * Shared function to load the latest data for a chat document
 * Ensures analysis uses the most up-to-date data including data operation changes
 */
import { ChatDocument } from "../models/chat.model.js";
import { getFileFromBlob } from "../lib/blobStorage.js";
import { parseFile, createDataSummary, convertDashToZeroForNumericColumns } from "../lib/fileParser.js";
import { getDataForAnalysis } from "../lib/largeFileProcessor.js";

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
 * Filter data to only include required columns
 * This reduces memory usage for large datasets
 */
function filterColumns(
  data: Record<string, any>[],
  requiredColumns: string[]
): Record<string, any>[] {
  if (!data || data.length === 0) return data;
  if (!requiredColumns || requiredColumns.length === 0) return data;
  
  // Get all available columns from first row
  const availableColumns = Object.keys(data[0] || {});
  
  // Find matching columns (case-insensitive, handle whitespace)
  const matchedColumns = requiredColumns
    .map(reqCol => {
      const reqLower = reqCol.toLowerCase().trim();
      return availableColumns.find(
        availCol => availCol.toLowerCase().trim() === reqLower
      ) || reqCol; // Keep original if no match found
    })
    .filter(col => availableColumns.includes(col)); // Only keep columns that exist
  
  if (matchedColumns.length === 0) {
    // No matches found, return all data (fallback)
    console.warn(`⚠️ No matching columns found for required columns: ${requiredColumns.join(', ')}. Returning all data.`);
    return data;
  }
  
  // Filter each row to only include matched columns
  return data.map(row => {
    const filteredRow: Record<string, any> = {};
    matchedColumns.forEach(col => {
      if (row[col] !== undefined) {
        filteredRow[col] = row[col];
      }
    });
    return filteredRow;
  });
}

/**
 * Load the latest data for a chat document
 * This function ensures that data operations performed by any user are reflected in analysis
 * Priority:
 * 1. currentDataBlob (modified data from data operations)
 * 2. rawData from document (if no blob exists)
 * 3. original blob (if rawData is not available)
 * 4. sampleRows (fallback)
 * 
 * @param chatDocument - The chat document to load data from
 * @param requiredColumns - Optional array of column names to filter. If provided and dataset is large (>10k rows), only these columns will be returned.
 */
export async function loadLatestData(
  chatDocument: ChatDocument,
  requiredColumns?: string[]
): Promise<Record<string, any>[]> {
  let fullData: Record<string, any>[] = [];
  
  console.log(`🔍 Loading latest data for session ${chatDocument.sessionId}`);
  console.log(`   - rawData: ${chatDocument.rawData?.length || 0} rows`);
  console.log(`   - sampleRows: ${chatDocument.sampleRows?.length || 0} rows`);
  console.log(`   - currentDataBlob: ${chatDocument.currentDataBlob?.blobName || 'none'}`);
  console.log(`   - original blob: ${chatDocument.blobInfo?.blobName || 'none'}`);
  console.log(`   - columnarStorage: ${(chatDocument as any).columnarStoragePath || 'none'}`);
  
  // Priority 0: For large files, use columnar storage
  if ((chatDocument as any).columnarStoragePath) {
    try {
      console.log(`📊 Loading from columnar storage for large file...`);
      // For large files, get sampled/aggregated data instead of full dataset
      const limit = requiredColumns && requiredColumns.length > 0 ? 50000 : 10000;
      fullData = await getDataForAnalysis(chatDocument.sessionId, requiredColumns, limit);
      
      // Normalize numeric columns
      fullData = normalizeNumericColumns(fullData);
      const numericColumns = chatDocument.dataSummary?.numericColumns || [];
      fullData = convertDashToZeroForNumericColumns(fullData, numericColumns);
      
      console.log(`✅ Loaded ${fullData.length} rows from columnar storage`);
      return fullData;
    } catch (error) {
      console.error('⚠️ Failed to load from columnar storage, trying other sources:', error);
      // Fall through to other methods
    }
  }
  
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
          
          // Apply column filtering for large datasets
          if (requiredColumns && requiredColumns.length > 0 && fullData.length > 10000) {
            const beforeFilter = fullData.length;
            fullData = filterColumns(fullData, requiredColumns);
            console.log(`📊 Filtered to ${requiredColumns.length} columns (${beforeFilter} rows)`);
          }
          
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
          
          // Apply column filtering for large datasets
          if (requiredColumns && requiredColumns.length > 0 && fullData.length > 10000) {
            const beforeFilter = fullData.length;
            fullData = filterColumns(fullData, requiredColumns);
            console.log(`📊 Filtered to ${requiredColumns.length} columns (${beforeFilter} rows)`);
          }
          
          return fullData;
        }
      }
    } catch (error) {
      console.error('⚠️ Failed to load from currentDataBlob, trying other sources:', error);
    }
  }
  
  // Priority 2: Use rawData from document (should be up-to-date after data operations)
  // Note: For large datasets, rawData might be empty in CosmosDB (stored only in blob)
  if (chatDocument.rawData && Array.isArray(chatDocument.rawData) && chatDocument.rawData.length > 0) {
    fullData = normalizeNumericColumns(chatDocument.rawData);
    // Convert "-" to 0 for numeric columns
    const numericColumns = chatDocument.dataSummary?.numericColumns || [];
    fullData = convertDashToZeroForNumericColumns(fullData, numericColumns);
    console.log(`✅ Using rawData from document: ${fullData.length} rows`);
    
    // Apply column filtering for large datasets
    if (requiredColumns && requiredColumns.length > 0 && fullData.length > 10000) {
      const beforeFilter = fullData.length;
      fullData = filterColumns(fullData, requiredColumns);
      console.log(`📊 Filtered to ${requiredColumns.length} columns (${beforeFilter} rows)`);
    }
    
    return fullData;
  }
  
  // If rawData is empty in document but we have blob storage, it means the dataset was too large
  // This is expected for large files - we'll load from blob instead
  
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
          
          // Apply column filtering for large datasets
          if (requiredColumns && requiredColumns.length > 0 && fullData.length > 10000) {
            const beforeFilter = fullData.length;
            fullData = filterColumns(fullData, requiredColumns);
            console.log(`📊 Filtered to ${requiredColumns.length} columns (${beforeFilter} rows)`);
          }
          
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
          
          // Apply column filtering for large datasets
          if (requiredColumns && requiredColumns.length > 0 && fullData.length > 10000) {
            const beforeFilter = fullData.length;
            fullData = filterColumns(fullData, requiredColumns);
            console.log(`📊 Filtered to ${requiredColumns.length} columns (${beforeFilter} rows)`);
          }
          
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

/**
 * Load data for specific columns only
 * This is a convenience function that loads data and filters to required columns
 * Use this when you know exactly which columns you need
 */
export async function loadDataForColumns(
  chatDocument: ChatDocument,
  requiredColumns: string[]
): Promise<Record<string, any>[]> {
  if (!requiredColumns || requiredColumns.length === 0) {
    return loadLatestData(chatDocument);
  }
  
  return loadLatestData(chatDocument, requiredColumns);
}

