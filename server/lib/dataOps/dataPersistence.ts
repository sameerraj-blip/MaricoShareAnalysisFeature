/**
 * Data Persistence Module
 * Handles saving modified data to blob storage and updating CosmosDB
 */
import { updateProcessedDataBlob } from '../blobStorage.js';
import { getChatBySessionIdEfficient, updateChatDocument, ChatDocument } from '../../models/chat.model.js';
import { createDataSummary } from '../fileParser.js';
import { generateColumnStatistics } from '../../models/chat.model.js';

export interface SaveDataResult {
  version: number;
  rowsBefore: number;
  rowsAfter: number;
  blobUrl: string;
  blobName: string;
}

// Removed SaveDataOptions - we'll generate preview from rawData instead

/**
 * Save modified data to blob storage and update CosmosDB metadata
 */
export async function saveModifiedData(
  sessionId: string,
  modifiedData: Record<string, any>[],
  operation: string,
  description: string,
  sessionDoc?: ChatDocument
): Promise<SaveDataResult> {
  // Get current document
  const doc = sessionDoc ?? await getChatBySessionIdEfficient(sessionId);
  if (!doc) {
    throw new Error('Session not found');
  }

  // Determine new version
  const currentVersion = doc.currentDataBlob?.version || 1;
  const newVersion = currentVersion + 1;

  // Get username from document
  const username = doc.username;

  // Save new version to blob
  const newBlob = await updateProcessedDataBlob(
    sessionId,
    modifiedData,
    newVersion,
    username
  );

  // Calculate metrics
  const rowsBefore = doc.dataSummary?.rowCount || 0;
  const rowsAfter = modifiedData.length;
  const columnsBefore = doc.dataSummary?.columns?.map(c => c.name) || [];
  const columnsAfter = Object.keys(modifiedData[0] || {});
  const affectedColumns = columnsBefore.filter(c => !columnsAfter.includes(c))
    .concat(columnsAfter.filter(c => !columnsBefore.includes(c)));

  // Update CosmosDB metadata
  doc.currentDataBlob = {
    blobUrl: newBlob.blobUrl,
    blobName: newBlob.blobName,
    version: newVersion,
    lastUpdated: Date.now(),
  };

  // Update sample rows (first 100)
  doc.sampleRows = modifiedData.slice(0, 100).map(row => {
    const serializedRow: Record<string, any> = {};
    for (const [key, value] of Object.entries(row)) {
      if (value instanceof Date) {
        serializedRow[key] = value.toISOString();
      } else {
        serializedRow[key] = value;
      }
    }
    return serializedRow;
  });

  // Validate data before creating summary
  if (!modifiedData || modifiedData.length === 0) {
    throw new Error('Cannot save empty dataset. The data operation resulted in no data.');
  }
  
  // Update data summary
  doc.dataSummary = createDataSummary(modifiedData);

  // Update column statistics
  doc.columnStatistics = generateColumnStatistics(modifiedData, doc.dataSummary.numericColumns);

  // Update rawData in document
  doc.rawData = modifiedData.map(row => {
    const serializedRow: Record<string, any> = {};
    for (const [key, value] of Object.entries(row)) {
      if (value instanceof Date) {
        serializedRow[key] = value.toISOString();
      } else {
        serializedRow[key] = value;
      }
    }
    return serializedRow;
  });

  // Add to version history
  if (!doc.dataVersions) {
    doc.dataVersions = [];
  }
  
  doc.dataVersions.push({
    versionId: `v${newVersion}`,
    blobName: newBlob.blobName,
    operation,
    description,
    timestamp: Date.now(),
    parameters: {
      rowsBefore,
      rowsAfter,
      columnsBefore: columnsBefore.length,
      columnsAfter: columnsAfter.length,
      affectedRows: rowsAfter - rowsBefore,
      affectedColumns: affectedColumns.length > 0 ? affectedColumns : undefined,
    },
    affectedRows: rowsAfter - rowsBefore,
    affectedColumns: affectedColumns.length > 0 ? affectedColumns : undefined,
    rowsBefore,
    rowsAfter,
  });

  // Keep only last 10 versions
  if (doc.dataVersions.length > 10) {
    doc.dataVersions = doc.dataVersions.slice(-10);
  }

  // Update last updated timestamp
  doc.lastUpdatedAt = Date.now();

  // Update document
  await updateChatDocument(doc);

  return {
    version: newVersion,
    rowsBefore,
    rowsAfter,
    blobUrl: newBlob.blobUrl,
    blobName: newBlob.blobName,
  };
}

