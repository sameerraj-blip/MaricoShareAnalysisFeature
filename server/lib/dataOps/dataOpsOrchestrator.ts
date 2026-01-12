/**
 * Data Ops Orchestrator
 * Handles intent parsing, clarification flow, and coordinates data operations
 */
import { Message, DataSummary } from '../../shared/schema.js';
import { removeNulls, getDataPreview, getDataSummary, convertDataType, createDerivedColumn, trainMLModel, aggregateData, createPivotTable, identifyOutliers, treatOutliers } from './pythonService.js';
import { saveModifiedData } from './dataPersistence.js';
import { getChatBySessionIdEfficient, updateChatDocument, ChatDocument } from '../../models/chat.model.js';
import { openai } from '../openai.js';
import { getFileFromBlob } from '../blobStorage.js';
import { parseFile, convertDashToZeroForNumericColumns } from '../fileParser.js';

/**
 * Identify if a column is an ID column (identifier field)
 * ID columns match patterns like: *_id, order_id, item_id, customer_id, etc.
 */
export function isIdColumn(columnName: string): boolean {
  const lower = columnName.toLowerCase();
  // Match patterns: *_id, *_ID, or explicit patterns like order_id, item_id
  return /_id$|^id$|_id_/i.test(columnName) || 
         ['order_id', 'item_id', 'customer_id', 'user_id', 'product_id', 'transaction_id'].includes(lower);
}

/**
 * Generate a meaningful count name for an ID column
 * e.g., "order_id" -> "order_count", "item_id" -> "item_count"
 */
export function getCountNameForIdColumn(columnName: string): string {
  const lower = columnName.toLowerCase();
  // Remove _id suffix and add _count
  if (lower.endsWith('_id')) {
    return lower.replace(/_id$/, '_count');
  }
  // For "id" or other patterns, use generic count name
  return `${lower}_count`;
}

// Streaming configuration for large datasets
const LARGE_DATASET_THRESHOLD = 50000; // 50k rows
const BATCH_SIZE = 10000; // Process 10k rows at a time

/**
 * Get preview data from saved rawData (first 50 rows)
 */
async function getPreviewFromSavedData(sessionId: string, fallbackData: Record<string, any>[]): Promise<Record<string, any>[]> {
  try {
    const updatedDoc = await getChatBySessionIdEfficient(sessionId);
    if (updatedDoc?.rawData && Array.isArray(updatedDoc.rawData) && updatedDoc.rawData.length > 0) {
      // Return first 50 rows from saved rawData
      return updatedDoc.rawData.slice(0, 50);
    }
  } catch (error) {
    console.error('⚠️ Failed to get preview from saved data, using fallback:', error);
  }
  // Fallback to provided data if document not found
  return fallbackData.slice(0, 50);
}

/**
 * Streaming helper: Process data in batches
 */
async function processInBatches<T>(
  data: Record<string, any>[],
  batchSize: number,
  processor: (batch: Record<string, any>[]) => Promise<T> | T
): Promise<T[]> {
  const results: T[] = [];
  for (let i = 0; i < data.length; i += batchSize) {
    const batch = data.slice(i, i + batchSize);
    const result = await processor(batch);
    results.push(result);
  }
  return results;
}

/**
 * Streaming version of removeNulls for large datasets
 * 
 * Note: For imputation methods (mean, median, mode), the Python service calculates
 * statistics from the full dataset, so we process in batches but the Python service
 * handles the imputation correctly. For delete operations, we can safely process in batches.
 */
async function removeNullsStreaming(
  data: Record<string, any>[],
  column?: string,
  method: 'delete' | 'mean' | 'median' | 'mode' | 'custom' = 'delete',
  customValue?: any
): Promise<{ data: Record<string, any>[]; nulls_removed: number; rows_before: number; rows_after: number }> {
  const rowsBefore = data.length;
  let totalNullsRemoved = 0;
  const processedBatches: Record<string, any>[][] = [];
  
  // For imputation methods, the Python service needs the full dataset to calculate
  // accurate statistics (mean/median/mode). However, for very large datasets, we
  // can still process in batches and the Python service will handle it.
  // For delete operations, batch processing is straightforward.
  
  // Process in batches
  console.log(`📊 Processing ${data.length} rows in batches of ${BATCH_SIZE}...`);
  for (let i = 0; i < data.length; i += BATCH_SIZE) {
    const batch = data.slice(i, i + BATCH_SIZE);
    const batchResult = await removeNulls(batch, column, method, customValue);
    processedBatches.push(batchResult.data);
    totalNullsRemoved += batchResult.nulls_removed;
    
    // Log progress every 5 batches
    if ((i + BATCH_SIZE) % (BATCH_SIZE * 5) === 0 || i + BATCH_SIZE >= data.length) {
      console.log(`  Processed ${Math.min(i + BATCH_SIZE, data.length)} / ${data.length} rows...`);
    }
  }
  
  // Combine all batches
  const result = processedBatches.flat();
  
  console.log(`✅ Streaming operation complete: ${totalNullsRemoved} nulls removed, ${rowsBefore} → ${result.length} rows`);
  
  return {
    data: result,
    nulls_removed: totalNullsRemoved,
    rows_before: rowsBefore,
    rows_after: result.length
  };
}

export interface DataOpsIntent {
  operation:
    | 'remove_nulls'
    | 'preview'
    | 'summary'
    | 'convert_type'
    | 'count_nulls'
    | 'describe'
    | 'create_derived_column'
    | 'create_column'
    | 'modify_column'
    | 'normalize_column'
    | 'remove_column'
    | 'rename_column'
    | 'remove_rows'
    | 'add_row'
    | 'aggregate'
    | 'pivot'
    | 'train_model'
    | 'replace_value'
    | 'identify_outliers'
    | 'treat_outliers'
    | 'revert'
    | 'unknown';
  column?: string;
  oldColumnName?: string; // For rename_column - the column to rename
  method?: 'delete' | 'mean' | 'median' | 'mode' | 'custom';
  customValue?: any;
  targetType?: 'numeric' | 'string' | 'date' | 'percentage' | 'boolean';
  limit?: number;
  previewMode?: 'first' | 'last' | 'specific' | 'range'; // For preview operations
  previewStartRow?: number; // For specific row or range start (1-based)
  previewEndRow?: number; // For range end (1-based)
  newColumnName?: string;
  expression?: string;
  defaultValue?: any; // For creating columns with static values
  transformType?: 'add' | 'subtract' | 'multiply' | 'divide';
  transformValue?: number;
  rowPosition?: 'first' | 'last' | 'keep_first';
  rowIndex?: number;
  rowCount?: number; // For removing multiple rows from start/end
  oldValue?: any; // For replace_value operation - the value to replace
  newValue?: any; // For replace_value operation - the value to replace with
  // Aggregation / pivot fields
  groupByColumn?: string; // For aggregate
  aggColumns?: string[];  // Optional explicit aggregation columns
  aggFunc?: 'sum' | 'avg' | 'mean' | 'min' | 'max' | 'count'; // Aggregation function (default: sum)
  aggFuncs?: Record<string, 'sum' | 'avg' | 'mean' | 'min' | 'max' | 'count'>; // Per-column aggregation functions
  orderByColumn?: string; // For sorting aggregated results
  orderByDirection?: 'asc' | 'desc'; // Sort direction (default: asc)
  pivotIndex?: string;    // For pivot - index column
  pivotValues?: string[]; // For pivot - value columns
  pivotFuncs?: Record<string, 'sum' | 'avg' | 'mean' | 'min' | 'max' | 'count'>; // Per-column aggregation functions for pivot
  requiresClarification: boolean;
  clarificationType?: 'column' | 'method' | 'target_type';
  clarificationMessage?: string;
  // ML model fields
  modelType?: 'linear' | 'log_log' | 'logistic' | 'ridge' | 'lasso' | 'random_forest' | 'decision_tree' | 'gradient_boosting' | 'elasticnet' | 'svm' | 'knn' | 'polynomial' | 'bayesian' | 'quantile' | 'poisson' | 'gamma' | 'tweedie' | 'extra_trees' | 'xgboost' | 'lightgbm' | 'catboost' | 'gaussian_process' | 'mlp' | 'multinomial_logistic' | 'naive_bayes_gaussian' | 'naive_bayes_multinomial' | 'naive_bayes_bernoulli' | 'lda' | 'qda';
  targetVariable?: string;
  features?: string[];
  // Outlier detection/treatment fields
  outlierMethod?: 'iqr' | 'zscore' | 'isolation_forest' | 'local_outlier_factor';
  outlierThreshold?: number; // For zscore (default: 3), for IQR multiplier (default: 1.5)
  treatmentMethod?: 'remove' | 'cap' | 'winsorize' | 'transform' | 'impute';
  treatmentValue?: 'mean' | 'median' | 'mode' | 'min' | 'max' | number; // For impute or cap methods
}

export interface DataOpsContext {
  pendingOperation?: {
    operation: string;
    column?: string;
    timestamp: number;
  };
  lastQuery?: string;
  lastCreatedColumn?: string; // Track the most recently created column name
  timestamp: number;
}

/**
 * Parse user intent for data operations
 */
export async function parseDataOpsIntent(
  message: string,
  chatHistory: Message[],
  dataSummary: DataSummary,
  sessionDoc?: ChatDocument
): Promise<DataOpsIntent> {
  const lowerMessage = message.toLowerCase().trim();
  const availableColumns = dataSummary.columns.map(c => c.name);
  
  // ---------------------------------------------------------------------------
  // STEP 0: Use AI as PRIMARY method for ALL operations
  // AI is more flexible and can handle natural language variations better than regex
  // ---------------------------------------------------------------------------
  
  // Resolve context references BEFORE AI detection
  const { resolveContextReferences, findLastCreatedColumn } = await import('../agents/contextResolver.js');
  let resolvedMessage = message;
  if (chatHistory && chatHistory.length > 0) {
    resolvedMessage = resolveContextReferences(message, chatHistory);
    if (resolvedMessage !== message) {
      console.log(`🔄 Context resolved: "${message}" → "${resolvedMessage}"`);
    }
  }

  // Try AI detection first for ALL operations (using resolved message)
  try {
    console.log(`🤖 Calling AI to detect intent for: "${resolvedMessage}"`);
    const aiIntent = await detectDataOpsIntentWithAI(resolvedMessage, availableColumns, chatHistory, sessionDoc);
    if (aiIntent) {
      console.log(`🤖 AI returned intent:`, {
        operation: aiIntent.operation,
        groupByColumn: aiIntent.groupByColumn,
        aggColumns: aiIntent.aggColumns,
        aggFunc: aiIntent.aggFunc,
        requiresClarification: aiIntent.requiresClarification,
        clarificationMessage: aiIntent.clarificationMessage,
      });
      
      if (aiIntent.operation !== 'unknown') {
        console.log(`✅ AI detected intent: ${aiIntent.operation}`);
        
        // If rename_column and no column specified, try to find from context
        if (aiIntent.operation === 'rename_column' && !aiIntent.column && !aiIntent.oldColumnName) {
          const lastColumn = findLastCreatedColumn(chatHistory || []);
          if (lastColumn) {
            aiIntent.oldColumnName = lastColumn;
            aiIntent.column = lastColumn;
            console.log(`📋 Using context column for rename: "${lastColumn}"`);
          }
        }
        
        // Fallback pattern matching for outlier treatment - fix common AI parsing issues
        if (aiIntent.operation === 'treat_outliers') {
          const lowerResolved = resolvedMessage.toLowerCase();
          
          // Pattern: "impute outliers with mean" or "impute with mean"
          if ((lowerResolved.includes('impute') && lowerResolved.includes('mean')) ||
              (lowerResolved.includes('replace') && lowerResolved.includes('outlier') && lowerResolved.includes('mean'))) {
            if (!aiIntent.treatmentMethod || aiIntent.treatmentMethod === 'remove') {
              console.log(`🔧 Fixing treatment method: detected "impute with mean" but AI returned "${aiIntent.treatmentMethod}", correcting to "impute"`);
              aiIntent.treatmentMethod = 'impute';
              aiIntent.treatmentValue = 'mean';
            }
          }
          // Pattern: "impute outliers with median"
          else if ((lowerResolved.includes('impute') && lowerResolved.includes('median')) ||
                   (lowerResolved.includes('replace') && lowerResolved.includes('outlier') && lowerResolved.includes('median'))) {
            if (!aiIntent.treatmentMethod || aiIntent.treatmentMethod === 'remove') {
              console.log(`🔧 Fixing treatment method: detected "impute with median" but AI returned "${aiIntent.treatmentMethod}", correcting to "impute"`);
              aiIntent.treatmentMethod = 'impute';
              aiIntent.treatmentValue = 'median';
            }
          }
          // Pattern: "impute outliers with mode"
          else if ((lowerResolved.includes('impute') && lowerResolved.includes('mode')) ||
                   (lowerResolved.includes('replace') && lowerResolved.includes('outlier') && lowerResolved.includes('mode'))) {
            if (!aiIntent.treatmentMethod || aiIntent.treatmentMethod === 'remove') {
              console.log(`🔧 Fixing treatment method: detected "impute with mode" but AI returned "${aiIntent.treatmentMethod}", correcting to "impute"`);
              aiIntent.treatmentMethod = 'impute';
              aiIntent.treatmentValue = 'mode';
            }
          }
          
          console.log(`📊 Final outlier treatment config:`, {
            treatmentMethod: aiIntent.treatmentMethod,
            treatmentValue: aiIntent.treatmentValue,
            outlierMethod: aiIntent.outlierMethod
          });
        }
        
        return aiIntent;
      } else {
        console.log(`⚠️ AI returned 'unknown' operation, will fall back to regex`);
      }
    } else {
      console.log(`⚠️ AI returned null, will fall back to regex`);
    }
  } catch (error) {
    console.error('⚠️ AI intent detection failed, falling back to regex patterns:', error);
    console.error('⚠️ Error details:', error instanceof Error ? error.stack : String(error));
  }
  
  // Fallback to regex patterns if AI didn't detect STEP 0 operations
  // Replace value intent - Handle multiple phrasings and edge cases
  // Examples: 
  // - "replace - with 0"
  // - "remove - and put 134.2 instead"
  // - "remove - and replace with 134.2"
  // - "change - to 134.2"
  // - "substitute - for 134.2"
  // - "remove - and add 134.2"
  // - "remove - and use 134.2"
  // - "remove the value - and put 0"
  
  // Helper function to normalize old value
  function normalizeOldValue(val: string): any {
    val = val.replace(/^['"]|['"]$/g, '').trim();
    const lower = val.toLowerCase();
    if (lower === 'null' || lower === 'empty' || lower === 'blank') {
      return null;
    } else if (val === '-') {
      return '-';
    }
    return val;
  }
  
  // Helper function to normalize new value
  function normalizeNewValue(val: string): any {
    val = val.trim();
    // Remove trailing punctuation
    val = val.replace(/[.,;:!?]+$/, '');
    // Remove quotes
    val = val.replace(/^['"]|['"]$/g, '');
    
    // Try to parse as number
    if (/^-?\d+\.?\d*$/.test(val)) {
      const num = parseFloat(val);
      if (!isNaN(num) && isFinite(num)) {
        return num;
      }
    }
    
    // Handle null
    if (val.toLowerCase() === 'null') {
      return null;
    }
    
    // Try extractCustomValue as fallback
    const customResult = extractCustomValue(`with ${val}`);
    if (customResult.found) {
      return customResult.value;
    }
    
    return val;
  }
  
  // Helper function to extract old and new values from various patterns
  function extractReplaceValueIntent(msg: string): { oldValue: any; newValue: any; column?: string } | null {
    // Pattern 1: "replace/remove/change X with/to/by Y"
    // Group 1: verb, Group 2: "the" (optional), Group 3: "value" (optional), Group 4: quote (optional),
    // Group 5: old value, Group 6: quote (optional), Group 7: "with/to/by", Group 8: new value
    let match = msg.match(/\b(replace|remove|change|substitute)\s+(the\s+)?(value\s+)?(['"]?)(-|N\/A|NA|n\/a|na|empty|null|blank)(['"]?)\s+(with|to|by)\s+(.+?)(?:\s|$|,|\.|;|in|for|instead)/i);
    if (match) {
      const oldVal = (match[5] || '').trim(); // Group 5 is the old value
      const newVal = (match[8] || '').trim(); // Group 8 is the new value
      if (oldVal && newVal) {
        return { oldValue: normalizeOldValue(oldVal), newValue: normalizeNewValue(newVal), column: findMentionedColumn(msg, availableColumns) };
      }
    }
    
    // Pattern 2: "replace/remove X with Y" (simpler)
    // Group 1: verb, Group 2: quote (optional), Group 3: old value, Group 4: quote (optional), Group 5: "with/to/by", Group 6: new value
    match = msg.match(/\b(replace|remove|change|substitute)\s+(['"]?)(-|N\/A|NA|n\/a|na|empty|null|blank)(['"]?)\s+(with|to|by)\s+(.+?)(?:\s|$|,|\.|;|in|for|instead)/i);
    if (match) {
      const oldVal = (match[3] || '').trim(); // Group 3 is the old value
      const newVal = (match[6] || '').trim(); // Group 6 is the new value
      if (oldVal && newVal) {
        return { oldValue: normalizeOldValue(oldVal), newValue: normalizeNewValue(newVal), column: findMentionedColumn(msg, availableColumns) };
      }
    }
    
    // Pattern 3: "remove X and put Y instead" or "remove X and replace with Y"
    match = msg.match(/\b(remove|delete)\s+(the\s+)?(value\s+)?(['"]?)(-|N\/A|NA|n\/a|na|empty|null|blank)(['"]?)\s+and\s+(put|replace|add|use|set)\s+(.+?)(?:\s+instead|\s+in\s+place|$|,|\.|;)/i);
    if (match) {
      const oldVal = (match[4] || '').trim();
      const newVal = (match[7] || '').trim();
      if (oldVal && newVal) {
        return { oldValue: normalizeOldValue(oldVal), newValue: normalizeNewValue(newVal), column: findMentionedColumn(msg, availableColumns) };
      }
    }
    
    // Pattern 4: "remove X and put Y instead" (simpler, no "the value")
    // Group 1: verb, Group 2: quote (optional), Group 3: old value, Group 4: quote (optional), Group 5: "put/replace/add/use/set", Group 6: new value
    match = msg.match(/\b(remove|delete)\s+(['"]?)(-|N\/A|NA|n\/a|na|empty|null|blank)(['"]?)\s+and\s+(put|replace|add|use|set)\s+(.+?)(?:\s+instead|\s+in\s+place|$|,|\.|;)/i);
    if (match) {
      const oldVal = (match[3] || '').trim(); // Group 3 is the old value
      const newVal = (match[6] || '').trim(); // Group 6 is the new value
      if (oldVal && newVal) {
        return { oldValue: normalizeOldValue(oldVal), newValue: normalizeNewValue(newVal), column: findMentionedColumn(msg, availableColumns) };
      }
    }
    
    // Pattern 5: "change X to Y" or "convert X to Y"
    match = msg.match(/\b(change|convert|transform)\s+(the\s+)?(value\s+)?(['"]?)(-|N\/A|NA|n\/a|na|empty|null|blank)(['"]?)\s+to\s+(.+?)(?:\s|$|,|\.|;)/i);
    if (match) {
      const oldVal = (match[4] || '').trim();
      const newVal = (match[6] || '').trim();
      if (oldVal && newVal) {
        return { oldValue: normalizeOldValue(oldVal), newValue: normalizeNewValue(newVal), column: findMentionedColumn(msg, availableColumns) };
      }
    }
    
    // Pattern 6: "substitute X for Y" (note: "for" means replace X with Y)
    match = msg.match(/\b(substitute|replace)\s+(the\s+)?(value\s+)?(['"]?)(-|N\/A|NA|n\/a|na|empty|null|blank)(['"]?)\s+for\s+(.+?)(?:\s|$|,|\.|;)/i);
    if (match) {
      const oldVal = (match[4] || '').trim();
      const newVal = (match[6] || '').trim();
      if (oldVal && newVal) {
        return { oldValue: normalizeOldValue(oldVal), newValue: normalizeNewValue(newVal), column: findMentionedColumn(msg, availableColumns) };
      }
    }
    
    // Pattern 7: "remove X, use Y" or "remove X, put Y"
    match = msg.match(/\b(remove|delete)\s+(the\s+)?(value\s+)?(['"]?)(-|N\/A|NA|n\/a|na|empty|null|blank)(['"]?)\s*[,;]\s*(use|put|replace|add|set)\s+(.+?)(?:\s|$|,|\.|;)/i);
    if (match) {
      const oldVal = (match[4] || '').trim();
      const newVal = (match[7] || '').trim();
      if (oldVal && newVal) {
        return { oldValue: normalizeOldValue(oldVal), newValue: normalizeNewValue(newVal), column: findMentionedColumn(msg, availableColumns) };
      }
    }
    
    return null;
  }
  
  // Try to extract replace value intent (regex fallback)
  const replaceIntent = extractReplaceValueIntent(message);
  if (replaceIntent) {
    return {
      operation: 'replace_value',
      column: replaceIntent.column,
      oldValue: replaceIntent.oldValue,
      newValue: replaceIntent.newValue,
      requiresClarification: false
    };
  }
  
  // ---------------------------------------------------------------------------
  // STEP 0b: Explicit revert pattern (high confidence - should be checked early)
  // ---------------------------------------------------------------------------

  // Pattern: "revert to original", "restore original data", "revert table", etc.
  if (lowerMessage.includes('revert') || lowerMessage.includes('restore') || 
      (lowerMessage.includes('original') && (lowerMessage.includes('back') || lowerMessage.includes('to') || lowerMessage.includes('form')))) {
    return {
      operation: 'revert',
      requiresClarification: false,
    };
  }

  // ---------------------------------------------------------------------------
  // STEP 0c: Explicit aggregation / pivot patterns (high confidence)
  // ---------------------------------------------------------------------------

  // Pattern: "aggregate X, group by Y, order by Z DESC" (e.g., "aggregate risk value, group by SKU Desc, order by risk value DESC")
  if (lowerMessage.includes('aggregate') && (lowerMessage.includes('group by') || lowerMessage.includes('groupby'))) {
    // Extract aggregation columns (before "group by")
    const groupByMatch = message.match(/\baggregate\s+(.+?)\s*,\s*group\s+by\s+/i) || 
                         message.match(/\baggregate\s+(.+?)\s+group\s+by\s+/i);
    
    if (groupByMatch) {
      const rawAggCols = groupByMatch[1].trim();
      const aggColumns = rawAggCols.split(',').map(c => {
        const col = c.trim();
        return findMentionedColumn(col, availableColumns) || col;
      });

      // Extract group by column (between "group by" and "order by" or end)
      const orderByMatch = message.match(/group\s+by\s+([^,]+?)(?:\s*,\s*order\s+by|$)/i);
      let groupByColumn: string | undefined;
      if (orderByMatch) {
        const rawGroupBy = orderByMatch[1].trim();
        groupByColumn = findMentionedColumn(rawGroupBy, availableColumns) || rawGroupBy;
      }

      // Extract order by column and direction
      const orderByRegex = /order\s+by\s+([a-zA-Z0-9_\s]+?)(?:\s+(asc|desc|ascending|descending))?/i;
      const orderByMatch2 = message.match(orderByRegex);
      let orderByColumn: string | undefined;
      let orderByDirection: 'asc' | 'desc' = 'asc';
      
      if (orderByMatch2) {
        const rawOrderBy = orderByMatch2[1].trim();
        orderByColumn = findMentionedColumn(rawOrderBy, availableColumns) || rawOrderBy;
        const direction = orderByMatch2[2]?.toLowerCase();
        if (direction === 'desc' || direction === 'descending') {
          orderByDirection = 'desc';
        }
      }

      if (groupByColumn && aggColumns.length > 0) {
        console.log(`✅ Matched aggregate with group by: ${aggColumns.join(', ')} grouped by ${groupByColumn}${orderByColumn ? `, ordered by ${orderByColumn} ${orderByDirection}` : ''}`);
        return {
          operation: 'aggregate',
          groupByColumn,
          aggColumns,
          orderByColumn,
          orderByDirection,
          requiresClarification: false,
        };
      }
    }
  }

  // Pattern: "aggregate X on Y" (e.g., "aggregate RISK_VOLUME on DEPOT")
  // More flexible pattern to handle various formats including underscores
  const aggregateOnRegex = /\baggregate\s+([a-zA-Z0-9_]+(?:\s+[a-zA-Z0-9_]+)*)\s+on\s+([a-zA-Z0-9_]+(?:\s+[a-zA-Z0-9_]+)*)(?:\s|$|\.|,)/i;
  const aggregateOnMatch = aggregateOnRegex.exec(message);
  if (aggregateOnMatch) {
    const rawAggColumn = aggregateOnMatch[1].trim();
    const rawGroupBy = aggregateOnMatch[2].trim();
    
    const aggColumn = findMentionedColumn(rawAggColumn, availableColumns) || rawAggColumn;
    const groupByColumn = findMentionedColumn(rawGroupBy, availableColumns) || rawGroupBy;

    console.log(`✅ Regex matched aggregate pattern: aggregate ${aggColumn} on ${groupByColumn}`);
    return {
      operation: 'aggregate',
      groupByColumn,
      aggColumns: [aggColumn],
      requiresClarification: false,
    };
  }

  // Simpler catch-all: if message contains "aggregate" and "on", try to extract columns
  if (lowerMessage.includes('aggregate') && lowerMessage.includes(' on ')) {
    const parts = message.split(/\s+on\s+/i);
    if (parts.length === 2) {
      const beforeOn = parts[0].replace(/^aggregate\s+/i, '').trim();
      const afterOn = parts[1].split(/\s|,|\./)[0].trim(); // Take first word after "on"
      
      if (beforeOn && afterOn) {
        const aggColumn = findMentionedColumn(beforeOn, availableColumns) || beforeOn;
        const groupByColumn = findMentionedColumn(afterOn, availableColumns) || afterOn;
        
        console.log(`✅ Fallback pattern matched: aggregate ${aggColumn} on ${groupByColumn}`);
        return {
          operation: 'aggregate',
          groupByColumn,
          aggColumns: [aggColumn],
          requiresClarification: false,
        };
      }
    }
  }

  // Pattern: "aggregate all the other columns by X" or "aggregate all columns by X"
  // Use a more flexible pattern that captures everything between "by" and "using" (or end of string)
  const aggregateAllColumnsPattern = /\baggregate\s+(?:all\s+(?:the\s+other\s+)?columns?|all\s+other\s+columns?)\s+by\s+(.+?)(?:\s+using\s+(sum|avg|mean|min|max|count))?$/i;
  const aggregateAllColumnsMatch = aggregateAllColumnsPattern.exec(message);
  if (aggregateAllColumnsMatch) {
    const rawGroupBy = aggregateAllColumnsMatch[1].trim();
    const aggFunc = (aggregateAllColumnsMatch[2] || 'sum').toLowerCase() as 'sum' | 'avg' | 'mean' | 'min' | 'max' | 'count';
    
    // Try to find the column in available columns first
    let groupByColumn = findMentionedColumn(rawGroupBy, availableColumns);
    
    // If not found and rawGroupBy is suspiciously short, search in message context
    if (!groupByColumn && rawGroupBy.length < 3) {
      const messageLower = message.toLowerCase();
      const byIndex = messageLower.indexOf(' by ');
      const usingIndex = messageLower.indexOf(' using ');
      const endIndex = usingIndex !== -1 ? usingIndex : message.length;
      
      if (byIndex !== -1) {
        const betweenByAndUsing = message.substring(byIndex + 4, endIndex).trim();
        console.log(`🔍 Searching for column in context: "${betweenByAndUsing}"`);
        
        // Try to find a column that matches this text
        for (const col of availableColumns) {
          const colLower = col.toLowerCase();
          if (betweenByAndUsing.toLowerCase().includes(colLower) || colLower.includes(betweenByAndUsing.toLowerCase())) {
            groupByColumn = col;
            console.log(`✅ Found column "${col}" in message context`);
            break;
          }
        }
      }
    }
    
    groupByColumn = groupByColumn || rawGroupBy;

    console.log(`✅ Regex matched aggregate all columns pattern: aggregate all columns by ${groupByColumn} using ${aggFunc}`);
    console.log(`📋 Extracted rawGroupBy: "${rawGroupBy}", matched to: "${groupByColumn}"`);
    return {
      operation: 'aggregate',
      groupByColumn,
      aggColumns: undefined, // undefined means auto-detect all numeric columns
      aggFunc: aggFunc,
      requiresClarification: false,
    };
  }

  // Pattern: "aggregate over X" or "aggregate the whole data over X" or "aggregate all data over X"
  const aggregateOverRegex = /\baggregate\s+(?:the\s+whole\s+data|all\s+data|whole\s+data)?\s+over\s+([a-zA-Z0-9_ ]+?)(?:\s+column)?(?:\?|$)/i;
  const aggregateOverMatch = aggregateOverRegex.exec(message);
  if (aggregateOverMatch) {
    const rawGroupBy = aggregateOverMatch[1].trim();
    const groupByColumn =
      findMentionedColumn(rawGroupBy, availableColumns) || rawGroupBy;

    console.log(`✅ Regex matched aggregate over pattern: aggregate over ${groupByColumn}`);
    return {
      operation: 'aggregate',
      groupByColumn,
      aggColumns: undefined, // undefined means auto-detect all numeric columns
      requiresClarification: false,
    };
  }

  // Pattern: "aggregate X by Y using sum" - explicit column and function
  // Use a more precise pattern that captures full words/column names
  const aggregateByUsingPattern = /\baggregate\s+([a-zA-Z0-9_]+(?:\s+[a-zA-Z0-9_]+)*)\s+by\s+([a-zA-Z0-9_]+(?:\s+[a-zA-Z0-9_]+)*)\s+using\s+(sum|avg|mean|min|max|count)/i;
  const aggregateByUsingMatch = aggregateByUsingPattern.exec(message);
  if (aggregateByUsingMatch) {
    const rawAggColumn = aggregateByUsingMatch[1].trim();
    const rawGroupByColumn = aggregateByUsingMatch[2].trim();
    const aggFunc = (aggregateByUsingMatch[3] || 'sum').toLowerCase() as 'sum' | 'avg' | 'mean' | 'min' | 'max' | 'count';
    
    // Try to match columns from available columns list
    let matchedAggCol = findMentionedColumn(rawAggColumn, availableColumns);
    let matchedGroupBy = findMentionedColumn(rawGroupByColumn, availableColumns);
    
    // If groupBy wasn't found, try searching in the original message context
    if (!matchedGroupBy) {
      const messageLower = message.toLowerCase();
      const byIndex = messageLower.indexOf(' by ');
      const usingIndex = messageLower.indexOf(' using ');
      const endIndex = usingIndex !== -1 ? usingIndex : message.length;
      
      if (byIndex !== -1) {
        const betweenByAndUsing = message.substring(byIndex + 4, endIndex).trim();
        console.log(`🔍 Column not found via findMentionedColumn, searching in context: "${betweenByAndUsing}"`);
        
        // Try to find a column that matches this text (case-insensitive, word boundary aware)
        for (const col of availableColumns) {
          const colLower = col.toLowerCase();
          const contextLower = betweenByAndUsing.toLowerCase();
          
          // Check if column name appears in context or vice versa
          if (contextLower.includes(colLower) || colLower.includes(contextLower)) {
            // Prefer exact match or longer column name
            if (contextLower === colLower || colLower.length >= contextLower.length) {
              matchedGroupBy = col;
              console.log(`✅ Found column "${col}" in message context`);
              break;
            }
          }
        }
      }
    }
    
    // Fallback: use extracted values if no match found
    matchedAggCol = matchedAggCol || rawAggColumn;
    matchedGroupBy = matchedGroupBy || rawGroupByColumn;
    
    console.log(`✅ Regex matched aggregate pattern: aggregate ${matchedAggCol} by ${matchedGroupBy} using ${aggFunc}`);
    console.log(`📋 Extracted: rawAggColumn="${rawAggColumn}", rawGroupByColumn="${rawGroupByColumn}" -> matched: "${matchedGroupBy}"`);
    return {
      operation: 'aggregate',
      groupByColumn: matchedGroupBy,
      aggColumns: [matchedAggCol],
      aggFunc: aggFunc,
      requiresClarification: false,
    };
  }

  // Pattern: "aggregate by X column" or "aggregate by X"
  const aggregateByRegex = /\baggregate\s+by\s+([a-zA-Z0-9_ ]+?)(?:\s+column|\?|$)/i;
  const aggregateMatch = aggregateByRegex.exec(message);
  if (aggregateMatch) {
    const rawGroupBy = aggregateMatch[1].trim();
    const groupByColumn =
      findMentionedColumn(rawGroupBy, availableColumns) || rawGroupBy;

    return {
      operation: 'aggregate',
      groupByColumn,
      requiresClarification: false,
    };
  }

  // Pattern: "create a pivot on X showing A, B, C fields"
  const pivotRegex =
    /\bcreate\s+(?:a\s+)?pivot\s+on\s+([a-zA-Z0-9_ ]+?)\s+showing\s+([a-zA-Z0-9_,&\s]+?)\s*(?:fields?|columns?)?(?:\?|$)/i;
  const pivotMatch = pivotRegex.exec(message);
  if (pivotMatch) {
    const rawIndex = pivotMatch[1].trim();
    const rawValues = pivotMatch[2].trim();

    const pivotIndex =
      findMentionedColumn(rawIndex, availableColumns) || rawIndex;

    const pivotValues = rawValues
      .split(/[,&]/)
      .map(v => v.trim())
      .filter(v => v.length > 0)
      .map(v => findMentionedColumn(v, availableColumns) || v);

    return {
      operation: 'pivot',
      pivotIndex,
      pivotValues,
      requiresClarification: false,
    };
  }

  // Pattern: "pivot table for X" or "pivot for X" or "pivot on X" or "pivot by X"
  // Also handles "pivot table for X over rest of the columns" or "over remaining columns"
  // This handles simpler requests where user just specifies the index column
  const simplePivotRegex = /\b(?:create\s+)?(?:a\s+)?pivot\s+(?:table\s+)?(?:for|on|by)\s+([a-zA-Z0-9_ ]+?)(?:\s+(?:showing\s+([a-zA-Z0-9_,&\s]+?)|over\s+(?:rest|remaining|all)\s+(?:of\s+)?(?:the\s+)?(?:columns?|fields?)))?\s*(?:fields?|columns?)?(?:\?|$)/i;
  const simplePivotMatch = simplePivotRegex.exec(message);
  if (simplePivotMatch) {
    const rawIndex = simplePivotMatch[1].trim();
    const rawValues = simplePivotMatch[2] ? simplePivotMatch[2].trim() : '';

    const pivotIndex =
      findMentionedColumn(rawIndex, availableColumns) || rawIndex;

    let pivotValues: string[] = [];
    if (rawValues) {
      pivotValues = rawValues
        .split(/[,&]/)
        .map(v => v.trim())
        .filter(v => v.length > 0)
        .map(v => findMentionedColumn(v, availableColumns) || v);
    }
    // If no value columns specified (or "over rest of columns" mentioned), will be handled in executeDataOperation to use all columns

    return {
      operation: 'pivot',
      pivotIndex,
      pivotValues,
      requiresClarification: false,
    };
  }
  
  // Additional pattern: "pivot table for X over rest of the columns" (more explicit)
  const pivotOverRestRegex = /\b(?:create\s+)?(?:a\s+)?pivot\s+(?:table\s+)?(?:for|on|by)\s+([a-zA-Z0-9_ ]+?)\s+over\s+(?:rest|remaining|all)\s+(?:of\s+)?(?:the\s+)?(?:columns?|fields?)/i;
  const pivotOverRestMatch = pivotOverRestRegex.exec(message);
  if (pivotOverRestMatch) {
    const rawIndex = pivotOverRestMatch[1].trim();
    const pivotIndex =
      findMentionedColumn(rawIndex, availableColumns) || rawIndex;

    return {
      operation: 'pivot',
      pivotIndex,
      pivotValues: [], // Empty means use all other columns
      requiresClarification: false,
    };
  }
  
  // High-confidence "remove column" pattern (regex fallback) – this should not be treated as
  // a clarification response even if we were previously asking about nulls.
  // Allow common typos like "remover"/"removing" by matching "remov*"
  const removeColumnRegex = /\b(remove|remov\w*|delete|drop)\s+(the\s+)?(column|col)\b/i;
  if (removeColumnRegex.test(message)) {
    const mentionedColumn = findMentionedColumn(message, availableColumns);
    
    if (mentionedColumn) {
    return {
        operation: 'remove_column',
        column: mentionedColumn,
        requiresClarification: false,
      };
    } else {
      return {
        operation: 'remove_column',
        requiresClarification: true,
        clarificationType: 'column',
        clarificationMessage: 'Which column would you like to remove? Please specify the column name.'
      };
    }
  }
  
  // High-confidence "remove row" patterns (regex fallback) – handle explicit first/last/index
  // directly before any clarification / AI logic so simple requests like
  // "remove the first row" just work.
  const lower = lowerMessage;
  
  // Pattern: "keep only first N rows" or "keep first N rows" - convert to "remove last (total - N) rows"
  // Also handles: "keep only the first N rows from the dataset and remove the rest"
  const keepFirstRegex = /\bkeep\s+(?:only\s+)?(?:the\s+)?first\s+(\d+)\s+rows?/i;
  const keepFirstMatch = keepFirstRegex.exec(message);
  if (keepFirstMatch) {
    const count = parseInt(keepFirstMatch[1], 10);
    if (!Number.isNaN(count) && count > 0) {
      // "Keep only first N rows" means "remove last (total - N) rows"
      // We'll handle this in executeDataOperation by calculating total - N
      return {
        operation: 'remove_rows',
        rowPosition: 'keep_first', // Special flag to indicate "keep first N, remove rest"
        rowCount: count,
        requiresClarification: false,
      };
    }
  }
  
  // Pattern: remove/delete/drop the first/last row
  const firstLastRowRegex = /\b(remove|remov\w*|delete|drop)\s+(the\s+)?(first|last)\s+row\b/i;
  const rowIndexRegex = /\b(remove|remov\w*|delete|drop)\s+row\s+(\d+)\b/i;
  const firstNRowsRegex = /\b(remove|remov\w*|delete|drop)\s+(the\s+)?first\s+(\d+)\s+rows?\b/i;
  const lastNRowsRegex = /\b(remove|remov\w*|delete|drop)\s+(the\s+)?last\s+(\d+)\s+rows?\b/i;
  
  // Explicit "first N rows"
  const firstNMatch = firstNRowsRegex.exec(message);
  if (firstNMatch) {
    const count = parseInt(firstNMatch[3], 10);
    if (!Number.isNaN(count) && count > 0) {
    return {
        operation: 'remove_rows',
        rowPosition: 'first',
        rowCount: count,
        requiresClarification: false,
      };
    }
  }

  // Explicit "last N rows"
  const lastNMatch = lastNRowsRegex.exec(message);
  if (lastNMatch) {
    const count = parseInt(lastNMatch[3], 10);
    if (!Number.isNaN(count) && count > 0) {
    return {
        operation: 'remove_rows',
        rowPosition: 'last',
        rowCount: count,
        requiresClarification: false,
      };
    }
  }

  const firstLastMatch = firstLastRowRegex.exec(message);
  if (firstLastMatch) {
    const which = firstLastMatch[3].toLowerCase();
    return {
      operation: 'remove_rows',
      rowPosition: which === 'last' ? 'last' : 'first',
      requiresClarification: false,
    };
  }
  
  const rowIndexMatch = rowIndexRegex.exec(message);
  if (rowIndexMatch) {
    const index = parseInt(rowIndexMatch[2], 10);
    if (!Number.isNaN(index) && index > 0) {
      return {
        operation: 'remove_rows',
        rowIndex: index,
        requiresClarification: false,
      };
    }
  }
  
  // ---------------------------------------------------------------------------
  // STEP 1: Handle clarification responses FIRST (highest priority)
  // This must come before AI detection to handle follow-up responses
  // ---------------------------------------------------------------------------
  const dataOpsContext = sessionDoc?.dataOpsContext as DataOpsContext | undefined;
  const pendingOp = dataOpsContext?.pendingOperation;
  
  if (pendingOp) {
    const age = Date.now() - pendingOp.timestamp;
    if (age < 5 * 60 * 1000) { // 5 minutes TTL
      // Detect if the user is clearly starting a NEW operation rather than
      // answering the previous clarification question.
      //
      // Example: After being asked how to handle nulls, the user says
      // "remove the column Maya TOM" – this should be treated as a
      // remove_column operation, not a clarification for remove_nulls.
      const mentionsColumn = lowerMessage.includes('column') || lowerMessage.includes('col ');
      const removalVerbs = lowerMessage.includes('remove') ||
        lowerMessage.includes('delete') ||
        lowerMessage.includes('drop');
      const mentionsNullLikeTerms =
        lowerMessage.includes('null') ||
        lowerMessage.includes('missing') ||
        lowerMessage.includes('nan');

      const looksLikeNewRemoveColumnRequest =
        pendingOp.operation === 'remove_nulls' &&
        mentionsColumn &&
        removalVerbs &&
        !mentionsNullLikeTerms;

      if (!looksLikeNewRemoveColumnRequest) {
      return handleClarificationResponse(message, pendingOp, availableColumns, dataSummary);
      }
      // If it looks like a new remove-column style request, we intentionally
      // skip clarification handling and let AI/regex logic below treat it
      // as a fresh intent.
    }
  }
  
  // ---------------------------------------------------------------------------
  // STEP 2: Fallback to regex patterns ONLY if AI failed or returned unknown
  // This is a safety net for cases where AI might fail or be unavailable
  // ---------------------------------------------------------------------------
  
  // Fill/Impute nulls intent (check this BEFORE remove/delete to prioritize imputation)
  if (lowerMessage.includes('fill null') || lowerMessage.includes('fill nulls') || 
      lowerMessage.includes('impute null') || lowerMessage.includes('replace null') ||
      (lowerMessage.includes('null') && (lowerMessage.includes('fill') || lowerMessage.includes('impute') || lowerMessage.includes('replace')))) {
    // Check if method is mentioned
    const mentionedColumn = findMentionedColumn(message, availableColumns);
    let method: 'mean' | 'median' | 'mode' | 'custom' | undefined;
    let customValue: any;
    
    if (lowerMessage.includes('mean') || lowerMessage.includes('average')) {
      method = 'mean';
    } else if (lowerMessage.includes('median')) {
      method = 'median';
    } else if (lowerMessage.includes('mode') || lowerMessage.includes('most frequent')) {
      method = 'mode';
    } else {
      // Check for custom value (number or string)
      const customValueResult = extractCustomValue(message);
      if (customValueResult.found) {
        method = 'custom';
        customValue = customValueResult.value;
      } else if (lowerMessage.includes('custom')) {
        // User mentioned "custom" but didn't specify value - need clarification
        method = 'custom';
        customValue = undefined;
      }
    }
    
    // If method is specified, check if custom value is needed
    if (method) {
      // If method is 'custom' but no value specified, ask for clarification
      if (method === 'custom' && customValue === undefined) {
        return {
          operation: 'remove_nulls',
          column: mentionedColumn,
          method: 'custom',
          requiresClarification: true,
          clarificationType: 'method',
          clarificationMessage: mentionedColumn 
            ? `What value would you like to use to fill null values in "${mentionedColumn}"? (e.g., 0, "N/A", "Unknown", etc.)`
            : 'What value would you like to use to fill null values? (e.g., 0, "N/A", "Unknown", etc.)'
        };
      }
      
      // Method and value (if needed) are specified, execute directly
      return {
        operation: 'remove_nulls',
        column: mentionedColumn,
        method,
        customValue,
        requiresClarification: false
      };
    }
    
    // Method not specified, need clarification
    if (mentionedColumn) {
      return {
        operation: 'remove_nulls',
        column: mentionedColumn,
        requiresClarification: true,
        clarificationType: 'method',
        clarificationMessage: `How do you want to fill null values in "${mentionedColumn}"?\n\nOption A: Delete Row\nOption B: Impute with mean/median/mode/custom value`
      };
    } else {
      return {
        operation: 'remove_nulls',
        requiresClarification: true,
        clarificationType: 'method',
        clarificationMessage: 'How do you want to fill null values?\n\nOption A: Delete Row\nOption B: Impute with mean/median/mode/custom value'
      };
    }
  }
  
  // Remove nulls intent (deletion-focused)
  if (lowerMessage.includes('remove null') || lowerMessage.includes('delete null') || lowerMessage.includes('handle null')) {
    // Check if column is mentioned
    const mentionedColumn = findMentionedColumn(message, availableColumns);
    
    if (mentionedColumn) {
      // Column specified, need method clarification
      return {
        operation: 'remove_nulls',
        column: mentionedColumn,
        requiresClarification: true,
        clarificationType: 'method',
        clarificationMessage: `How do you want to deal with null values in "${mentionedColumn}"?\n\nOption A: Delete Row\nOption B: Impute with mean/median/mode/custom value`
      };
    } else {
      // No column specified, need column clarification
      return {
        operation: 'remove_nulls',
        requiresClarification: true,
        clarificationType: 'column',
        clarificationMessage: 'Is it about a specific column or in the entire data?'
      };
    }
  }
  
  // Preview intent - handle "data preview", "give me data preview", "show data", etc.
  // Check for explicit "data preview" patterns first (HIGH PRIORITY)
  if (lowerMessage.includes('data preview') || lowerMessage.includes('preview data') || 
      lowerMessage.match(/(?:give me|show me|display|view|see)\s+(?:the\s+)?(?:data\s+)?preview/i)) {
    // Extract number if specified (e.g., "give me data preview of 10 rows")
    const limitMatch = lowerMessage.match(/(\d+)\s*(?:rows?|records?)?/i);
    const limit = limitMatch ? Math.min(parseInt(limitMatch[1], 10), 10000) : 50;
    
    return {
      operation: 'preview',
      previewMode: 'first',
      limit: limit,
      requiresClarification: false
    };
  }
  
  // Preview intent - handle first, last, specific rows, and ranges
  if (lowerMessage.includes('show') && (lowerMessage.includes('data') || lowerMessage.includes('rows') || lowerMessage.includes('row'))) {
    // Pattern 1: Range - handle multiple phrasings
    // "show rows 12 to 28" or "show rows 12-28" or "show rows 12 through 28"
    // "show me row from range 3 to 10 rows" or "row from range 3 to 10"
    // "range 3 to 10 rows" or "rows from range 3 to 10"
    let rangeMatch = lowerMessage.match(/rows?\s+(\d+)\s+(?:to|through|-)\s+(\d+)/i) ||
                     lowerMessage.match(/row\s+from\s+range\s+(\d+)\s+to\s+(\d+)/i) ||
                     lowerMessage.match(/range\s+(\d+)\s+to\s+(\d+)\s+rows?/i) ||
                     lowerMessage.match(/rows?\s+from\s+range\s+(\d+)\s+to\s+(\d+)/i) ||
                     lowerMessage.match(/from\s+range\s+(\d+)\s+to\s+(\d+)/i);
    if (rangeMatch) {
      const startRow = parseInt(rangeMatch[1], 10);
      const endRow = parseInt(rangeMatch[2], 10);
      if (startRow > 0 && endRow > 0 && endRow >= startRow) {
        return {
          operation: 'preview',
          previewMode: 'range',
          previewStartRow: startRow,
          previewEndRow: endRow,
          requiresClarification: false
        };
      }
    }
    
    // Pattern 2: Specific row - "show row 12" or "show the 12th row" or "show row number 12"
    let specificMatch = lowerMessage.match(/(?:the\s+)?(\d+)(?:st|nd|rd|th)\s+row/i) || 
                        lowerMessage.match(/row\s+(?:number\s+)?(\d+)/i) ||
                        lowerMessage.match(/show\s+(?:the\s+)?row\s+(\d+)/i);
    if (specificMatch) {
      const rowNum = parseInt(specificMatch[1], 10);
      if (rowNum > 0) {
        return {
          operation: 'preview',
          previewMode: 'specific',
          previewStartRow: rowNum,
          requiresClarification: false
        };
      }
    }
    
    // Pattern 3: Last N rows - "show last 5 rows" or "show me the last 10 rows"
    let lastMatch = lowerMessage.match(/last\s+(\d+)\s+rows?/i) ||
                    lowerMessage.match(/show\s+(?:me\s+)?(?:the\s+)?last\s+(\d+)\s+rows?/i);
    if (lastMatch) {
      const limit = parseInt(lastMatch[1], 10);
      if (limit > 0) {
        return {
          operation: 'preview',
          previewMode: 'last',
          limit: Math.min(limit, 10000),
          requiresClarification: false
        };
      }
    }
    
    // Pattern 4: First N rows - "show first 10 rows" or "show me only first 10 rows"
    let firstMatch = lowerMessage.match(/(?:first|top)\s+(\d+)\s+rows?/i) ||
                     lowerMessage.match(/show\s+(?:me\s+)?(?:only\s+)?(?:the\s+)?(?:first|top)\s+(\d+)\s+rows?/i);
    if (firstMatch) {
      const limit = parseInt(firstMatch[1], 10);
      if (limit > 0) {
        return {
          operation: 'preview',
          previewMode: 'first',
          limit: Math.min(limit, 10000),
          requiresClarification: false
        };
      }
    }
    
    // Pattern 5: Generic "show N rows" - defaults to first N
    let genericMatch = lowerMessage.match(/show\s+(?:me\s+)?(?:only\s+)?(?:the\s+)?(\d+)\s+rows?/i);
    if (genericMatch) {
      const limit = parseInt(genericMatch[1], 10);
      if (limit > 0) {
        return {
          operation: 'preview',
          previewMode: 'first',
          limit: Math.min(limit, 10000),
          requiresClarification: false
        };
      }
    }
    
    // Pattern 6: Simple "show N" or "first N" - defaults to first N
    let simpleMatch = lowerMessage.match(/(?:show|first|top)\s+(\d+)/i);
    if (simpleMatch) {
      const limit = parseInt(simpleMatch[1], 10);
      if (limit > 0) {
        return {
          operation: 'preview',
          previewMode: 'first',
          limit: Math.min(limit, 10000),
          requiresClarification: false
        };
      }
    }
    
    // Default: show first 50 rows
    return {
      operation: 'preview',
      previewMode: 'first',
      limit: 50,
      requiresClarification: false
    };
  }
  
  // Count nulls intent (conversational) - handle various phrasings
  if ((lowerMessage.includes('null') || lowerMessage.includes('missing') || lowerMessage.includes('empty')) && 
      (lowerMessage.includes('how many') || lowerMessage.includes('count') || 
       lowerMessage.includes('number of') || lowerMessage.includes('how much') ||
       lowerMessage.includes('are there'))) {
    const mentionedColumn = findMentionedColumn(message, availableColumns);
    return {
      operation: 'count_nulls',
      column: mentionedColumn,
      requiresClarification: false
    };
  }
  
  // Handle "how many rows/columns" questions
  if (lowerMessage.includes('how many rows') || lowerMessage.includes('how many records')) {
    return {
      operation: 'describe',
      requiresClarification: false
    };
  }
  
  if (lowerMessage.includes('how many columns') || lowerMessage.includes('how many variables')) {
    return {
      operation: 'describe',
      requiresClarification: false
    };
  }
  
  // Summary intent - check for "data summary" patterns first (HIGH PRIORITY)
  if (lowerMessage.includes('data summary') || lowerMessage.includes('summary of data') ||
      lowerMessage.match(/(?:give me|show me|display|view|see)\s+(?:the\s+)?(?:data\s+)?summary/i)) {
    // Check if a specific column is mentioned
    const mentionedColumn = findMentionedColumn(message, availableColumns);
    
    if (mentionedColumn) {
      return {
        operation: 'summary',
        column: mentionedColumn,
        requiresClarification: false
      };
    } else {
      return {
        operation: 'summary',
        requiresClarification: false
      };
    }
  }
  
  // Summary intent - check if specific column is mentioned
  if (lowerMessage.includes('summary') || lowerMessage.includes('statistics')) {
    // Check if a specific column is mentioned
    const mentionedColumn = findMentionedColumn(message, availableColumns);
    
    if (mentionedColumn) {
      return {
        operation: 'summary',
        column: mentionedColumn,
        requiresClarification: false
      };
    } else {
      return {
        operation: 'summary',
        requiresClarification: false
      };
    }
  }
  
  // Describe data intent (conversational)
  if (lowerMessage.includes('describe') || lowerMessage.includes('tell me about') ||
      lowerMessage.includes('what is') || lowerMessage.includes('how many rows') ||
      lowerMessage.includes('how many columns') || lowerMessage.includes('data shape') ||
      lowerMessage.includes('data size')) {
    return {
      operation: 'describe',
      requiresClarification: false
    };
  }
  
  // Create column intent - check if it's a derived column (with expression) or simple column (with static value)
  if ((lowerMessage.includes('create') || lowerMessage.includes('add') || lowerMessage.includes('make')) &&
      (lowerMessage.includes('column') || lowerMessage.includes('new column'))) {
    
    // Check if it's a derived column (has expression with column references or operations)
    // Also check for conditional logic (if/then/else/otherwise/when)
    const hasConditionalLogic = /\b(if|when|where)\s+.+\s+(then|put|set|assign|use|return)/i.test(lowerMessage) ||
                                 /\botherwise|else\b/i.test(lowerMessage) ||
                                 /\bmore\s+than|less\s+than|greater\s+than|equal\s+to|not\s+equal/i.test(lowerMessage);
    
    if (hasConditionalLogic ||
        lowerMessage.includes('sum') || lowerMessage.includes('add') || lowerMessage.includes('+') ||
        lowerMessage.includes('multiply') || lowerMessage.includes('*') || lowerMessage.includes('times') ||
        lowerMessage.includes('subtract') || lowerMessage.includes('-') || lowerMessage.includes('minus') ||
        lowerMessage.includes('divide') || lowerMessage.includes('/') ||
        lowerMessage.includes('mean') || lowerMessage.includes('average') || lowerMessage.includes('median') ||
        lowerMessage.includes('=') && (lowerMessage.includes('[') || availableColumns.some(col => lowerMessage.includes(col)))) {
      // This is a derived column - will be handled by AI extraction
      return {
        operation: 'create_derived_column',
        requiresClarification: false
      };
    } else {
      // This is likely a simple column with static value - will be handled by AI extraction
      return {
        operation: 'create_column',
        requiresClarification: false
      };
    }
  }
  
  // Normalize column intent
  if (lowerMessage.includes('normalize') || lowerMessage.includes('normalise') || lowerMessage.includes('standardize')) {
    // First, try to extract column name using regex pattern
    // Patterns: "normalize Emami 7 Oils TOM", "normalize the column X", "normalize column X"
    let extractedColumnName: string | undefined;
    
    // Pattern 1: "normalize [column name]" - captures everything after "normalize" to end of message
    // Then we'll clean it up by removing stop words
    const normalizePattern1 = /\b(normalize|normalise|standardize)\s+(?:the\s+)?(?:column\s+)?(.+)/i;
    const match1 = message.match(normalizePattern1);
    if (match1 && match1[2]) {
      extractedColumnName = match1[2].trim();
      // Remove common stop words that might be at the end (but preserve column name words)
      extractedColumnName = extractedColumnName.replace(/\s+(please|can|you|will|the|column|columns|for|to|with|by)$/i, '').trim();
      // Remove trailing punctuation
      extractedColumnName = extractedColumnName.replace(/[.,;:!?]+$/, '');
    }
    
    // If we extracted a column name, try to match it against available columns
    let mentionedColumn: string | undefined;
    if (extractedColumnName) {
      const normalizedExtracted = extractedColumnName.toLowerCase().replace(/\s+/g, ' ').trim();
      const extractedWords = normalizedExtracted.split(/\s+/).filter(w => w.length > 0);
      
      // First try exact match (case-insensitive, normalized spaces)
      for (const col of availableColumns) {
        const normalizedCol = col.toLowerCase().replace(/\s+/g, ' ').trim();
        if (normalizedCol === normalizedExtracted) {
          mentionedColumn = col;
          break;
        }
      }
      
      // If no exact match, try to find column where ALL words from extracted name match
      // This ensures "Emami 7 Oils TOM" matches "Emami 7 Oils TOM" not "Emami 7 Oils nGRP"
      if (!mentionedColumn && extractedWords.length > 0) {
        // Sort columns by length (longest first) to prioritize more specific matches
        const sortedColumns = [...availableColumns].sort((a, b) => b.length - a.length);
        
        for (const col of sortedColumns) {
          const colLower = col.toLowerCase();
          let allWordsMatch = true;
          let matchCount = 0;
          
          for (const word of extractedWords) {
            // Use word boundary regex to ensure we match complete words
            const wordRegex = new RegExp(`\\b${word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
            if (wordRegex.test(colLower)) {
              matchCount++;
            } else {
              // If word doesn't match as a word boundary, check if it's a substring
              // but only if the word is significant (length >= 2)
              if (word.length >= 2 && colLower.includes(word)) {
                matchCount++;
              } else {
                allWordsMatch = false;
                break;
              }
            }
          }
          
          // If all words match, return this column immediately
          if (allWordsMatch && matchCount === extractedWords.length) {
            mentionedColumn = col;
            break;
          }
        }
      }
      
      // If still no match, try word-boundary matching with the extracted name
      if (!mentionedColumn) {
        mentionedColumn = findMentionedColumn(extractedColumnName, availableColumns);
      }
    }
    
    // Fallback to original method if regex extraction didn't work
    if (!mentionedColumn) {
      mentionedColumn = findMentionedColumn(message, availableColumns);
    }
    
    if (mentionedColumn) {
      return {
        operation: 'normalize_column',
        column: mentionedColumn,
        requiresClarification: false,
      };
    } else {
      return {
        operation: 'normalize_column',
        requiresClarification: true,
        clarificationType: 'column',
        clarificationMessage: 'Which column would you like to normalize?'
      };
    }
  }

  // Remove row intent
  if ((lowerMessage.includes('remove') || lowerMessage.includes('delete')) && lowerMessage.includes('row')) {
    const indexMatch = lowerMessage.match(/row\s+(\d+)/);
    if (indexMatch) {
      return {
        operation: 'remove_rows',
        rowIndex: parseInt(indexMatch[1], 10),
        requiresClarification: false,
      };
    }
    if (lowerMessage.includes('last')) {
      return {
        operation: 'remove_rows',
        rowPosition: 'last',
        requiresClarification: false,
      };
    }
    if (lowerMessage.includes('first') || lowerMessage.includes('top')) {
      return {
        operation: 'remove_rows',
        rowPosition: 'first',
        requiresClarification: false,
      };
    }
  }

  // Add row intent
  if (lowerMessage.includes('add row') || lowerMessage.includes('insert row') || lowerMessage.includes('append row')) {
    return {
      operation: 'add_row',
      requiresClarification: false,
    };
  }

  // Modify existing column values intent (increase/decrease a column)
  if ((lowerMessage.includes('increase') || lowerMessage.includes('decrease') || lowerMessage.includes('reduce') ||
      lowerMessage.includes('subtract') || lowerMessage.includes('add') || lowerMessage.includes('adjust')) &&
      lowerMessage.includes('column')) {
    const mentionedColumn = findMentionedColumn(message, availableColumns);
    if (mentionedColumn) {
      const verbMatch = lowerMessage.match(/\b(increase|decrease|reduce|subtract|add|adjust)\b/);
      const valueMatch = message.match(/(?:by|add|increase|decrease|reduce|subtract)\s+(-?\d+(?:\.\d+)?)/i);

      if (verbMatch && valueMatch) {
        const transformValue = parseFloat(valueMatch[1]);
        let transformType: DataOpsIntent['transformType'];

        switch (verbMatch[1]) {
          case 'increase':
          case 'add':
          case 'adjust':
            transformType = 'add';
            break;
          case 'decrease':
          case 'reduce':
          case 'subtract':
            transformType = 'subtract';
            break;
          default:
            transformType = 'add';
        }

        return {
          operation: 'modify_column',
          column: mentionedColumn,
          transformType,
          transformValue,
          requiresClarification: false,
        };
      }
    }
  }

  // Rename column intent - check BEFORE remove_column to avoid conflicts
  if ((lowerMessage.includes('rename') || lowerMessage.includes('change') || lowerMessage.includes('update')) &&
      (lowerMessage.includes('column') || lowerMessage.includes('name'))) {
    // Pattern 1: "rename column X to Y" or "change column name from X to Y"
    const renamePattern1 = /(?:rename|change|update)\s+(?:the\s+)?(?:column\s+)?(?:name\s+)?(?:from\s+)?["']?([^"'\s]+)["']?\s+to\s+["']?([^"'\s]+)["']?/i;
    const match1 = message.match(renamePattern1);
    if (match1) {
      const oldName = match1[1].trim();
      const newName = match1[2].trim();
      const matchedColumn = findMatchingColumn(oldName, availableColumns);
      return {
        operation: 'rename_column',
        oldColumnName: matchedColumn || oldName,
        column: matchedColumn || oldName,
        newColumnName: newName,
        requiresClarification: false
      };
    }
    
    // Pattern 2: "rename column X Y" (without "to")
    const renamePattern2 = /(?:rename|change|update)\s+(?:the\s+)?column\s+["']?([^"'\s]+)["']?\s+["']?([^"'\s]+)["']?/i;
    const match2 = message.match(renamePattern2);
    if (match2 && !lowerMessage.includes('to')) {
      const oldName = match2[1].trim();
      const newName = match2[2].trim();
      const matchedColumn = findMatchingColumn(oldName, availableColumns);
      return {
        operation: 'rename_column',
        oldColumnName: matchedColumn || oldName,
        column: matchedColumn || oldName,
        newColumnName: newName,
        requiresClarification: false
      };
    }
    
    // Pattern 3: "change the above column name to X" or "rename that column to X"
    // This will be handled by context resolution, but we can still detect the operation
    if ((lowerMessage.includes('above') || lowerMessage.includes('that') || lowerMessage.includes('it') || 
         lowerMessage.includes('previous') || lowerMessage.includes('last')) &&
        lowerMessage.includes('to')) {
      const toMatch = message.match(/\bto\s+["']?([^"'\s]+)["']?/i);
      if (toMatch) {
        const newName = toMatch[1].trim();
        // Column will be resolved from context
        return {
          operation: 'rename_column',
          newColumnName: newName,
          requiresClarification: false
        };
      }
    }
    
    // If we detected rename intent but couldn't extract names, ask for clarification
    return {
      operation: 'rename_column',
      requiresClarification: true,
      clarificationType: 'column',
      clarificationMessage: 'Which column would you like to rename, and what should the new name be? For example: "Rename column Sales to Revenue"'
    };
  }

  // Remove column intent
  if ((lowerMessage.includes('remove') || lowerMessage.includes('delete') || lowerMessage.includes('drop')) &&
      (lowerMessage.includes('column') || lowerMessage.includes('col'))) {
    const mentionedColumn = findMentionedColumn(message, availableColumns);
    
    if (mentionedColumn) {
      return {
        operation: 'remove_column',
        column: mentionedColumn,
        requiresClarification: false
      };
    } else {
      return {
        operation: 'remove_column',
        requiresClarification: true,
        clarificationType: 'column',
        clarificationMessage: 'Which column would you like to remove? Please specify the column name.'
      };
    }
  }
  
  // Type conversion intent - handle multi-word column names
  // Patterns: "convert Dove nGRP Adstocked to string", "convert column X to numeric", etc.
  const convertTypePatterns = [
    // Pattern 1: "convert [column name] to [type]" - captures everything between "convert" and "to"
    /\b(convert|change|transform)\s+(?:the\s+)?(?:column\s+)?(.+?)\s+(?:data\s+)?type\s+to\s+(numeric|string|date|percentage|boolean|number)/i,
    // Pattern 2: "convert [column name] to [type]" - simpler pattern
    /\b(convert|change|transform)\s+(?:the\s+)?(?:column\s+)?(.+?)\s+to\s+(numeric|string|date|percentage|boolean|number)/i,
  ];
  
  for (const pattern of convertTypePatterns) {
    const typeMatch = message.match(pattern);
    if (typeMatch) {
      let extractedColumnName = typeMatch[2].trim();
      const targetTypeRaw = (typeMatch[3] || typeMatch[4] || '').toLowerCase();
      
      // Clean up extracted column name - remove common stop words at the end
      extractedColumnName = extractedColumnName.replace(/\s+(please|can|you|will|the|column|columns|for|to|with|by|data|type)$/i, '').trim();
      // Remove trailing punctuation
      extractedColumnName = extractedColumnName.replace(/[.,;:!?]+$/, '');
      
      // Try to match the extracted column name against available columns
      let mentionedColumn: string | undefined;
      if (extractedColumnName) {
        const normalizedExtracted = extractedColumnName.toLowerCase().replace(/\s+/g, ' ').trim();
        const extractedWords = normalizedExtracted.split(/\s+/).filter(w => w.length > 0);
        
        // First try exact match (case-insensitive, normalized spaces)
        for (const col of availableColumns) {
          const normalizedCol = col.toLowerCase().replace(/\s+/g, ' ').trim();
          if (normalizedCol === normalizedExtracted) {
            mentionedColumn = col;
            break;
          }
        }
        
        // If no exact match, try to find column where ALL words from extracted name match
        if (!mentionedColumn && extractedWords.length > 0) {
          // Sort columns by length (longest first) to prioritize more specific matches
          const sortedColumns = [...availableColumns].sort((a, b) => b.length - a.length);
          
          for (const col of sortedColumns) {
            const colLower = col.toLowerCase();
            let allWordsMatch = true;
            let matchCount = 0;
            
            for (const word of extractedWords) {
              // Use word boundary regex to ensure we match complete words
              const wordRegex = new RegExp(`\\b${word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
              if (wordRegex.test(colLower)) {
                matchCount++;
              } else {
                // If word doesn't match as a word boundary, check if it's a substring
                // but only if the word is significant (length >= 2)
                if (word.length >= 2 && colLower.includes(word)) {
                  matchCount++;
                } else {
                  allWordsMatch = false;
                  break;
                }
              }
            }
            
            // If all words match, return this column immediately
            if (allWordsMatch && matchCount === extractedWords.length) {
              mentionedColumn = col;
              break;
            }
          }
        }
        
        // If still no match, try the original findMentionedColumn function
        if (!mentionedColumn) {
          mentionedColumn = findMentionedColumn(extractedColumnName, availableColumns);
        }
      }
      
      if (mentionedColumn && targetTypeRaw) {
        const normalizedTarget = (targetTypeRaw === 'number' ? 'numeric' : targetTypeRaw) as 'numeric' | 'string' | 'date' | 'percentage' | 'boolean';
        return {
          operation: 'convert_type',
          column: mentionedColumn,
          targetType: normalizedTarget,
          requiresClarification: false
        };
      } else if (!mentionedColumn) {
        return {
          operation: 'convert_type',
          requiresClarification: true,
          clarificationType: 'column',
          clarificationMessage: `Column "${extractedColumnName}" not found. Please specify a valid column name.`
        };
      } else if (!targetTypeRaw) {
        return {
          operation: 'convert_type',
          column: mentionedColumn,
          requiresClarification: true,
          clarificationType: 'target_type',
          clarificationMessage: `What type would you like to convert "${mentionedColumn}" to? (numeric, string, date, percentage, or boolean)`
        };
      }
    }
  }
  
  // Detect advice-style questions about models (should NOT trigger train_model here)
  const isModelAdviceQuestion =
    (
      lowerMessage.includes('how can we improve') ||
      lowerMessage.includes('how do we improve') ||
      lowerMessage.includes('how to improve') ||
      lowerMessage.includes('what should we do') ||
      lowerMessage.includes('what can we do to') ||
      lowerMessage.includes('what would help') ||
      lowerMessage.includes('suggestions for') ||
      lowerMessage.includes('recommendations for') ||
      lowerMessage.includes('advice on')
    ) &&
    lowerMessage.includes('model');

  if (isModelAdviceQuestion) {
    return {
      operation: 'unknown',
      requiresClarification: false
    };
  }

  // ML Model training intent (regex fallback)
  if (lowerMessage.includes('build') && (lowerMessage.includes('model') || lowerMessage.includes('linear') || lowerMessage.includes('regression'))) {
    return {
      operation: 'train_model',
      requiresClarification: false
    };
  }
  
  if (lowerMessage.includes('train') && lowerMessage.includes('model')) {
    return {
      operation: 'train_model',
      requiresClarification: false
    };
  }
  
  if (lowerMessage.includes('create') && lowerMessage.includes('model')) {
    return {
      operation: 'train_model',
      requiresClarification: false
    };
  }
  
  // Follow-up / conversational model requests (regex fallback)
  if (
    lowerMessage.includes('model') &&
    !isModelAdviceQuestion && // Avoid treating pure advice questions as train_model
    (
      lowerMessage.includes('less variance') ||
      lowerMessage.includes('lowest variance') ||
      lowerMessage.includes('reduce variance') ||
      lowerMessage.includes('lower variance') ||
      lowerMessage.includes('different model') ||
      lowerMessage.includes('another model') ||
      lowerMessage.includes('better fit') ||
      lowerMessage.includes('improve fit') ||
      lowerMessage.includes('for above') ||
      lowerMessage.includes('the above') ||
      lowerMessage.includes('previous model') ||
      lowerMessage.includes('random forest') ||
      lowerMessage.includes('randomforest') ||
      lowerMessage.includes('decision tree') ||
      lowerMessage.includes('decisiontree')
    )
  ) {
    return {
      operation: 'train_model',
      requiresClarification: false
    };
  }
  
  // If no pattern matched, return unknown
  return {
    operation: 'unknown',
    requiresClarification: false
  };
}

/**
 * Use AI to detect data ops intent for conversational queries
 */
async function detectDataOpsIntentWithAI(
  message: string,
  availableColumns: string[],
  chatHistory?: Message[],
  sessionDoc?: ChatDocument
): Promise<DataOpsIntent | null> {
  try {
    // Include all columns for better matching (up to 50 to avoid token issues)
    const columnsList = availableColumns.slice(0, 50).join(', ');
    const columnsListForMatching = availableColumns.map((col, idx) => `${idx + 1}. "${col}"`).join('\n');

    // Build chat history context with more detail
    const historyText = chatHistory && chatHistory.length
      ? chatHistory
          .slice(-15) // Keep last ~15 messages for better context
          .map((m, idx) => {
            const role = m.role.toUpperCase();
            const content = m.content;
            const timestamp = m.timestamp ? new Date(m.timestamp).toISOString() : '';
            return `[${idx + 1}] ${role}${timestamp ? ` (${timestamp})` : ''}: ${content}`;
          })
          .join('\n')
      : 'No previous messages.';
    
    const prompt = `You are an expert data operations assistant. Your job is to accurately infer what data operation the USER wants to perform on their dataset.

CRITICAL: You must match column names EXACTLY as they appear in the available columns list below. Column names are case-sensitive and may contain spaces, underscores, or special characters.

=== CHAT HISTORY (most recent messages are last) ===
${historyText}

=== USER'S CURRENT MESSAGE ===
"${message}"

=== AVAILABLE COLUMNS IN THE DATASET ===
${columnsListForMatching}

=== COLUMN NAME MATCHING RULES ===
1. ALWAYS match column names EXACTLY as they appear in the list above (case-sensitive)
2. If the user mentions a partial column name (e.g., "status" when the column is "order_status"), find the BEST MATCH from the available columns
3. For aggregation operations, if user says "all the other columns" or "all columns", set aggColumns to null (not an empty array)
4. Column names may contain:
   - Spaces: "First Name", "Customer Since"
   - Underscores: "order_id", "qty_ordered"
   - Special characters: "E Mail", "Discount_Percent"
   - Mixed case: "Name Prefix", "SSN"
5. When extracting column names from the user's message:
   - Look for exact matches first
   - Then look for partial matches (e.g., "status" matches "order_status" or "status")
   - Consider word boundaries (e.g., "id" should match "order_id" or "item_id", not "valid")
   - For multi-word columns, match all words (e.g., "first name" matches "First Name")

=== CONTEXT UNDERSTANDING ===
1. Pay close attention to the chat history - the user may be referring to previous operations
2. If the user says "yes", "ok", "do it", etc., look at the most recent ASSISTANT message to understand what they're confirming
3. If the user says "that column", "the above column", "it", etc., find the column from recent context
4. For follow-up questions, use the full conversation context to understand intent

When deciding the operation:
- Always interpret the USER's last message in the context of the conversation above.
- CRITICAL DEFAULT BEHAVIOR FOR OUTLIER OPERATIONS:
  • When user says "find outliers", "identify outliers", "detect outliers", "show outliers", or "what are the outliers" WITHOUT mentioning a specific column, set operation: "identify_outliers", column: null, requiresClarification: false
  • When user says "remove outliers", "treat outliers", "handle outliers", or "fix outliers" WITHOUT mentioning a specific column, set operation: "treat_outliers", column: null, requiresClarification: false
  • DO NOT ask for clarification - the system will automatically process ALL numeric columns by default
  • Only set requiresClarification: true if the user's intent is truly unclear or ambiguous
- The USER may reply with short confirmations like "yes", "yeah", "yep", "yup", "ok", "okay", "sure", "sounds good", "do it", "go ahead", "please do", etc.
  • In that case, look at the most recent ASSISTANT message(s).
  • If the ASSISTANT just suggested a specific data operation (for example: "Should I rename column 'XYZ' to 'tuko' now?" or "Do you want me to create a new column Total = Price + Tax?" or "Just to clarify, are you asking to rename the column 'XYZ' to 'tuko'?"),
    then treat the USER's confirmation as a request to execute that suggested operation.
  • Infer the correct operation type and parameters (column names, new column name, expression, target variable, features, etc.) from the ASSISTANT's suggestion and the overall context.
  • For rename operations: If the ASSISTANT asked "are you asking to rename the column 'XYZ' to 'tuko'?", extract oldColumnName: "XYZ" and newColumnName: "tuko".
- If the USER's last message itself directly describes an operation (for example: "create a new column X = A/B", "remove nulls from column Y", "rename column Sales to Revenue"),
  extract the appropriate operation and parameters from that message, using the column list above to match column names.
- If you cannot confidently determine a specific operation and its parameters, return "operation": "unknown" and set "clarificationMessage" to a concise follow-up question asking the user what they want you to do (and which columns/values to use).

Determine the intent and return JSON with this structure:
{
  "operation": "remove_nulls" | "preview" | "summary" | "convert_type" | "count_nulls" | "describe" | "create_derived_column" | "create_column" | "modify_column" | "normalize_column" | "remove_column" | "rename_column" | "remove_rows" | "add_row" | "aggregate" | "pivot" | "train_model" | "replace_value" | "identify_outliers" | "treat_outliers" | "revert" | "unknown",
  "column": "column_name" (if specific column mentioned for single-column operations, null otherwise),
  "oldColumnName": "OldColumnName" (if rename_column operation, the column to rename, null otherwise),
  "method": "delete" | "mean" | "median" | "mode" | "custom" (if operation is remove_nulls and method is specified, null otherwise),
  "customValue": any (if method is "custom", the value to use for imputation),
  "newColumnName": "NewColumnName" (if creating new column, null otherwise),
  "expression": "[Column1] + [Column2]" (if creating derived column, use [ColumnName] format, null otherwise),
  "defaultValue": any (if creating static column),
  "transformType": "add" | "subtract" | "multiply" | "divide" (if modifying existing column),
  "transformValue": number (if modifying existing column),
  "targetType": "numeric" | "string" | "date" | "percentage" | "boolean" (if convert_type operation, the target data type),
  "limit": number (if preview operation with first/last mode, the number of rows to show, e.g., "show first 10 rows" -> limit: 10, default: 50),
  "previewMode": "first" | "last" | "specific" | "range" (if preview operation, how to select rows),
  "previewStartRow": number (if previewMode is "specific" or "range", the starting row number, 1-based),
  "previewEndRow": number (if previewMode is "range", the ending row number, 1-based),
  "rowPosition": "first" | "last" | "keep_first" (if removing rows from start/end, or keeping only first N rows),
  "rowIndex": number (if removing specific row by index, 1-based),
  "rowCount": number (if removing multiple rows, e.g., "remove first 5 rows" or "remove last 3 rows"),
  "oldValue": any (if replace_value operation, the value to replace, null otherwise),
  "newValue": any (if replace_value operation, the value to replace with, null otherwise),
  "groupByColumn": "column_name" (if aggregate operation, the column to group by, null otherwise),
  "aggColumns": ["col1", "col2"] (if aggregate operation, columns to aggregate, null otherwise),
  "aggFunc": "sum" | "avg" | "mean" | "min" | "max" | "count" (if aggregate operation, default aggregation function, null otherwise),
  "aggFuncs": {"col1": "sum", "col2": "avg"} (if aggregate operation, per-column aggregation functions, null otherwise),
  "orderByColumn": "column_name" (if aggregate operation, column to sort results by, null otherwise),
  "orderByDirection": "asc" | "desc" (if aggregate operation, sort direction, null otherwise),
  "pivotIndex": "column_name" (if pivot operation, index column, null otherwise),
  "pivotValues": ["col1", "col2"] (if pivot operation, value columns, null otherwise),
  "pivotFuncs": {"col1": "sum", "col2": "avg"} (if pivot operation, per-column aggregation functions, null otherwise),
  "outlierMethod": "iqr" | "zscore" | "isolation_forest" | "local_outlier_factor" (if identify_outliers or treat_outliers operation, the detection method, default: "iqr"),
  "outlierThreshold": number (if identify_outliers or treat_outliers operation, threshold for detection - for zscore default: 3, for IQR default: 1.5, null otherwise),
  "treatmentMethod": "remove" | "cap" | "winsorize" | "transform" | "impute" (if treat_outliers operation, how to treat outliers, default: "remove"),
  "treatmentValue": "mean" | "median" | "mode" | "min" | "max" | number (if treatmentMethod is "impute" or "cap", the value to use, null otherwise),
  "requiresClarification": false,
  "clarificationMessage": null
}

Operations:
- "train_model": User wants to build/train/create a machine learning model (e.g., "build a linear model", "train a model", "create a model")
  * Extract modelType: "linear", "log_log", "logistic", "ridge", "lasso", "random_forest", "decision_tree", "gradient_boosting", "elasticnet", "svm", "knn", "polynomial", "bayesian", etc. (default: "linear")
  * Extract targetVariable: the target/dependent variable to predict
  * Extract features: array of independent variables/features
- "create_column": User wants to create new column with static/default value (e.g., "create column status with value active", "add column Notes", "create column Price with default 100")
  * Extract newColumnName: the name of the new column to create
  * Extract defaultValue: the static value to put in all rows (string, number, boolean, or null)
- "create_derived_column": User wants to create new column from expression (e.g., "create column XYZ = A + B", "add two columns X and Y", "create column XYZ with sum of PA and PAB", "create column xyz where if qty_ordered is more than the mean of qty_ordered then put it as 'outperform' otherwise 'notperforming'")
  * Extract newColumnName: the name of the new column to create
  * Extract expression: formula using [ColumnName] format (e.g., "[PA nGRP Adstocked] + [PAB nGRP Adstocked]")
  * For conditional logic (if/then/else), use np.where format: "np.where([Column] > [Column].mean(), 'value1', 'value2')"
  * If user says "sum of X and Y", expression should be "[X] + [Y]"
  * If user says "add X and Y", expression should be "[X] + [Y]"
  * If user says "if X > mean(X) then 'A' else 'B'", expression should be "np.where([X] > [X].mean(), 'A', 'B')"
- "modify_column": User wants to increase/decrease/multiply/divide an existing column
  * Extract column, transformType, transformValue
- "normalize_column": User wants to normalize or standardize an existing column
  * Extract column to normalize
- "remove_rows": User wants to remove first/last or a specific row(s), OR keep only first N rows
  * Examples: "remove last row", "delete row 5", "remove first 3 rows", "delete last 5 rows"
  * Special case: "keep only first N rows" or "keep first N rows" -> rowPosition: "keep_first", rowCount: N
  * Extract rowPosition (first/last/keep_first) if removing from start/end or keeping only first N
  * Extract rowIndex (1-based) if removing a specific row by index
  * Extract rowCount (number) if removing multiple rows (e.g., "remove first 5 rows" -> rowPosition: "first", rowCount: 5)
  * For "keep only first 100 rows" -> rowPosition: "keep_first", rowCount: 100
- "remove_column": User wants to remove/delete/drop a column (e.g., "remove column X", "delete the column Y", "drop column Z")
  * Extract column: the name of the column to remove
  * If column name is not specified, set requiresClarification to true
- "rename_column": User wants to rename/change the name of a column (e.g., "rename column X to Y", "change column name from X to Y", "rename the above column to Two", "change that column name to NewName")
  * Extract oldColumnName: the current name of the column to rename (can be from context like "above", "that", "it", "previous", or from assistant's clarification message)
  * Extract newColumnName: the new name for the column
  * If oldColumnName is not specified but user references "above", "that", "it", "previous", try to find from context
  * IMPORTANT: If the user replies "yes" to an assistant clarification like "are you asking to rename the column 'XYZ' to 'tuko'?", extract oldColumnName: "XYZ" and newColumnName: "tuko" from the assistant's message
  * Examples:
    - "rename column Sales to Revenue" → oldColumnName: "Sales", newColumnName: "Revenue"
    - "change the above column name to Two" → oldColumnName: (from context), newColumnName: "Two"
    - "rename that column to NewName" → oldColumnName: (from context), newColumnName: "NewName"
    - "change column name from OldName to NewName" → oldColumnName: "OldName", newColumnName: "NewName"
    - User says "yes" after assistant asks "are you asking to rename the column 'XYZ' to 'tuko'?" → oldColumnName: "XYZ", newColumnName: "tuko"
- "add_row": User wants to add/append a row (e.g., "add a new row", "append row at bottom")
- "count_nulls": User wants to count/null values (e.g., "how many nulls", "count missing values")
- "replace_value": User wants to replace a specific NON-NULL value with another. CRITICAL: 
  * DO NOT use this for null imputation - use "remove_nulls" instead
  * Only use for replacing specific values like "replace 0 with 1", "replace 'N/A' with 'Unknown'", etc.
  * If user mentions "null" in the context of filling/imputing, use "remove_nulls" NOT "replace_value"
  * Handle various phrasings:
  * "replace - with 0" or "replace '-' with 0"
  * "remove - and put 134.2 instead" or "remove - and replace with 134.2"
  * "change - to 0" or "convert - to 0"
  * "substitute - for 0" or "remove - and use 0"
  * "remove the value - and put 0" or "remove -, use 0"
  * Extract oldValue: the value to replace (e.g., "-", "N/A", "null", "empty")
  * Extract newValue: the value to replace with (e.g., 0, 134.2, null, "N/A")
  * Extract column: if a specific column is mentioned
- "describe": User wants general info about data (e.g., "how many rows", "describe the data", "what's in the dataset")
- "preview": User wants to see data. Handle various modes:
  * "first" mode: "show first 10 rows", "show me only first 5 rows", "display top 20 rows" -> previewMode: "first", limit: 10/5/20
  * "last" mode: "show last 5 rows", "show me the last 10 rows" -> previewMode: "last", limit: 5/10
  * "specific" mode: "show row 12", "show the 12th row", "show row number 28" -> previewMode: "specific", previewStartRow: 12/28
  * "range" mode: "show rows 12 to 28", "show rows 12-28", "show rows 12 through 28", "show me row from range 3 to 10 rows", "rows from range 3 to 10", "range 3 to 10 rows", "show me rows from range 5 to 15", "display rows from range 1 to 20" -> previewMode: "range", previewStartRow: 12/3/5/1, previewEndRow: 28/10/15/20
  * IMPORTANT: When user says "show me row from range 3 to 10 rows", this means rows 3 through 10 (inclusive), so previewMode: "range", previewStartRow: 3, previewEndRow: 10
  * If no specific mode is mentioned, default to "first" mode with limit: 50
- "summary": User wants statistics summary
- "remove_nulls": User wants to remove/handle nulls. CRITICAL: This is the CORRECT operation for ANY request involving null values, including:
  * "fill null values with mean/median/mode" → operation: "remove_nulls", method: "mean"/"median"/"mode", requiresClarification: false
  * "fill all null values with the mean of their respective columns" → operation: "remove_nulls", method: "mean", column: null (all columns), requiresClarification: false
  * "impute null values" → operation: "remove_nulls", method: "mean" (default), requiresClarification: false
  * "replace null with mean" → operation: "remove_nulls", method: "mean", requiresClarification: false
  * DO NOT use "replace_value" for null imputation - ALWAYS use "remove_nulls" with method
  * If user says "remove null" or "delete null" without specifying fill/impute, default to asking for clarification.
- "convert_type": User wants to convert column type
- "aggregate": User wants to group data by a column and summarize other columns
  * CRITICAL: Match column names EXACTLY from the available columns list above
  * Patterns: "aggregate by X", "aggregate X by Y using sum", "aggregate X on Y", "aggregate X, group by Y, order by Z DESC", "aggregate by Month column", "aggregate by Brand", "aggregate over X", "aggregate the whole data over X", "aggregate all data over X", "aggregate all the other columns by X", "aggregate all columns by X"
  * Extract groupByColumn: the column to group by - MUST match exactly from available columns (e.g., if available columns have "status", use "status", not "s" or "Status")
  * Extract aggColumns: 
    * If user specifies specific columns: array of column names (e.g., ["qty_ordered", "price"])
    * If user says "all the other columns", "all columns", "all data", "whole data": set to null (not empty array [])
    * If not specified: set to null (will auto-detect all numeric columns)
  * Extract aggFunc: default aggregation function ("sum", "avg", "mean", "min", "max", "count") - default is "sum"
  * Extract aggFuncs: per-column aggregation functions if user specifies different functions for different columns
  * Extract orderByColumn: optional column to sort results by (must match exactly from available columns)
  * Extract orderByDirection: "asc" or "desc" (default: "asc")
  * Examples with EXACT column matching:
    - User: "aggregate by Month" → groupByColumn: "Month" (if "Month" exists in columns), aggColumns: null
    - User: "aggregate qty_ordered by status using sum" → groupByColumn: "status", aggColumns: ["qty_ordered"], aggFunc: "sum"
      * MUST match "status" exactly (not "s", "Status", "STATUS")
      * MUST match "qty_ordered" exactly (not "qty", "quantity", "Qty Ordered")
    - User: "aggregate all the other columns by status using sum" → groupByColumn: "status", aggColumns: null, aggFunc: "sum"
      * "status" must match exactly from available columns
      * aggColumns: null (not [] or undefined) triggers auto-detection
    - User: "aggregate RISK_VOLUME on DEPOT" → groupByColumn: "DEPOT", aggColumns: ["RISK_VOLUME"]
    - User: "aggregate risk value, group by SKU Desc, order by risk value DESC" → groupByColumn: "SKU Desc", aggColumns: ["risk value"], orderByColumn: "risk value", orderByDirection: "desc"
    - User: "aggregate by Brand showing Total Sales (sum) and Avg Spend (avg)" → groupByColumn: "Brand", aggColumns: ["Sales", "Spend"], aggFuncs: {"Sales": "sum", "Spend": "avg"}
    - User: "aggregate the whole data over status" → groupByColumn: "status", aggColumns: null
    - User: "aggregate over status column" → groupByColumn: "status", aggColumns: null
    - User: "aggregate all data over status" → groupByColumn: "status", aggColumns: null
  * COMMON MISTAKES TO AVOID:
    - DO NOT extract partial column names (e.g., "s" instead of "status")
    - DO NOT use case variations (e.g., "Status" when column is "status")
    - DO NOT use empty array [] when user says "all columns" - use null instead
    - DO match column names EXACTLY as they appear in the available columns list
- "pivot": User wants to create a pivot table
  * Extract pivotIndex: the column to use as pivot index/rows (e.g., "Brand", "Month", "status")
  * Extract pivotValues: array of columns to show as metrics (e.g., ["Sales", "Spend", "ROI"]). If not specified or user says "over rest of the columns"/"over remaining columns", will default to all columns except the index column
  * Extract pivotFuncs: per-column aggregation functions if user specifies (e.g., {"Sales": "sum", "Spend": "sum", "ROI": "avg"})
  * Default aggregation function is "sum" if not specified
  * IMPORTANT: If user mentions "pivot" or "pivot table", this is ALWAYS a pivot operation, NOT a "create_column" operation, even if the message contains "create"
  * Examples: 
    - "create a pivot on Brand showing Sales, Spend, ROI"
    - "pivot on Month showing Total Sales (sum) and Avg Spend (avg)"
    - "pivot table for status" (extract pivotIndex: "status", pivotValues: [] - will use all columns)
    - "pivot for status" (extract pivotIndex: "status", pivotValues: [])
    - "create pivot table for status" (extract pivotIndex: "status", pivotValues: [])
    - "pivot by status" (extract pivotIndex: "status", pivotValues: [])
    - "create a pivot table for status over rest of the columns" (extract pivotIndex: "status", pivotValues: [] - use all other columns)
    - "pivot table for status over remaining columns" (extract pivotIndex: "status", pivotValues: [])
- "identify_outliers": User wants to find/identify/detect outliers in the data
  * CRITICAL: When user says "find outliers", "identify outliers", "detect outliers", "show outliers", or "what are the outliers" WITHOUT specifying a column, proceed immediately with analyzing ALL numeric columns (set column: null, requiresClarification: false)
  * DO NOT ask for clarification - the system will automatically analyze all numeric columns by default
  * Examples: "find outliers", "identify outliers", "detect outliers", "show outliers", "what are the outliers", "find outliers in column X", "detect outliers using IQR", "identify outliers with z-score"
  * Extract column: if a specific column is mentioned (null for all numeric columns - this is the DEFAULT and should be used when no column is specified)
  * Extract outlierMethod: "iqr" (default), "zscore", "isolation_forest", or "local_outlier_factor" based on user's preference
  * Extract outlierThreshold: if user specifies (e.g., "z-score > 2.5" -> threshold: 2.5, default: 3 for zscore, 1.5 for IQR)
  * IMPORTANT: Set requiresClarification: false for simple outlier identification requests like "find outliers" - proceed immediately
  * This operation only identifies and reports outliers, does not modify data
- "treat_outliers": User wants to remove/handle/fix/treat outliers in the data
  * CRITICAL: When user says "remove outliers", "treat outliers", "handle outliers", or "fix outliers" WITHOUT specifying a column, proceed immediately with treating outliers in ALL numeric columns (set column: null, requiresClarification: false)
  * DO NOT ask for clarification - the system will automatically treat outliers in all numeric columns by default
  * Examples: "remove outliers", "treat outliers", "handle outliers", "fix outliers", "remove outliers from column X", "cap outliers", "winsorize outliers", "replace outliers with mean"
  * Extract column: if a specific column is mentioned (null for all numeric columns - this is the DEFAULT and should be used when no column is specified)
  * Extract outlierMethod: "iqr" (default), "zscore", "isolation_forest", or "local_outlier_factor" based on user's preference
  * Extract outlierThreshold: if user specifies (default: 3 for zscore, 1.5 for IQR)
  * Extract treatmentMethod: "remove" (default), "cap", "winsorize", "transform", or "impute" based on user's request
  * Extract treatmentValue: if treatmentMethod is "impute" or "cap", extract "mean", "median", "mode", "min", "max", or a specific number
  * IMPORTANT: Set requiresClarification: false for simple outlier treatment requests like "remove outliers" - proceed immediately
  * Examples:
    - "remove outliers" -> treatmentMethod: "remove", outlierMethod: "iqr", column: null, requiresClarification: false
    - "cap outliers at 95th percentile" -> treatmentMethod: "cap", treatmentValue: 95 (or calculate percentile)
    - "replace outliers with median" -> treatmentMethod: "impute", treatmentValue: "median"
    - "impute outliers with mean" -> treatmentMethod: "impute", treatmentValue: "mean", outlierMethod: "iqr", column: null, requiresClarification: false
    - "impute outliers with median" -> treatmentMethod: "impute", treatmentValue: "median", outlierMethod: "iqr", column: null, requiresClarification: false
    - "impute outliers with mode" -> treatmentMethod: "impute", treatmentValue: "mode", outlierMethod: "iqr", column: null, requiresClarification: false
    - "winsorize outliers" -> treatmentMethod: "winsorize"
    - "remove outliers using z-score > 3" -> treatmentMethod: "remove", outlierMethod: "zscore", outlierThreshold: 3
- "revert": User wants to restore the data to its original form (e.g., "revert to original", "restore original data", "revert table", "go back to original")
  * This will load the original uploaded file and restore it, undoing all data operations
  * Examples: "revert to original", "restore original data", "revert table", "go back to original", "revert to original form"
- "unknown": Cannot determine intent

Return ONLY valid JSON, no other text.`;

    console.log(`🤖 Sending AI prompt for intent detection. Message: "${message}"`);
    console.log(`📋 Available columns (${availableColumns.length}): ${availableColumns.slice(0, 10).join(', ')}${availableColumns.length > 10 ? '...' : ''}`);
    
    const response = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        { 
          role: 'system', 
          content: 'You are an expert data operations assistant. You must return ONLY valid JSON. Match column names EXACTLY as they appear in the available columns list. Never truncate your response - always return complete JSON.' 
        },
        { role: 'user', content: prompt }
      ],
      temperature: 0.2, // Lower temperature for more consistent, accurate responses
      max_tokens: 600, // Increased from 200 to prevent truncation
      response_format: { type: 'json_object' }, // Force JSON output format
    });
    
    console.log(`🤖 AI response received, parsing...`);

    const content = response.choices[0]?.message?.content?.trim();
    const finishReason = response.choices[0]?.finish_reason;
    
    console.log(`🤖 AI raw response (first 300 chars):`, content?.substring(0, 300));
    console.log(`🤖 Finish reason: ${finishReason}`);
    
    if (!content) {
      console.log(`⚠️ AI returned empty content`);
      return null;
    }

    // Check if response was truncated
    if (finishReason === 'length') {
      console.warn(`⚠️ AI response was truncated (finish_reason: length). Response length: ${content.length}`);
      // Try to extract JSON anyway, but log warning
    }

    // Extract JSON from response (handle markdown code blocks and plain JSON)
    let jsonMatch = content.match(/\{[\s\S]*\}/);
    if (!jsonMatch) {
      // Try to find JSON object even if wrapped in text
      jsonMatch = content.match(/\{[\s\S]*?\}/);
    }
    
    if (!jsonMatch) {
      console.log(`⚠️ No JSON found in AI response. Full response:`, content);
      return null;
    }

    let parsed;
    try {
      parsed = JSON.parse(jsonMatch[0]);
      console.log(`✅ Successfully parsed AI intent JSON`);
    } catch (parseError) {
      console.error(`❌ Failed to parse AI JSON response:`, parseError);
      console.error(`❌ JSON string:`, jsonMatch[0].substring(0, 500));
      return null;
    }
    
    console.log(`🤖 Parsed AI intent:`, {
      operation: parsed.operation,
      groupByColumn: parsed.groupByColumn,
      aggColumns: parsed.aggColumns,
      aggFunc: parsed.aggFunc,
      column: parsed.column,
      oldColumnName: parsed.oldColumnName,
    });
    
    // Enhanced column name matching with better logging
    if (parsed.column) {
      const originalColumn = parsed.column;
      const matchedColumn = findMatchingColumn(parsed.column, availableColumns);
      if (matchedColumn && matchedColumn !== originalColumn) {
        console.log(`🔍 Column matched: "${originalColumn}" → "${matchedColumn}"`);
      } else if (!matchedColumn) {
        console.warn(`⚠️ Column "${originalColumn}" not found in available columns`);
      }
      parsed.column = matchedColumn || parsed.column;
    }
    
    // Map oldColumnName for rename operations
    if (parsed.oldColumnName) {
      const originalColumn = parsed.oldColumnName;
      const matchedColumn = findMatchingColumn(parsed.oldColumnName, availableColumns);
      if (matchedColumn && matchedColumn !== originalColumn) {
        console.log(`🔍 OldColumnName matched: "${originalColumn}" → "${matchedColumn}"`);
      }
      parsed.oldColumnName = matchedColumn || parsed.oldColumnName;
    }
    
    // Map groupByColumn for aggregation operations
    if (parsed.groupByColumn) {
      const originalColumn = parsed.groupByColumn;
      const matchedColumn = findMatchingColumn(parsed.groupByColumn, availableColumns);
      if (matchedColumn && matchedColumn !== originalColumn) {
        console.log(`🔍 groupByColumn matched: "${originalColumn}" → "${matchedColumn}"`);
      } else if (!matchedColumn) {
        console.warn(`⚠️ groupByColumn "${originalColumn}" not found in available columns. Available: ${availableColumns.slice(0, 5).join(', ')}...`);
      }
      parsed.groupByColumn = matchedColumn || parsed.groupByColumn;
    }
    
    // Map aggColumns array for aggregation operations
    if (parsed.aggColumns && Array.isArray(parsed.aggColumns)) {
      parsed.aggColumns = parsed.aggColumns.map((col: string) => {
        const matched = findMatchingColumn(col, availableColumns);
        if (matched && matched !== col) {
          console.log(`🔍 aggColumn matched: "${col}" → "${matched}"`);
        }
        return matched || col;
      });
    }

    // Extract method for remove_nulls operation if not explicitly provided
    let method: 'delete' | 'mean' | 'median' | 'mode' | 'custom' | undefined;
    let customValue: any;
    
    if (parsed.operation === 'remove_nulls') {
      const lowerMsg = message.toLowerCase();
      if (lowerMsg.includes('fill') || lowerMsg.includes('impute') || lowerMsg.includes('replace')) {
        // This is an imputation request, not deletion
        if (lowerMsg.includes('mean') || lowerMsg.includes('average')) {
          method = 'mean';
        } else if (lowerMsg.includes('median')) {
          method = 'median';
        } else if (lowerMsg.includes('mode') || lowerMsg.includes('most frequent')) {
          method = 'mode';
        } else {
          // Check for custom value (number or string)
          const customValueResult = extractCustomValue(message);
          if (customValueResult.found) {
            method = 'custom';
            customValue = customValueResult.value;
          } else if (lowerMsg.includes('custom')) {
            // User mentioned "custom" but didn't specify value
            method = 'custom';
            customValue = undefined;
          }
        }
      } else if (lowerMsg.includes('delete') || lowerMsg.includes('remove')) {
        method = 'delete';
      }
    }

    // Build the intent object with all mapped columns
    const intent: DataOpsIntent = {
      operation: parsed.operation || 'unknown',
      column: parsed.column,
      method: method || parsed.method,
      customValue: customValue !== undefined ? customValue : parsed.customValue,
      newColumnName: parsed.newColumnName,
      expression: parsed.expression,
      defaultValue: parsed.defaultValue,
      transformType: parsed.transformType,
      transformValue: parsed.transformValue,
      targetType: parsed.targetType,
      limit: parsed.limit,
      previewMode: parsed.previewMode,
      previewStartRow: parsed.previewStartRow,
      previewEndRow: parsed.previewEndRow,
      rowPosition: parsed.rowPosition,
      rowIndex: parsed.rowIndex,
      rowCount: parsed.rowCount,
      oldValue: parsed.oldValue,
      newValue: parsed.newValue,
      oldColumnName: parsed.oldColumnName,
      modelType: parsed.modelType,
      targetVariable: parsed.targetVariable,
      features: parsed.features,
      requiresClarification: method ? false : (parsed.requiresClarification || false),
      clarificationType: parsed.clarificationType,
      clarificationMessage: parsed.clarificationMessage,
    };
    
    // Add aggregation-specific fields with mapped columns
    if (parsed.groupByColumn) {
      intent.groupByColumn = parsed.groupByColumn;
    }
    if (parsed.aggColumns !== undefined) {
      intent.aggColumns = parsed.aggColumns; // Already mapped above
    }
    if (parsed.aggFunc) {
      intent.aggFunc = parsed.aggFunc;
    }
    if (parsed.aggFuncs) {
      intent.aggFuncs = parsed.aggFuncs;
    }
    if (parsed.orderByColumn) {
      const matchedOrderBy = findMatchingColumn(parsed.orderByColumn, availableColumns);
      intent.orderByColumn = matchedOrderBy || parsed.orderByColumn;
    }
    if (parsed.orderByDirection) {
      intent.orderByDirection = parsed.orderByDirection;
    }
    
    // Add pivot-specific fields
    if (parsed.pivotIndex) {
      const matchedPivotIndex = findMatchingColumn(parsed.pivotIndex, availableColumns);
      intent.pivotIndex = matchedPivotIndex || parsed.pivotIndex;
    }
    if (parsed.pivotValues) {
      intent.pivotValues = parsed.pivotValues.map((col: string) => {
        const matched = findMatchingColumn(col, availableColumns);
        return matched || col;
      });
    }
    if (parsed.pivotFuncs) {
      intent.pivotFuncs = parsed.pivotFuncs;
    }
    
    console.log(`✅ Final mapped intent:`, {
      operation: intent.operation,
      groupByColumn: intent.groupByColumn,
      aggColumns: intent.aggColumns,
      column: intent.column,
    });
    
    return intent;
  } catch (error) {
    console.error('Error in AI intent detection:', error);
    return null;
  }
}

/**
 * Handle clarification response
 */
function handleClarificationResponse(
  message: string,
  pendingOp: { operation: string; column?: string },
  availableColumns: string[],
  dataSummary: DataSummary
): DataOpsIntent {
  const lowerMessage = message.toLowerCase().trim();
  
  if (pendingOp.operation === 'remove_rows') {
    const lower = message.toLowerCase();
    if (lower.includes('first') || lower.includes('top')) {
      return {
        operation: 'remove_rows',
        rowPosition: 'first',
        requiresClarification: false,
      };
    }
    if (lower.includes('last') || lower.includes('bottom')) {
      return {
        operation: 'remove_rows',
        rowPosition: 'last',
        requiresClarification: false,
      };
    }
    const indexMatch = lower.match(/row\s*(\d+)/);
    if (indexMatch) {
      return {
        operation: 'remove_rows',
        rowIndex: parseInt(indexMatch[1], 10),
        requiresClarification: false,
      };
    }
  }

  if (pendingOp.operation === 'remove_nulls') {
    // Check if this is a column specification
    if (!pendingOp.column) {
      // Check if user is specifying a method (for entire dataset)
      // This handles the case where user said "entire dataset" and now responds with method
      if (lowerMessage.includes('delete') || lowerMessage.includes('remove') || lowerMessage.includes('option a')) {
        return {
          operation: 'remove_nulls',
          method: 'delete',
          requiresClarification: false
        };
      } else if (lowerMessage.includes('mean') || lowerMessage.includes('average') || lowerMessage.includes('impute with mean')) {
        return {
          operation: 'remove_nulls',
          method: 'mean',
          requiresClarification: false
        };
      } else if (lowerMessage.includes('median') || lowerMessage.includes('impute with median')) {
        return {
          operation: 'remove_nulls',
          method: 'median',
          requiresClarification: false
        };
      } else if (lowerMessage.includes('mode') || lowerMessage.includes('most frequent') || lowerMessage.includes('impute with mode')) {
        return {
          operation: 'remove_nulls',
          method: 'mode',
          requiresClarification: false
        };
      } else if (lowerMessage.includes('custom')) {
        // Check if custom value is specified
        const customValueResult = extractCustomValue(message);
        if (customValueResult.found) {
          return {
            operation: 'remove_nulls',
            method: 'custom',
            customValue: customValueResult.value,
            requiresClarification: false
          };
        } else {
          // User said "custom" but didn't specify value
          return {
            operation: 'remove_nulls',
            method: 'custom',
            requiresClarification: true,
            clarificationType: 'method',
            clarificationMessage: 'What value would you like to use to fill null values? (e.g., 0, "N/A", "Unknown", etc.)'
          };
        }
      }
      
      // User is specifying column
      const mentionedColumn = findMentionedColumn(message, availableColumns);
      if (mentionedColumn) {
        return {
          operation: 'remove_nulls',
          column: mentionedColumn,
          requiresClarification: true,
          clarificationType: 'method',
          clarificationMessage: `How do you want to deal with null values in "${mentionedColumn}"?\n\nOption A: Delete Row\nOption B: Impute with mean/median/mode/custom value`
        };
      } else if (lowerMessage.includes('entire') || lowerMessage.includes('all') || lowerMessage.includes('whole')) {
        return {
          operation: 'remove_nulls',
          requiresClarification: true,
          clarificationType: 'method',
          clarificationMessage: 'How do you want to deal with null values?\n\nOption A: Delete Row\nOption B: Impute with mean/median/mode/custom value'
        };
      }
    } else {
      // User is specifying method for a specific column
      if (lowerMessage.includes('delete') || lowerMessage.includes('remove') || lowerMessage.includes('option a')) {
        return {
          operation: 'remove_nulls',
          column: pendingOp.column,
          method: 'delete',
          requiresClarification: false
        };
      } else if (lowerMessage.includes('mean') || lowerMessage.includes('average') || lowerMessage.includes('impute with mean')) {
        return {
          operation: 'remove_nulls',
          column: pendingOp.column,
          method: 'mean',
          requiresClarification: false
        };
      } else if (lowerMessage.includes('median') || lowerMessage.includes('impute with median')) {
        return {
          operation: 'remove_nulls',
          column: pendingOp.column,
          method: 'median',
          requiresClarification: false
        };
      } else if (lowerMessage.includes('mode') || lowerMessage.includes('most frequent') || lowerMessage.includes('impute with mode')) {
        return {
          operation: 'remove_nulls',
          column: pendingOp.column,
          method: 'mode',
          requiresClarification: false
        };
      } else if (lowerMessage.includes('custom')) {
        // Check if custom value is specified
        const customValueResult = extractCustomValue(message);
        if (customValueResult.found) {
        return {
          operation: 'remove_nulls',
          column: pendingOp.column,
          method: 'custom',
            customValue: customValueResult.value,
          requiresClarification: false
        };
        } else {
          // User said "custom" but didn't specify value
          return {
            operation: 'remove_nulls',
            column: pendingOp.column,
            method: 'custom',
            requiresClarification: true,
            clarificationType: 'method',
            clarificationMessage: `What value would you like to use to fill null values in "${pendingOp.column}"? (e.g., 0, "N/A", "Unknown", etc.)`
          };
        }
      }
    }
  }
  
  // Check if user is providing a custom value (for when method was already set to 'custom' in a previous clarification)
  // This handles cases where user said "custom" and we asked "what value?", and now they're providing the value
  if (pendingOp.operation === 'remove_nulls') {
    const customValueResult = extractCustomValue(message);
    // Only treat as custom value if:
    // 1. We found a value in the message, AND
    // 2. The message doesn't look like they're choosing a different method (mean/median/mode/delete)
    const looksLikeMethodChoice = lowerMessage.includes('mean') || lowerMessage.includes('median') || 
                                   lowerMessage.includes('mode') || lowerMessage.includes('delete') ||
                                   lowerMessage.includes('remove') || lowerMessage.includes('option');
    
    if (customValueResult.found && !looksLikeMethodChoice) {
      return {
        operation: 'remove_nulls',
        column: pendingOp.column,
        method: 'custom',
        customValue: customValueResult.value,
        requiresClarification: false
      };
    }
  }
  
  // Default: still need clarification
  return {
    operation: 'remove_nulls',
    column: pendingOp.column,
    requiresClarification: true,
    clarificationType: 'method',
    clarificationMessage: 'Please specify: Delete Row or Impute with mean/median/mode/custom value'
  };
}

/**
 * Extract custom value from message (handles numbers and strings)
 * Examples: "fill nulls with 0", "fill nulls with the 132.45", "fill nulls with 'N/A'", "fill nulls with N/A", "fill nulls with Unknown"
 */
function extractCustomValue(message: string): { value: any; found: boolean } {
  const lowerMessage = message.toLowerCase();
  
  // Patterns to match custom value specifications
  // "with 0", "with the 132.45", "with 'N/A'", "with N/A", "with Unknown", "as 0", "as 'N/A'", etc.
  
  // Try number pattern first - handle "with 0", "with the 132.45", "with value 123.45", etc.
  // Pattern: (with|as|value|using|to) (optional: the|a|an) (number)
  // Match both "with 132.45" and "with the 132.45"
  // Use word boundaries to ensure we match the right "with"
  const numberPatterns = [
    /\b(?:with|as|value|using|to)\s+(?:the|a|an)\s+(-?\d+\.?\d*)/i,  // "with the 132.45"
    /\b(?:with|as|value|using|to)\s+(-?\d+\.?\d*)/i,  // "with 132.45"
  ];
  
  for (const pattern of numberPatterns) {
    const numberMatch = message.match(pattern);
    if (numberMatch && numberMatch[1]) {
      const numStr = numberMatch[1].trim();
      const num = parseFloat(numStr);
      if (!isNaN(num) && isFinite(num)) {
        return { value: num, found: true };
      }
    }
  }
  
  // Try quoted string pattern - "with 'N/A'", "with \"Unknown\"", "with the 'value'"
  const quotedMatch = message.match(/(?:with|as|value|using|to)\s+(?:the|a|an)?\s*['"]([^'"]+)['"]/i);
  if (quotedMatch) {
    return { value: quotedMatch[1], found: true };
  }
  
  // Try unquoted string pattern (but exclude common method words and articles)
  // This should come last to avoid matching "the" or "a" as values
  // Match patterns like "with N/A", "with Unknown", but NOT "with the" or "with a"
  const unquotedPatterns = [
    /(?:with|as|value|using|to)\s+(?:the|a|an)\s+([A-Za-z][A-Za-z0-9\s_-]+?)(?:\s|$|,|\.|;|in|for|from)/i,  // "with the N/A"
    /(?:with|as|value|using|to)\s+([A-Za-z][A-Za-z0-9\s_-]+?)(?:\s|$|,|\.|;|in|for|from)/i,  // "with N/A"
  ];
  
  for (const pattern of unquotedPatterns) {
    const unquotedMatch = message.match(pattern);
    if (unquotedMatch) {
      const potentialValue = unquotedMatch[1].trim();
      // Exclude method keywords and articles
      const methodKeywords = ['mean', 'median', 'mode', 'custom', 'delete', 'remove', 'fill', 'impute', 'replace', 'the', 'a', 'an', 'null', 'value', 'values'];
      if (potentialValue && !methodKeywords.includes(potentialValue.toLowerCase())) {
        return { value: potentialValue, found: true };
      }
    }
  }
  
  return { value: undefined, found: false };
}

/**
 * Find mentioned column in message
 * Improved to handle cases like "Emami 7 Oils TOM" matching "Emami 7 Oils TOM" instead of "Emami 7 Oils nGRP"
 */
function findMentionedColumn(message: string, availableColumns: string[]): string | undefined {
  const lowerMessage = message.toLowerCase();
  
  // Extract potential column name from message by removing common operation words
  // This helps isolate the column name better
  const operationWords = ['normalize', 'normalise', 'standardize', 'remove', 'delete', 'drop', 
                          'create', 'add', 'make', 'modify', 'change', 'update', 'convert', 
                          'replace', 'fill', 'count', 'show', 'display', 'get', 'find'];
  let cleanedMessage = lowerMessage;
  for (const opWord of operationWords) {
    cleanedMessage = cleanedMessage.replace(new RegExp(`\\b${opWord}\\b`, 'gi'), '').trim();
  }
  // Remove common words that might interfere, but preserve important words like "TOM", "nGRP", etc.
  cleanedMessage = cleanedMessage.replace(/\b(the|a|an|column|columns|value|values|with|to|by|for|in|on|at)\b/gi, '').trim();
  
  // If cleaned message is too short or empty, use original message
  if (cleanedMessage.length < 3) {
    cleanedMessage = lowerMessage;
  }
  
  // Try exact match first (case-insensitive, ignoring extra spaces)
  for (const col of availableColumns) {
    const colLower = col.toLowerCase().trim();
    const colNormalized = colLower.replace(/\s+/g, ' ');
    const msgNormalized = cleanedMessage.replace(/\s+/g, ' ');
    
    // Exact match (normalized)
    if (colNormalized === msgNormalized) {
      return col;
    }
    
    // Exact match with original message (if column name appears as-is)
    if (lowerMessage.includes(colLower) && colLower.length >= 3) {
      // Check if it's a word-boundary match (not just substring)
      const wordBoundaryRegex = new RegExp(`\\b${colLower.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
      if (wordBoundaryRegex.test(message)) {
        return col;
      }
    }
  }
  
  // Try word-boundary matching - all words from search term must appear in column
  const searchWords = cleanedMessage.split(/\s+/).filter(w => w.length >= 1); // Allow single char words like "7"
  if (searchWords.length > 0) {
    // Sort columns by length (longest first) to prioritize more specific matches
  const sortedColumns = [...availableColumns].sort((a, b) => b.length - a.length);
  
    // First, try to find columns where ALL words match (perfect match)
  for (const col of sortedColumns) {
      const colLower = col.toLowerCase();
      let allWordsMatch = true;
      let matchCount = 0;
      
      for (const word of searchWords) {
        // Use word boundary regex to ensure we match complete words
        const wordRegex = new RegExp(`\\b${word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
        if (wordRegex.test(colLower)) {
          matchCount++;
        } else {
          // If word doesn't match as a word boundary, check if it's a substring
          // but only if the word is significant (length >= 2)
          if (word.length >= 2 && colLower.includes(word)) {
            matchCount++;
          } else {
            allWordsMatch = false;
            break;
          }
        }
      }
      
      // If all words match, return this column immediately
      if (allWordsMatch && matchCount === searchWords.length) {
        return col;
      }
    }
    
    // If no perfect match, try to find column with highest word match count
    // Prioritize columns that match the LAST word (often the distinguishing part like "TOM" vs "nGRP")
    const lastWord = searchWords[searchWords.length - 1];
    let bestMatch: { col: string; score: number } | null = null;
    
    for (const col of sortedColumns) {
      const colLower = col.toLowerCase();
      let matchCount = 0;
      let lastWordMatches = false;
      
      for (let i = 0; i < searchWords.length; i++) {
        const word = searchWords[i];
        const wordRegex = new RegExp(`\\b${word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
        const matches = wordRegex.test(colLower) || (word.length >= 2 && colLower.includes(word));
        
        if (matches) {
          matchCount++;
          // Check if this is the last word
          if (i === searchWords.length - 1) {
            lastWordMatches = true;
          }
        }
      }
      
      // Calculate score: 
      // - Base score: percentage of words matched
      // - Bonus: if last word matches (critical for distinguishing "TOM" vs "nGRP")
      // - Bonus: longer column names (more specific)
      let score = (matchCount / searchWords.length) * 100;
      if (lastWordMatches) {
        score += 50; // Big bonus for matching the last word
      }
      score += (col.length / 100); // Small bonus for longer names
      
      if (matchCount > 0 && (!bestMatch || score > bestMatch.score)) {
        bestMatch = { col, score };
      }
    }
    
    // Require at least 50% word match, or if last word matches, require at least 30%
    const minScore = lastWord && bestMatch?.col.toLowerCase().includes(lastWord.toLowerCase()) ? 30 : 50;
    if (bestMatch && bestMatch.score >= minScore) {
      return bestMatch.col;
    }
  }
  
  // Fallback: Try substring match, but only for longer substrings (>= 5 chars)
  // This prevents matching "Emami 7 Oils" when user says "Emami 7 Oils TOM"
  const sortedColumns = [...availableColumns].sort((a, b) => b.length - a.length);
  for (const col of sortedColumns) {
    const colLower = col.toLowerCase();
    // Only match if the substring is significant (>= 5 chars) or if it's an exact word match
    if (lowerMessage.includes(colLower) && (colLower.length >= 5 || 
        new RegExp(`\\b${colLower.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i').test(message))) {
      return col;
    }
  }
  
  // Try column number (e.g., "column 3", "column#3")
  const colNumMatch = message.match(/column\s*#?\s*(\d+)/i);
  if (colNumMatch) {
    const colIndex = parseInt(colNumMatch[1], 10) - 1;
    if (colIndex >= 0 && colIndex < availableColumns.length) {
      return availableColumns[colIndex];
    }
  }
  
  return undefined;
}

/**
 * Find matching column (fuzzy match)
 */
function findMatchingColumn(searchName: string, availableColumns: string[]): string | undefined {
  const normalized = searchName.toLowerCase().replace(/[\s_-]/g, '');
  
  // Prefer exact matches first
  for (const col of availableColumns) {
    const colNormalized = col.toLowerCase().replace(/[\s_-]/g, '');
    if (colNormalized === normalized) {
      return col;
    }
  }
  
  // Fallback to columns that contain the search term
  for (const col of availableColumns) {
    const colNormalized = col.toLowerCase().replace(/[\s_-]/g, '');
    if (colNormalized.includes(normalized)) {
      return col;
    }
  }
  
  return undefined;
}

function normalizeNumericValue(value: any): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }

  const parsed = parseFloat(String(value).replace(/[,%\$]/g, '').trim());
  return Number.isNaN(parsed) ? null : parsed;
}

/**
 * Extract column details (name and default value) using AI
 */
async function extractColumnDetails(
  message: string
): Promise<{ columnName: string; defaultValue?: any } | null> {
  try {
    const prompt = `Extract the column name and default value from the user's query for creating a new column with a static value.

User query: "${message}"

Extract:
1. columnName: The name of the new column to create
2. defaultValue: The value to put in the column (can be string, number, boolean, or null)

Examples:
- "create a new column status and put the value active in it" → columnName: "status", defaultValue: "active"
- "add column Notes with value empty" → columnName: "Notes", defaultValue: ""
- "create column Price with default 100" → columnName: "Price", defaultValue: 100
- "add column Active with value true" → columnName: "Active", defaultValue: true
- "create column Comments" → columnName: "Comments", defaultValue: null

Return JSON:
{
  "columnName": "ColumnName",
  "defaultValue": "value" | number | boolean | null
}`;

    const response = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        { role: 'system', content: 'You extract column names and default values from natural language. Return only valid JSON.' },
        { role: 'user', content: prompt }
      ],
      temperature: 0.2,
      max_tokens: 200,
    });

    const content = response.choices[0]?.message?.content?.trim();
    if (!content) return null;

    const jsonMatch = content.match(/\{[\s\S]*\}/);
    if (!jsonMatch) return null;

    const parsed = JSON.parse(jsonMatch[0]);
    
    if (parsed.columnName) {
      return {
        columnName: parsed.columnName.trim(),
        defaultValue: parsed.defaultValue !== undefined ? parsed.defaultValue : null,
      };
    }
    
    return null;
  } catch (error) {
    console.error('Error extracting column details:', error);
    return null;
  }
}

/**
 * Extract previous model parameters from chat history
 */
function extractPreviousModelParams(chatHistory: Message[]): { targetVariable?: string; features?: string[]; modelType?: string } | null {
  if (!chatHistory || chatHistory.length === 0) {
    console.log('📋 No chat history provided for context extraction');
    return null;
  }
  
  console.log(`📋 Searching through ${chatHistory.length} messages for previous model context`);
  
  // Look backwards through chat history for the most recent model result
  for (let i = chatHistory.length - 1; i >= 0; i--) {
    const msg = chatHistory[i];
    if (msg.role === 'assistant' && msg.content) {
      const content = msg.content;
      
      // Check if this is a model result (contains "Model Summary" and "Target Variable")
      if (content.includes('Model Summary') && content.includes('Target Variable')) {
        console.log(`📋 Found potential model result at message index ${i}`);
        
        // Try multiple patterns for target variable (handle markdown format with dashes)
        const targetMatch = content.match(/[-*]\s*Target Variable:\s*([^\n]+)/i) || 
                           content.match(/Target Variable:\s*([^\n]+)/i) ||
                           content.match(/target[:\s]+([^\n]+)/i);
        
        // Try multiple patterns for features (handle markdown format with dashes)
        const featuresMatch = content.match(/[-*]\s*Features:\s*([^\n]+)/i) ||
                             content.match(/Features:\s*([^\n]+)/i) ||
                             content.match(/features[:\s]+([^\n]+)/i);
        
        // Try multiple patterns for model type
        const modelTypeMatch = content.match(/trained a (\w+(?:\s+\w+)?)\s+model/i) ||
                              content.match(/successfully trained a (\w+(?:\s+\w+)?)\s+model/i) ||
                              content.match(/(\w+(?:\s+\w+)?)\s+model/i) ||
                              content.match(/model type[:\s]+(\w+(?:\s+\w+)?)/i);
        
        if (targetMatch && featuresMatch) {
          const targetVariable = targetMatch[1].trim();
          const featuresStr = featuresMatch[1].trim();
          // Parse features (comma-separated list, handle "&" and "and")
          const features = featuresStr
            .split(/[,&]| and /i)
            .map(f => f.trim())
            .filter(f => f.length > 0);
          
          let modelType: string | undefined;
          if (modelTypeMatch) {
            let modelTypeStr = modelTypeMatch[1].trim().toLowerCase();
            // Normalize model type names
            modelTypeStr = modelTypeStr.replace(/\s+/g, '_');
            // Handle variations
            if (modelTypeStr.includes('random') && modelTypeStr.includes('forest')) {
              modelTypeStr = 'random_forest';
            } else if (modelTypeStr.includes('decision') && modelTypeStr.includes('tree')) {
              modelTypeStr = 'decision_tree';
            }
            
            if (['linear', 'logistic', 'ridge', 'lasso', 'random_forest', 'decision_tree'].includes(modelTypeStr)) {
              modelType = modelTypeStr;
            }
          }
          
          console.log(`✅ Found previous model in chat history: target="${targetVariable}", features=[${features.join(', ')}], type=${modelType || 'unknown'}`);
          return { targetVariable, features, modelType };
        } else {
          console.log(`⚠️ Found model result but couldn't parse: targetMatch=${!!targetMatch}, featuresMatch=${!!featuresMatch}`);
        }
      }
    }
  }
  
  console.log('📋 No previous model found in chat history');
  return null;
}

/**
 * Extract ML model details using AI
 */
async function extractMLModelDetails(
  message: string,
  availableColumns: string[],
  chatHistory?: Message[],
  previousModelParams?: { targetVariable?: string; features?: string[]; modelType?: string }
): Promise<{ modelType?: 'linear' | 'log_log' | 'logistic' | 'ridge' | 'lasso' | 'random_forest' | 'decision_tree' | 'gradient_boosting' | 'elasticnet' | 'svm' | 'knn' | 'polynomial' | 'bayesian' | 'quantile' | 'poisson' | 'gamma' | 'tweedie' | 'extra_trees' | 'xgboost' | 'lightgbm' | 'catboost' | 'gaussian_process' | 'mlp' | 'multinomial_logistic' | 'naive_bayes_gaussian' | 'naive_bayes_multinomial' | 'naive_bayes_bernoulli' | 'lda' | 'qda'; targetVariable?: string; features?: string[] } | null> {
  try {
    const columnsList = availableColumns.slice(0, 30).join(', ');
    
    // Build context about previous model if available
    let previousModelContext = '';
    if (previousModelParams && previousModelParams.targetVariable) {
      previousModelContext = `\n\nPREVIOUS MODEL CONTEXT (if user references "for above", "that model", etc., use these parameters):\n`;
      previousModelContext += `- Target Variable: ${previousModelParams.targetVariable}\n`;
      previousModelContext += `- Features: ${previousModelParams.features?.join(', ') || 'N/A'}\n`;
      if (previousModelParams.modelType) {
        previousModelContext += `- Previous Model Type: ${previousModelParams.modelType}\n`;
      }
    }
    
    // Check if user is referencing previous model
    const messageLower = message.toLowerCase();
    const referencesPrevious = messageLower.includes('for above') || 
                               messageLower.includes('for the above') ||
                               messageLower.includes('that model') || 
                               messageLower.includes('previous model') ||
                               messageLower.includes('the above') ||
                               messageLower.includes('above model') ||
                               (messageLower.includes('same') && messageLower.includes('model')) ||
                               (messageLower.includes('less variance') && previousModelParams) ||
                               (messageLower.includes('reduce variance') && previousModelParams);
    
    // Determine model type based on user request
    let suggestedModelType = 'linear';
    if (messageLower.includes('log') && (messageLower.includes('log log') || messageLower.includes('log-log') || messageLower.includes('logarithmic'))) {
      suggestedModelType = 'log_log';
    } else if (messageLower.includes('less variance') || messageLower.includes('reduce variance') || messageLower.includes('lower variance')) {
      // Ridge or Lasso for variance reduction
      suggestedModelType = 'ridge'; // Default to Ridge for variance reduction
    } else if (messageLower.includes('ridge')) {
      suggestedModelType = 'ridge';
    } else if (messageLower.includes('lasso')) {
      suggestedModelType = 'lasso';
    } else if (messageLower.includes('random forest') || messageLower.includes('randomforest')) {
      suggestedModelType = 'random_forest';
    } else if (messageLower.includes('decision tree') || messageLower.includes('decisiontree')) {
      suggestedModelType = 'decision_tree';
    } else if (messageLower.includes('logistic')) {
      suggestedModelType = 'logistic';
    }
    
    const prompt = `Extract ML model parameters from the user's query.${previousModelContext}

User query: "${message}"

Available columns: ${columnsList}

${referencesPrevious && previousModelParams ? 'IMPORTANT: The user is referencing a previous model. Use the previous model parameters (target variable and features) from the context above unless they explicitly specify different ones.' : ''}

Extract:
1. modelType: "linear", "log_log", "logistic", "ridge", "lasso", "random_forest", "decision_tree", "gradient_boosting", "elasticnet", "svm", "knn", "polynomial", "bayesian", etc.
   ${messageLower.includes('less variance') || messageLower.includes('reduce variance') ? '   → If user wants "less variance", use "ridge" or "lasso" (prefer "ridge")' : ''}
   ${messageLower.includes('log') && (messageLower.includes('log') || messageLower.includes('logarithmic')) ? '   → If user mentions "log log", "log-log", or "logarithmic" model, use "log_log"' : ''}
   ${suggestedModelType !== 'linear' ? `   → Suggested: "${suggestedModelType}" based on user query` : ''}
2. targetVariable: The target/dependent variable to predict
   ${referencesPrevious && previousModelParams?.targetVariable ? `   → If referencing previous model, use: "${previousModelParams.targetVariable}"` : ''}
3. features: Array of independent variables/features
   ${referencesPrevious && previousModelParams?.features ? `   → If referencing previous model, use: [${previousModelParams.features.map(f => `"${f}"`).join(', ')}]` : ''}

Examples:
- "Build a linear model choosing Sales as target variable and Price, Marketing as independent variables"
  → modelType: "linear", targetVariable: "Sales", features: ["Price", "Marketing"]
- "for above can you choose a model with less variance" (when previous model had Target: Sales, Features: Price, Marketing)
  → modelType: "ridge", targetVariable: "Sales", features: ["Price", "Marketing"]
- "Create a linear model with PA TOM as target and PA nGRP Adstocked, PAB nGRP Adstocked as features"
  → modelType: "linear", targetVariable: "PA TOM", features: ["PA nGRP Adstocked", "PAB nGRP Adstocked"]
- "Train a random forest model to predict Revenue using Price, Marketing, Season"
  → modelType: "random_forest", targetVariable: "Revenue", features: ["Price", "Marketing", "Season"]

Return JSON:
{
  "modelType": "linear" | "log_log" | "logistic" | "ridge" | "lasso" | "random_forest" | "decision_tree" | "gradient_boosting" | "elasticnet" | "svm" | "knn" | "polynomial" | "bayesian" | etc.,
  "targetVariable": "ColumnName",
  "features": ["Column1", "Column2", "Column3"]
}`;

    const response = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        { role: 'system', content: 'You extract ML model parameters from natural language. Return only valid JSON.' },
        { role: 'user', content: prompt }
      ],
      temperature: 0.2,
      max_tokens: 300,
    });

    const content = response.choices[0]?.message?.content?.trim();
    if (!content) return null;

    const jsonMatch = content.match(/\{[\s\S]*\}/);
    if (!jsonMatch) return null;

    const parsed = JSON.parse(jsonMatch[0]);
    
    if (parsed.targetVariable && parsed.features && Array.isArray(parsed.features)) {
      return {
        modelType: parsed.modelType || 'linear',
        targetVariable: parsed.targetVariable.trim(),
        features: parsed.features.map((f: string) => f.trim()).filter((f: string) => f.length > 0),
      };
    }
    
    return null;
  } catch (error) {
    console.error('Error extracting ML model details:', error);
    return null;
  }
}

/**
 * Format ML model response
 */
function formatMLModelResponse(
  result: any,
  modelType: string,
  targetCol: string,
  features: string[]
): string {
  let answer = `I've successfully trained a ${modelType.replace('_', ' ')} model.\n\n`;
  
  answer += `**Model Summary:**\n`;
  answer += `- Target Variable: ${targetCol}\n`;
  answer += `- Features: ${features.join(', ')}\n`;
  answer += `- Training Samples: ${result.n_train}\n`;
  answer += `- Test Samples: ${result.n_test}\n\n`;

  // Add metrics
  answer += `**Model Performance:**\n`;
  
  if (result.task_type === 'regression') {
    const testMetrics = result.metrics.test;
    answer += `- R² Score: ${testMetrics.r2_score?.toFixed(4) || 'N/A'}\n`;
    answer += `- RMSE: ${testMetrics.rmse?.toFixed(4) || 'N/A'}\n`;
    answer += `- MAE: ${testMetrics.mae?.toFixed(4) || 'N/A'}\n`;
    
    if (result.metrics.cross_validation?.mean_r2) {
      answer += `- Cross-Validation R² (mean): ${result.metrics.cross_validation.mean_r2.toFixed(4)}\n`;
    }
  } else {
    const testMetrics = result.metrics.test;
    answer += `- Accuracy: ${(testMetrics.accuracy * 100)?.toFixed(2) || 'N/A'}%\n`;
    answer += `- Precision: ${(testMetrics.precision * 100)?.toFixed(2) || 'N/A'}%\n`;
    answer += `- Recall: ${(testMetrics.recall * 100)?.toFixed(2) || 'N/A'}%\n`;
    answer += `- F1 Score: ${(testMetrics.f1_score * 100)?.toFixed(2) || 'N/A'}%\n`;
    
    if (result.metrics.cross_validation?.mean_accuracy) {
      answer += `- Cross-Validation Accuracy (mean): ${(result.metrics.cross_validation.mean_accuracy * 100).toFixed(2)}%\n`;
    }
  }

  answer += `\n`;

  // Add coefficients for linear models
  if (result.coefficients) {
    answer += `**Model Coefficients:**\n`;
    answer += `- Intercept: ${typeof result.coefficients.intercept === 'number' ? result.coefficients.intercept.toFixed(4) : 'N/A'}\n`;
    
    if (result.coefficients.features) {
      const featureCoefs = Object.entries(result.coefficients.features)
        .sort((a, b) => {
          const aVal = typeof a[1] === 'number' ? Math.abs(a[1]) : 0;
          const bVal = typeof b[1] === 'number' ? Math.abs(b[1]) : 0;
          return bVal - aVal;
        });
      
      for (const [feature, coef] of featureCoefs) {
        const coefValue = typeof coef === 'number' ? coef.toFixed(4) : 'N/A';
        answer += `- ${feature}: ${coefValue}\n`;
      }
    }
    answer += `\n`;
  }

  // Add feature importance for tree-based models
  if (result.feature_importance) {
    answer += `**Feature Importance:**\n`;
    const importanceEntries = Object.entries(result.feature_importance)
      .sort((a, b) => (b[1] as number) - (a[1] as number));
    
    for (const [feature, importance] of importanceEntries) {
      answer += `- ${feature}: ${(importance as number).toFixed(4)}\n`;
    }
    answer += `\n`;
  }

  // Add insights
  answer += `**Key Insights:**\n`;
  if (result.task_type === 'regression') {
    const r2 = result.metrics.test.r2_score;
    if (r2 > 0.8) {
      answer += `- The model explains ${(r2 * 100).toFixed(1)}% of the variance, indicating excellent fit.\n`;
    } else if (r2 > 0.6) {
      answer += `- The model explains ${(r2 * 100).toFixed(1)}% of the variance, indicating good fit.\n`;
    } else if (r2 > 0.4) {
      answer += `- The model explains ${(r2 * 100).toFixed(1)}% of the variance, indicating moderate fit.\n`;
    } else {
      answer += `- The model explains ${(r2 * 100).toFixed(1)}% of the variance, indicating poor fit. Consider feature engineering or different model types.\n`;
    }
  } else {
    const accuracy = result.metrics.test.accuracy;
    if (accuracy > 0.9) {
      answer += `- The model achieves ${(accuracy * 100).toFixed(1)}% accuracy, indicating excellent performance.\n`;
    } else if (accuracy > 0.7) {
      answer += `- The model achieves ${(accuracy * 100).toFixed(1)}% accuracy, indicating good performance.\n`;
    } else {
      answer += `- The model achieves ${(accuracy * 100).toFixed(1)}% accuracy. Consider feature engineering or different model types.\n`;
    }
  }

  return answer;
}

/**
 * Extract derived column details using AI
 */
async function extractDerivedColumnDetails(
  message: string,
  availableColumns: string[]
): Promise<{ columnName: string; expression: string } | null> {
  try {
    const columnsList = availableColumns.slice(0, 30).join(', ');
    
    const prompt = `Extract the new column name and expression from the user's query for creating a derived column.

User query: "${message}"

Available columns: ${columnsList}

CRITICAL: You MUST use EXACT column names from the available columns list above. Match column names case-sensitively and exactly as they appear in the list.

Extract:
1. newColumnName: The name of the new column to create
2. expression: The formula using [ColumnName] format where ColumnName must match EXACTLY one of the available columns

Examples:
- "create column XYZ with value of each row is the sum of PA nGRP Adstocked and PAB nGRP Adstocked"
  → columnName: "XYZ", expression: "[PA nGRP Adstocked] + [PAB nGRP Adstocked]"
- "create column Total = Price * Quantity"
  → columnName: "Total", expression: "[Price] * [Quantity]"
- "add two columns X and Y and name it Sum"
  → columnName: "Sum", expression: "[X] + [Y]"
- "create column xyz where if qty_ordered is more than the mean of qty_ordered then put it as 'outperform' otherwise 'notperforming'"
  → columnName: "xyz", expression: "np.where([qty_ordered] > [qty_ordered].mean(), 'outperform', 'notperforming')"
- "add column status where if price > 100 then 'high' else 'low'"
  → columnName: "status", expression: "np.where([price] > 100, 'high', 'low')"
- "create column category where if quantity > mean(quantity) then 'above_average' otherwise 'below_average'"
  → columnName: "category", expression: "np.where([quantity] > [quantity].mean(), 'above_average', 'below_average')"

Rules:
- Use [ColumnName] format for column references
- CRITICAL: For conditional logic (if/then/else), you MUST use np.where(condition, value_if_true, value_if_false) format
- NEVER use Python ternary operator (value_if_true if condition else value_if_false) - this will cause errors with arrays
- For mean/average of a column, use [ColumnName].mean()
- For comparisons: "more than" or "greater than" → >, "less than" → <, "equal to" → ==, "not equal" → !=
- String values should be in quotes: 'value' or "value"
- Default operation when multiple columns are mentioned is addition (+)
- Match column names to available columns (case-insensitive)
- When comparing a column to its mean: use [ColumnName] > [ColumnName].mean() inside np.where()

Return JSON:
{
  "columnName": "NewColumnName",
  "expression": "[Column1] + [Column2]" or "np.where([Column1] > [Column1].mean(), 'value1', 'value2')"
}`;

    const response = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        { role: 'system', content: 'You extract column names and expressions from natural language. Return only valid JSON.' },
        { role: 'user', content: prompt }
      ],
      temperature: 0.2,
      max_tokens: 300,
    });

    const content = response.choices[0]?.message?.content?.trim();
    if (!content) return null;

    const jsonMatch = content.match(/\{[\s\S]*\}/);
    if (!jsonMatch) return null;

    const parsed = JSON.parse(jsonMatch[0]);
    
    if (parsed.columnName && parsed.expression) {
      // Match column names in expression to actual column names
      let expression = parsed.expression;
      const columnPattern = /\[([^\]]+)\]/g;
      const matches = [...expression.matchAll(columnPattern)];
      
      // Track which columns were matched and which weren't
      const unmatchedColumns: string[] = [];
      
      for (const match of matches) {
        const colRef = match[1];
        // Try to match the column name
        const matchedCol = findMatchingColumn(colRef, availableColumns);
        if (matchedCol) {
          // Replace all occurrences of this column reference
          expression = expression.replace(new RegExp(`\\[${colRef.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\]`, 'g'), `[${matchedCol}]`);
          console.log(`✅ Matched column reference "${colRef}" → "${matchedCol}"`);
        } else {
          unmatchedColumns.push(colRef);
          console.warn(`⚠️ Could not match column reference: "${colRef}"`);
        }
      }
      
      // If there are unmatched columns, log available columns for debugging
      if (unmatchedColumns.length > 0) {
        console.warn(`⚠️ Unmatched columns: ${unmatchedColumns.join(', ')}`);
        console.warn(`📋 Available columns (first 20): ${availableColumns.slice(0, 20).join(', ')}`);
      }
      
      return {
        columnName: parsed.columnName.trim(),
        expression: expression.trim(),
      };
    }
    
    return null;
  } catch (error) {
    console.error('Error extracting derived column details:', error);
    return null;
  }
}

/**
 * Check if user explicitly requested to see/preview the data
 */
function userRequestedPreview(message: string | undefined): boolean {
  if (!message) return false;
  const lowerMessage = message.toLowerCase();
  
  // Check for explicit preview/show requests
  const previewPatterns = [
    /show\s+(?:me\s+)?(?:the\s+)?(?:data|dataset|result|updated\s+data|new\s+data)/i,
    /preview/i,
    /display/i,
    /see\s+(?:the\s+)?(?:data|dataset|result)/i,
    /view\s+(?:the\s+)?(?:data|dataset)/i,
    /give\s+me\s+(?:a\s+)?(?:preview|look)/i,
  ];
  
  return previewPatterns.some(pattern => pattern.test(lowerMessage));
}

/**
 * Check if an operation modifies data and should automatically show preview
 */
function isDataModificationOperation(operation: DataOpsIntent['operation']): boolean {
  const dataModificationOperations: DataOpsIntent['operation'][] = [
    'remove_nulls',
    'create_column',
    'create_derived_column',
    'normalize_column',
    'modify_column',
    'remove_column',
    'rename_column',
    'remove_rows',
    'add_row',
    'replace_value',
    'convert_type',
    'aggregate',
    'pivot',
    'treat_outliers',
    'revert',
  ];
  
  return dataModificationOperations.includes(operation);
}

/**
 * Execute data operation based on intent
 */
export async function executeDataOperation(
  intent: DataOpsIntent,
  data: Record<string, any>[],
  sessionId: string,
  sessionDoc?: ChatDocument,
  originalMessage?: string,
  chatHistory?: Message[]
): Promise<{
  answer: string;
  data?: Record<string, any>[];
  preview?: Record<string, any>[];
  summary?: any[];
  saved?: boolean;
  // For operations like aggregate/pivot that only return a table,
  // the table will be included in "data" and "saved" will be false.
}> {
  console.log(`🔍 executeDataOperation called with intent:`, {
    operation: intent.operation,
    groupByColumn: intent.groupByColumn,
    aggColumns: intent.aggColumns,
    aggFunc: intent.aggFunc,
    requiresClarification: intent.requiresClarification,
    clarificationMessage: intent.clarificationMessage,
  });
  
  // Check if user explicitly requested preview OR if this is a data modification operation
  // Data modification operations (add/remove columns/rows, etc.) should always show preview
  const shouldShowPreview = 
    intent.operation === 'preview' || 
    userRequestedPreview(originalMessage) ||
    isDataModificationOperation(intent.operation);
  
  // Detect large dataset
  const isLargeDataset = data.length > LARGE_DATASET_THRESHOLD;
  if (isLargeDataset) {
    console.log(`📊 Large dataset detected (${data.length} rows). Using streaming mode for operations.`);
  }
  
  if (intent.requiresClarification) {
    console.log(`⚠️ Intent requires clarification: ${intent.clarificationMessage}`);
    // Save pending operation to context
    if (sessionDoc) {
      const context: DataOpsContext = {
        pendingOperation: {
          operation: intent.operation,
          column: intent.column,
          timestamp: Date.now()
        },
        lastQuery: intent.operation,
        timestamp: Date.now()
      };
      sessionDoc.dataOpsContext = context as any;
      // Persist updated Data Ops context using shared chat model helper
      await updateChatDocument(sessionDoc);
    }
    
    return {
      answer: intent.clarificationMessage || 'Please provide more information.'
    };
  }
  
  console.log(`✅ Executing operation: ${intent.operation}`);
  
  switch (intent.operation) {
    case 'remove_nulls': {
      // Validate input data
      if (!data || data.length === 0) {
        return {
          answer: '❌ No data available to process. Please ensure your dataset has been loaded correctly.',
        };
      }
      
      // Use streaming for large datasets
      const result = isLargeDataset
        ? await removeNullsStreaming(
            data,
            intent.column,
            intent.method || 'delete',
            intent.customValue
          )
        : await removeNulls(
            data,
            intent.column,
            intent.method || 'delete',
            intent.customValue
          );
      
      // Validate result data
      if (!result.data || result.data.length === 0) {
        return {
          answer: '⚠️ The operation resulted in an empty dataset. This can happen if all rows were deleted. Please try a different approach, such as imputing values instead of deleting rows.',
        };
      }
      
      // Determine if this is imputation or deletion
      const isImputation = intent.method && intent.method !== 'delete';
      const actionVerb = isImputation ? 'Imputed' : 'Removed';
      const actionVerbLower = isImputation ? 'imputed' : 'removed';
      
      // Save modified data first
      const saveResult = await saveModifiedData(
        sessionId,
        result.data,
        'remove_nulls',
        `${actionVerb} nulls from ${intent.column || 'all columns'} using ${intent.method || 'delete'}`,
        sessionDoc
      );
      
      // Only show preview if user explicitly requested it
      let previewData: Record<string, any>[] | undefined;
      let answerText = `✅ ${actionVerb} ${result.nulls_removed} null value(s)${isImputation ? ` with ${intent.method}` : ''}. Rows: ${result.rows_before} → ${result.rows_after}.`;
      
      if (shouldShowPreview) {
        previewData = await getPreviewFromSavedData(sessionId, result.data);
        answerText += ` Here's a preview of the updated data:`;
      }
      
      return {
        answer: answerText,
        data: result.data,
        preview: previewData,
        saved: true
      };
    }
    
    case 'preview': {
      let previewData: Record<string, any>[];
      let answer: string;
      
      // Check for range first (even if previewMode is not explicitly set to 'range')
      // This handles cases where AI might return previewStartRow and previewEndRow but miss previewMode
      if ((intent.previewMode === 'range' || (!intent.previewMode && intent.previewStartRow && intent.previewEndRow)) 
          && intent.previewStartRow && intent.previewEndRow) {
        // Show range of rows (1-based indices)
        const startIndex = intent.previewStartRow - 1;
        const endIndex = intent.previewEndRow; // slice is exclusive, so use endIndex directly
        if (startIndex >= 0 && startIndex < data.length && endIndex > startIndex && endIndex <= data.length) {
          previewData = data.slice(startIndex, endIndex);
          answer = `Showing rows ${intent.previewStartRow} to ${intent.previewEndRow} (${previewData.length} rows) of ${data.length} total rows:`;
        } else {
          return {
            answer: `Invalid range. Rows ${intent.previewStartRow} to ${intent.previewEndRow} are out of range. The dataset has ${data.length} rows.`
          };
        }
      } else if (intent.previewMode === 'last') {
        // Show last N rows
        const limit = intent.limit || 50;
        const startIndex = Math.max(0, data.length - limit);
        previewData = data.slice(startIndex);
        answer = `Showing last ${previewData.length} of ${data.length} rows:`;
      } else if (intent.previewMode === 'specific' && intent.previewStartRow) {
        // Show specific row (1-based index)
        const rowIndex = intent.previewStartRow - 1;
        if (rowIndex >= 0 && rowIndex < data.length) {
          previewData = [data[rowIndex]];
          answer = `Showing row ${intent.previewStartRow} of ${data.length} rows:`;
        } else {
          return {
            answer: `Row ${intent.previewStartRow} is out of range. The dataset has ${data.length} rows.`
          };
        }
      } else {
        // Default: first N rows (or use Python service for consistency)
        const limit = intent.limit || 50;
        const result = await getDataPreview(data, limit);
        previewData = result.data;
        answer = `Showing ${result.returned_rows} of ${result.total_rows} rows:`;
      }
      
      return {
        answer,
        preview: previewData
      };
    }
    
    case 'count_nulls': {
      // Count null values in data
      let nullCount = 0;
      let columnNulls: Array<{ column: string; count: number }> = [];
      
      if (intent.column) {
        // Count nulls in specific column
        const columnNullCount = data.filter(row => 
          row[intent.column!] === null || 
          row[intent.column!] === undefined || 
          row[intent.column!] === ''
        ).length;
        nullCount = columnNullCount;
        
        return {
          answer: `There are ${nullCount} null/missing values in the "${intent.column}" column out of ${data.length} total rows.`
        };
      } else {
        // Count nulls across all columns
        const columns = Object.keys(data[0] || {});
        columnNulls = columns.map(col => {
          const count = data.filter(row => 
            row[col] === null || row[col] === undefined || row[col] === ''
          ).length;
          return { column: col, count };
        });
        
        nullCount = columnNulls.reduce((sum, item) => sum + item.count, 0);
        const columnsWithNulls = columnNulls.filter(item => item.count > 0);
        
        if (columnsWithNulls.length === 0) {
          return {
            answer: `Great! There are no null or missing values in your dataset. All ${data.length} rows have complete data across all ${columns.length} columns.`
          };
        } else {
          const nullDetails = columnsWithNulls
            .sort((a, b) => b.count - a.count)
            .slice(0, 10)
            .map(item => `  • ${item.column}: ${item.count} null${item.count !== 1 ? 's' : ''}`)
            .join('\n');
          
          const moreText = columnsWithNulls.length > 10 
            ? `\n  ... and ${columnsWithNulls.length - 10} more column(s) with nulls`
            : '';
          
          return {
            answer: `There are ${nullCount} null/missing value(s) in your dataset across ${columnsWithNulls.length} column(s) out of ${columns.length} total columns.\n\nColumns with null values:\n${nullDetails}${moreText}\n\nTotal rows: ${data.length}`
          };
        }
      }
    }
    
    case 'describe': {
      // Provide conversational description of the data
      const totalRows = data.length;
      const columns = Object.keys(data[0] || {});
      const totalColumns = columns.length;
      
      // Count nulls
      const nullCounts = columns.map(col => ({
        column: col,
        count: data.filter(row => row[col] === null || row[col] === undefined || row[col] === '').length
      }));
      const totalNulls = nullCounts.reduce((sum, item) => sum + item.count, 0);
      const columnsWithNulls = nullCounts.filter(item => item.count > 0).length;
      
      // Get data types (simple inference)
      const columnTypes = columns.map(col => {
        const sampleValues = data.slice(0, 100).map(row => row[col]).filter(v => v != null);
        if (sampleValues.length === 0) return 'unknown';
        
        const firstValue = sampleValues[0];
        if (typeof firstValue === 'number') return 'numeric';
        if (typeof firstValue === 'boolean') return 'boolean';
        if (firstValue instanceof Date || (typeof firstValue === 'string' && /^\d{4}-\d{2}-\d{2}/.test(firstValue))) return 'date';
        return 'text';
      });
      
      const numericCols = columns.filter((col, idx) => columnTypes[idx] === 'numeric').length;
      const textCols = columns.filter((col, idx) => columnTypes[idx] === 'text').length;
      const dateCols = columns.filter((col, idx) => columnTypes[idx] === 'date').length;
      
      let answer = `Your dataset contains:\n`;
      answer += `  • **${totalRows.toLocaleString()} rows** of data\n`;
      answer += `  • **${totalColumns} columns**: ${numericCols} numeric, ${textCols} text, ${dateCols} date\n`;
      
      if (totalNulls > 0) {
        answer += `  • **${totalNulls.toLocaleString()} null/missing values** across ${columnsWithNulls} column(s)\n`;
      } else {
        answer += `  • **No null or missing values** - complete dataset! ✅\n`;
      }
      
      answer += `\nColumn names: ${columns.slice(0, 10).join(', ')}${columns.length > 10 ? `, ... and ${columns.length - 10} more` : ''}`;
      
      return {
        answer
      };
    }
    
    case 'summary': {
      const result = await getDataSummary(data, intent.column);
      
      if (intent.column) {
        // Single column summary
        const columnSummary = result.summary.find((s: any) => s.variable === intent.column);
        if (columnSummary) {
          return {
            answer: `Here's a summary for column "${intent.column}":`,
            summary: [columnSummary] // Return as array with single item for consistency
          };
        } else {
          return {
            answer: `Column "${intent.column}" not found. Here's a summary of all columns:`,
            summary: result.summary
          };
        }
      } else {
        // All columns summary
        return {
          answer: 'Here\'s a summary of your data:',
          summary: result.summary
        };
      }
    }
    
    case 'create_column': {
      // Extract column name and default value if not already provided
      let newColumnName = intent.newColumnName;
      let defaultValue = intent.defaultValue;
      
      // If not provided, try to extract from message using AI
      if (!newColumnName || defaultValue === undefined) {
        const messageText = originalMessage || sessionDoc?.messages?.slice().reverse().find(m => m.role === 'user')?.content || '';
        const extraction = await extractColumnDetails(messageText);
        if (extraction) {
          newColumnName = newColumnName || extraction.columnName;
          defaultValue = defaultValue !== undefined ? defaultValue : extraction.defaultValue;
        }
      }
      
      if (!newColumnName) {
        return {
          answer: 'Please specify a name for the new column. For example: "Create column status with value active"'
        };
      }
      
      // Create the column with default value
      // Round numeric default values to 2 decimal places
      let processedDefaultValue = defaultValue;
      if (defaultValue !== undefined && defaultValue !== null) {
        if (typeof defaultValue === 'number') {
          processedDefaultValue = Math.round(defaultValue * 100) / 100; // Round to 2 decimal places
        } else if (typeof defaultValue === 'string') {
          // Try to parse as number and round if successful
          const numValue = parseFloat(defaultValue);
          if (!isNaN(numValue) && isFinite(numValue)) {
            processedDefaultValue = Math.round(numValue * 100) / 100;
          }
        }
      }
      
      const modifiedData = data.map(row => ({
        ...row,
        [newColumnName!]: processedDefaultValue !== undefined ? processedDefaultValue : null
      }));
      
      // Save modified data first
      const saveResult = await saveModifiedData(
        sessionId,
        modifiedData,
        'create_column',
        `Created column "${newColumnName}" with default value: ${defaultValue !== undefined ? String(defaultValue) : 'null'}`,
        sessionDoc
      );

      // Update context to track last created column
      if (sessionDoc) {
        const context: DataOpsContext = {
          ...(sessionDoc.dataOpsContext as DataOpsContext || {}),
          lastCreatedColumn: newColumnName,
          timestamp: Date.now()
        };
        sessionDoc.dataOpsContext = context as any;
        await updateChatDocument(sessionDoc);
      }
      
      // Only show preview if user explicitly requested it
      let previewData: Record<string, any>[] | undefined;
      let answerText = `✅ Successfully created column "${newColumnName}"${defaultValue !== undefined ? ` with value "${defaultValue}"` : ''}.`;
      
      if (shouldShowPreview) {
        const updatedDoc = await getChatBySessionIdEfficient(sessionId);
        previewData = updatedDoc?.rawData ? updatedDoc.rawData.slice(0, 50) : modifiedData.slice(0, 50);
        answerText += ` Here's a preview of the updated data:`;
      }
      
      return {
        answer: answerText,
        data: modifiedData,
        preview: previewData,
        saved: true
      };
    }
    
    case 'create_derived_column': {
      // Extract column name and expression if not already provided
      let newColumnName = intent.newColumnName;
      let expression = intent.expression;
      
      // If not provided, try to extract from message using AI
      if (!newColumnName || !expression) {
        const messageText = originalMessage || sessionDoc?.messages?.slice().reverse().find(m => m.role === 'user')?.content || '';
        const availableColumns = sessionDoc?.dataSummary?.columns?.map(c => c.name) || Object.keys(data[0] || {});
        
        const extraction = await extractDerivedColumnDetails(messageText, availableColumns);
        if (extraction) {
          newColumnName = newColumnName || extraction.columnName;
          expression = expression || extraction.expression;
        }
      }
      
      if (!newColumnName) {
        return {
          answer: 'Please specify a name for the new column. For example: "Create column XYZ = [Column A] + [Column B]"'
        };
      }
      
      if (!expression) {
        const availableColumns = sessionDoc?.dataSummary?.columns?.map(c => c.name) || Object.keys(data[0] || {});
        const columnsList = availableColumns.slice(0, 10).join(', ');
        return {
          answer: `Please specify the formula for column "${newColumnName}". For example: "Create column ${newColumnName} = [Column A] + [Column B]"\n\nAvailable columns: ${columnsList}${availableColumns.length > 10 ? '...' : ''}`
        };
      }
      
      // Log the expression and available columns for debugging
      const availableColumns = sessionDoc?.dataSummary?.columns?.map(c => c.name) || Object.keys(data[0] || {});
      console.log(`🔍 Creating derived column "${newColumnName}" with expression: ${expression}`);
      console.log(`📋 Available columns: ${availableColumns.slice(0, 10).join(', ')}${availableColumns.length > 10 ? '...' : ''}`);
      
      const result = await createDerivedColumn(data, newColumnName, expression);
      
      if (result.errors && result.errors.length > 0) {
        // Extract column names from the expression to provide better error messages
        const columnPattern = /\[([^\]]+)\]/g;
        const expressionColumns = [...expression.matchAll(columnPattern)].map(m => m[1]);
        const availableColumnsList = availableColumns.slice(0, 10).join(', ');
        
        let errorMessage = `Error creating column: ${result.errors.join('; ')}`;
        
        // If the error mentions a column not found, suggest similar columns
        if (result.errors.some(e => e.includes('not found'))) {
          errorMessage += `\n\nExpression columns: ${expressionColumns.join(', ')}`;
          errorMessage += `\nAvailable columns: ${availableColumnsList}${availableColumns.length > 10 ? '...' : ''}`;
          errorMessage += `\n\nPlease check that the column names in your expression match the available columns. Column names are case-sensitive.`;
        }
        
        return {
          answer: errorMessage
        };
      }
      
      // Save modified data first
      const saveResult = await saveModifiedData(
        sessionId,
        result.data,
        'create_derived_column',
        `Created derived column "${newColumnName}" with expression: ${expression}`,
        sessionDoc
      );

      // Update context to track last created column
      if (sessionDoc) {
        const context: DataOpsContext = {
          ...(sessionDoc.dataOpsContext as DataOpsContext || {}),
          lastCreatedColumn: newColumnName,
          timestamp: Date.now()
        };
        sessionDoc.dataOpsContext = context as any;
        await updateChatDocument(sessionDoc);
      }
      
      // Only show preview if user explicitly requested it
      let previewData: Record<string, any>[] | undefined;
      let answerText = `✅ Successfully created column "${newColumnName}" with expression: ${expression}.`;
      
      if (shouldShowPreview) {
        previewData = await getPreviewFromSavedData(sessionId, result.data);
        answerText += `\n\nHere's a preview of the updated data:`;
      }
      
      return {
        answer: answerText,
        data: result.data,
        preview: previewData,
        saved: true
      };
    }

    case 'normalize_column': {
      if (!intent.column) {
        return {
          answer: 'Please specify which column you want to normalize.'
        };
      }

      if (data.length > 0 && !(intent.column in data[0])) {
        return {
          answer: `Column "${intent.column}" was not found. Available columns: ${Object.keys(data[0] || {}).join(', ')}`
        };
      }

      const numericValues = data
        .map(row => normalizeNumericValue(row[intent.column!]))
        .filter((value): value is number => value !== null);

      if (numericValues.length === 0) {
        return {
          answer: `Column "${intent.column}" does not contain numeric data to normalize.`
        };
      }

      // Calculate min/max without spread operator to avoid stack overflow on large arrays
      let min = numericValues[0];
      let max = numericValues[0];
      for (let i = 1; i < numericValues.length; i++) {
        if (numericValues[i] < min) min = numericValues[i];
        if (numericValues[i] > max) max = numericValues[i];
      }
      const range = max - min;

      const modifiedData = data.map(row => {
        const newRow = { ...row };
        const currentValue = normalizeNumericValue(row[intent.column!]);
        if (currentValue === null) {
          newRow[intent.column!] = null;
        } else if (range === 0) {
          newRow[intent.column!] = 0;
        } else {
          // Round to 2 decimal places
          const normalizedValue = (currentValue - min) / range;
          newRow[intent.column!] = Math.round(normalizedValue * 100) / 100;
        }
        return newRow;
      });

      // Save modified data first
      const saveResult = await saveModifiedData(
        sessionId,
        modifiedData,
        'normalize_column',
        `Normalized column "${intent.column}" using min-max scaling`,
        sessionDoc
      );

      // Only show preview if user explicitly requested it
      let previewData: Record<string, any>[] | undefined;
      let answerText = `✅ Normalized column "${intent.column}" using min-max scaling (0-1).`;
      
      if (shouldShowPreview) {
        previewData = await getPreviewFromSavedData(sessionId, modifiedData);
        answerText += ` Here's a preview:`;
      }
      
      return {
        answer: answerText,
        data: modifiedData,
        preview: previewData,
        saved: true
      };
    }

    case 'modify_column': {
      if (!intent.column || !intent.transformType || intent.transformValue === undefined) {
        return {
          answer: 'Please specify which column to adjust and by how much (e.g., "Reduce column XYZ by 100").'
        };
      }

      if (data.length > 0 && !(intent.column in data[0])) {
        return {
          answer: `Column "${intent.column}" was not found. Available columns: ${Object.keys(data[0] || {}).join(', ')}`
        };
      }

      const modifiedData = data.map(row => {
        const newRow = { ...row };
        const currentValue = normalizeNumericValue(row[intent.column!]);
        if (currentValue === null) {
          return newRow;
        }

        let updatedValue = currentValue;
        switch (intent.transformType) {
          case 'add':
            updatedValue = currentValue + intent.transformValue!;
            break;
          case 'subtract':
            updatedValue = currentValue - intent.transformValue!;
            break;
          case 'multiply':
            updatedValue = currentValue * intent.transformValue!;
            break;
          case 'divide':
            if (intent.transformValue === 0) {
              return newRow;
            }
            updatedValue = currentValue / intent.transformValue!;
            break;
          default:
            break;
        }

        // Round to 2 decimal places
        newRow[intent.column!] = Math.round(updatedValue * 100) / 100;
        return newRow;
      });

      // Save modified data first
      const saveResult = await saveModifiedData(
        sessionId,
        modifiedData,
        'modify_column',
        `Adjusted column "${intent.column}" by ${intent.transformType} ${intent.transformValue}`,
        sessionDoc
      );

      // Only show preview if user explicitly requested it
      let previewData: Record<string, any>[] | undefined;
      let answerText = `✅ Updated column "${intent.column}" by ${intent.transformType === 'add' ? 'adding' : intent.transformType === 'subtract' ? 'subtracting' : intent.transformType === 'multiply' ? 'multiplying by' : 'dividing by'} ${intent.transformValue}.`;
      
      if (shouldShowPreview) {
        previewData = await getPreviewFromSavedData(sessionId, modifiedData);
        answerText += ` Here's a preview of the updated data:`;
      }
      
      return {
        answer: answerText,
        data: modifiedData,
        preview: previewData,
        saved: true
      };
    }

    case 'aggregate': {
      // Use Python service for aggregation
      const groupBy =
        intent.groupByColumn ||
        intent.column ||
        findMentionedColumn(originalMessage || '', Object.keys(data[0] || {}));

      if (!groupBy) {
        return {
          answer:
            'Please specify which column to aggregate by. For example: "Aggregate by Month column".',
        };
      }

      if (data.length > 0 && !(groupBy in data[0])) {
        return {
          answer: `Column "${groupBy}" was not found. Available columns: ${Object.keys(
            data[0] || {},
          ).join(', ')}`,
        };
      }

      try {
        // If aggColumns is empty array or undefined, pass undefined to Python service for auto-detection
        const aggColumnsForPython = (intent.aggColumns && intent.aggColumns.length > 0) ? intent.aggColumns : undefined;
        
        console.log(`📊 Aggregating by "${groupBy}". aggColumns: ${aggColumnsForPython ? JSON.stringify(aggColumnsForPython) : 'undefined (auto-detect all numeric columns)'}`);
        
        // Call Python service for aggregation
        // Pass original message for semantic intent detection (average, median, highest, etc.)
        const result = await aggregateData(
          data,
          groupBy,
          aggColumnsForPython,
          intent.aggFuncs,
          intent.orderByColumn,
          intent.orderByDirection,
          originalMessage  // Pass user's original message for semantic analysis
        );

        const aggregatedData = result.data;
        const rowsBefore = result.rows_before;
        const rowsAfter = result.rows_after;

        // Build description
        const allAggColumns = intent.aggColumns || [];
        const funcDesc = intent.aggFunc || 'sum';
        const aggColCount = aggregatedData.length > 0 ? Object.keys(aggregatedData[0]).filter(k => k !== groupBy && !k.includes('(Sum)') && !k.includes('(Avg)') && !k.includes('(Min)') && !k.includes('(Max)') && !k.includes('(Count)') && !k.endsWith('_count')).length : 0;
        const numericColCount = Object.keys(aggregatedData[0] || {}).filter(k => k.includes('(Sum)') || k.includes('(Avg)') || k.includes('(Min)') || k.includes('(Max)') || k.includes('(Count)')).length;
        let description = `Aggregated data by "${groupBy}" using ${funcDesc} for ${numericColCount} numeric column${numericColCount === 1 ? '' : 's'} (excluding ID and string columns)`;

        // Save aggregated data to session (this changes the data structure permanently)
        // This saves the transformed data to blob storage in JSON format and updates the session document
        // All subsequent operations will use this aggregated data instead of the original data
        const saveResult = await saveModifiedData(
          sessionId,
          aggregatedData,
          'aggregate',
          description,
          sessionDoc
        );

        let answer = `✅ I've created a new aggregated table grouped by "${groupBy}".`;
        answer += ` Aggregated ${numericColCount} numeric column${numericColCount === 1 ? '' : 's'} (excluding ID columns and string columns).`;
        answer += ` The new table has ${rowsAfter} row${rowsAfter === 1 ? '' : 's'} (down from ${rowsBefore}) and has been saved to blob storage.`;
        if (intent.orderByColumn) {
          answer += ` Results are sorted by ${intent.orderByColumn} ${intent.orderByDirection === 'desc' ? 'descending' : 'ascending'}.`;
        }

        // For aggregation, always show the aggregated data (first 50 rows)
        // Don't try to get from CosmosDB rawData as it might be empty for large datasets
        const previewData = aggregatedData.length > 0 ? aggregatedData.slice(0, 50) : [];
        
        console.log(`✅ Aggregation complete: ${rowsAfter} rows, showing preview of ${previewData.length} rows`);
        if (previewData.length > 0) {
          console.log(`📊 Preview columns: ${Object.keys(previewData[0]).join(', ')}`);
          console.log(`📊 Sample row:`, JSON.stringify(previewData[0], null, 2));
        } else {
          console.warn(`⚠️ No preview data available - aggregatedData is empty`);
        }

        return {
          answer,
          data: aggregatedData, // Full aggregated dataset
          preview: previewData,  // Preview for display (first 50 rows)
          saved: true,
        };
      } catch (error) {
        console.error('Error calling Python service for aggregation:', error);
        return {
          answer: `Error during aggregation: ${error instanceof Error ? error.message : String(error)}. Please try again.`,
        };
      }
    }

    case 'pivot': {
      // Use Python service for pivot table creation
      const indexCol =
        intent.pivotIndex ||
        intent.groupByColumn ||
        intent.column ||
        findMentionedColumn(originalMessage || '', Object.keys(data[0] || {}));

      if (!indexCol) {
        return {
          answer:
            'Please specify which column to use as the pivot index. For example: "Create a pivot on Brand showing Sales, Spend, ROI fields".',
        };
      }

      if (data.length > 0 && !(indexCol in data[0])) {
        return {
          answer: `Column "${indexCol}" was not found. Available columns: ${Object.keys(
            data[0] || {},
          ).join(', ')}`,
        };
      }

      const allColumns = Object.keys(data[0] || {});
      const valueColumns =
        intent.pivotValues && intent.pivotValues.length > 0
          ? intent.pivotValues
          : allColumns.filter(c => c !== indexCol);

      if (valueColumns.length === 0) {
        return {
          answer: `Please specify at least one value column for the pivot (e.g., "showing Sales, Spend").`,
        };
      }

      try {
        console.log(`🔄 Starting pivot operation: indexCol="${indexCol}", valueColumns=[${valueColumns.join(', ')}]`);
        console.log(`📊 Input data: ${data.length} rows`);
        if (data.length > 0) {
          console.log(`📊 Input columns: ${Object.keys(data[0]).join(', ')}`);
          console.log(`📊 Sample input row:`, JSON.stringify(data[0], null, 2));
        }
        
        // Call Python service for pivot table
        const result = await createPivotTable(
          data,
          indexCol,
          valueColumns,
          intent.pivotFuncs
        );

        console.log(`✅ Python service returned pivot result:`);
        console.log(`   - rows_before: ${result.rows_before}`);
        console.log(`   - rows_after: ${result.rows_after}`);
        console.log(`   - data length: ${result.data?.length || 0}`);
        console.log(`   - has large file buffer: ${!!(result as any)._largeFileBuffer}`);

        const pivotData = result.data || [];
        const rowsBefore = result.rows_before;
        const rowsAfter = result.rows_after;
        const largeFileBuffer = (result as any)._largeFileBuffer as Buffer | undefined;

        // If we have a large file buffer, save it directly to blob storage
        if (largeFileBuffer) {
          console.log(`💾 Large pivot table detected. Saving ${(largeFileBuffer.length / 1024 / 1024).toFixed(2)}MB buffer directly to blob storage...`);
          
          // Get current document for username
          const doc = sessionDoc ?? await getChatBySessionIdEfficient(sessionId);
          if (!doc) {
            throw new Error('Session not found');
          }
          
          // Get current version
          const currentVersion = doc.currentDataBlob?.version || 1;
          const newVersion = currentVersion + 1;
          const username = doc.username;
          
          // Import blob storage function
          const { updateProcessedDataBlob } = await import('../blobStorage.js');
          
          // Save buffer directly to blob storage
          const blobResult = await updateProcessedDataBlob(
            sessionId,
            largeFileBuffer, // Pass buffer directly
            newVersion,
            username
          );
          
          console.log(`✅ Saved large pivot data to blob storage: ${blobResult.blobName}`);
          
          // Update CosmosDB metadata
          doc.currentDataBlob = {
            blobUrl: blobResult.blobUrl,
            blobName: blobResult.blobName,
            version: newVersion,
            lastUpdated: Date.now(),
          };
          
          // Update sample rows with preview
          doc.sampleRows = pivotData.slice(0, 100);
          
          // Update data summary
          if (pivotData.length > 0) {
            const firstRow = pivotData[0];
            const columns = Object.keys(firstRow).map(name => {
              // Get sample values from preview data
              const sampleValues = pivotData
                .slice(0, Math.min(10, pivotData.length))
                .map(row => row[name] ?? null)
                .filter(val => val !== null)
                .slice(0, 5);
              
              return {
                name,
                type: 'unknown' as string,
                sampleValues: sampleValues.length > 0 ? sampleValues : [null],
              };
            });
            doc.dataSummary = {
              ...doc.dataSummary,
              rowCount: rowsAfter,
              columns,
            };
          }
          
          // Save updated document
          const { updateChatDocument } = await import('../../models/chat.model.js');
          await updateChatDocument(doc);
          
          // Build description
          let description = `Created pivot on "${indexCol}" showing ${valueColumns.length} column${valueColumns.length === 1 ? '' : 's'}`;
          
          // Get unique values from preview
          const uniquePivotValues = new Set<string>();
          if (pivotData.length > 0) {
            const firstRow = pivotData[0];
            Object.keys(firstRow).forEach(key => {
              if (key.includes('_') && key !== indexCol) {
                const parts = key.split('_');
                if (parts.length > 1) {
                  uniquePivotValues.add(parts.slice(1).join('_'));
                }
              }
            });
          }
          
          const pivotValuesText = uniquePivotValues.size > 0 
            ? Array.from(uniquePivotValues).slice(0, 3).join(', ') + (uniquePivotValues.size > 3 ? '...' : '')
            : 'various values';
          
          let answer = `✅ I've created a pivot table on "${indexCol}".`;
          answer += ` The values from "${indexCol}" (${pivotValuesText}) have been converted into separate columns.`;
          answer += ` All other columns have been preserved.`;
          answer += ` The new table has ${rowsAfter} row${rowsAfter === 1 ? '' : 's'} (down from ${rowsBefore}) and has been saved to blob storage.`;
          answer += ` Showing preview of first ${pivotData.length} rows.`;
          
          const previewData = pivotData.slice(0, 50);
          
          return {
            answer,
            data: pivotData, // Preview data only
            preview: previewData,
            saved: true,
          };
        }

        // Normal flow for smaller pivot tables
        if (!pivotData || pivotData.length === 0) {
          console.error(`❌ Pivot returned empty data!`);
          return {
            answer: `Error: Pivot operation returned no data. Please check your data and try again.`,
          };
        }

        console.log(`📊 Pivot data details:`);
        console.log(`   - Total rows: ${pivotData.length}`);
        console.log(`   - Columns: ${Object.keys(pivotData[0] || {}).join(', ')}`);
        if (pivotData.length > 0) {
          console.log(`   - Sample pivot row:`, JSON.stringify(pivotData[0], null, 2));
          if (pivotData.length > 1) {
            console.log(`   - Second pivot row:`, JSON.stringify(pivotData[1], null, 2));
          }
        }

        // Build description
        let description = `Created pivot on "${indexCol}" showing ${valueColumns.length} column${valueColumns.length === 1 ? '' : 's'}`;

        // Save pivot data to session (this changes the data structure permanently)
        // This saves the transformed data to blob storage in JSON format and updates the session document
        // All subsequent operations will use this pivoted data instead of the original data
        console.log(`💾 Saving pivot data to blob storage...`);
        const saveResult = await saveModifiedData(
          sessionId,
          pivotData,
          'pivot',
          description,
          sessionDoc
        );
        console.log(`✅ Saved pivot data: version ${saveResult.version}, blob: ${saveResult.blobName}`);

        // Get unique values from the pivot column to show in the answer
        const uniquePivotValues = new Set<string>();
        if (pivotData.length > 0) {
          const firstRow = pivotData[0];
          // Find columns that contain the pivot index column name (these are the pivoted columns)
          Object.keys(firstRow).forEach(key => {
            if (key.includes('_') && key !== indexCol) {
              // Extract the pivot value from column names like "Sales_Complete" -> "Complete"
              const parts = key.split('_');
              if (parts.length > 1) {
                uniquePivotValues.add(parts.slice(1).join('_'));
              }
            }
          });
        }
        
        const pivotValuesText = uniquePivotValues.size > 0 
          ? Array.from(uniquePivotValues).slice(0, 3).join(', ') + (uniquePivotValues.size > 3 ? '...' : '')
          : 'various values';
        
        let answer = `✅ I've created a pivot table on "${indexCol}".`;
        answer += ` The values from "${indexCol}" (${pivotValuesText}) have been converted into separate columns.`;
        answer += ` All other columns have been preserved.`;
        answer += ` The new table has ${rowsAfter} row${rowsAfter === 1 ? '' : 's'} (down from ${rowsBefore}) and has been saved to blob storage.`;

        // For pivot, show data grouped by status category (3-4 rows per category)
        // Sort by status column if it exists, then group and limit
        let previewData: Record<string, any>[] = [];
        
        if (pivotData.length > 0) {
          // Check if status column exists in the data
          const hasStatusColumn = pivotData.some(row => indexCol in row);
          
          if (hasStatusColumn) {
            // Group by status and show 3-4 rows per category
            const groupedByStatus = new Map<string, Record<string, any>[]>();
            
            for (const row of pivotData) {
              const statusValue = row[indexCol] ?? 'Unknown';
              const statusKey = String(statusValue);
              
              if (!groupedByStatus.has(statusKey)) {
                groupedByStatus.set(statusKey, []);
              }
              
              const group = groupedByStatus.get(statusKey)!;
              if (group.length < 4) { // Show max 4 rows per status category
                group.push(row);
              }
            }
            
            // Flatten grouped data, maintaining status order
            const statusOrder = Array.from(groupedByStatus.keys()).sort();
            for (const status of statusOrder) {
              previewData.push(...groupedByStatus.get(status)!);
            }
            
            console.log(`✅ Grouped preview by status: ${groupedByStatus.size} categories, ${previewData.length} total rows`);
          } else {
            // No status column, just show first 50 rows
            previewData = pivotData.slice(0, 50);
          }
        }
        
        console.log(`✅ Pivot complete: ${rowsAfter} rows, showing preview of ${previewData.length} rows`);
        if (previewData.length > 0) {
          console.log(`📊 Preview columns: ${Object.keys(previewData[0]).join(', ')}`);
          console.log(`📊 Preview sample row:`, JSON.stringify(previewData[0], null, 2));
        } else {
          console.warn(`⚠️ No preview data available - pivotData is empty`);
        }

        console.log(`📤 Returning pivot result: answer length=${answer.length}, preview rows=${previewData.length}, saved=${true}`);

        return {
          answer,
          data: pivotData, // Full pivoted dataset
          preview: previewData,  // Preview for display (first 50 rows)
          saved: true,
        };
      } catch (error) {
        console.error('❌ Error calling Python service for pivot:', error);
        console.error('❌ Error stack:', error instanceof Error ? error.stack : 'No stack trace');
        return {
          answer: `Error during pivot creation: ${error instanceof Error ? error.message : String(error)}. Please try again.`,
        };
      }
    }

    case 'remove_rows': {
      if (data.length === 0) {
        return { answer: 'There are no rows to remove.' };
      }

      // Determine which row(s) to remove
      const indicesToRemove = new Set<number>();
      const rowCount = intent.rowCount && intent.rowCount > 0 ? intent.rowCount : 1;

      if (intent.rowIndex && intent.rowIndex > 0 && intent.rowIndex <= data.length) {
        // Remove a specific row index (1-based)
        indicesToRemove.add(intent.rowIndex - 1);
      } else if (intent.rowPosition === 'keep_first') {
        // Special case: "keep only first N rows" means "remove all rows after row N"
        // Keep first N rows, remove the rest
        const keepCount = Math.min(rowCount, data.length);
        for (let i = keepCount; i < data.length; i++) {
          indicesToRemove.add(i);
        }
      } else if (intent.rowPosition === 'first') {
        const count = Math.min(rowCount, data.length);
        for (let i = 0; i < count; i++) {
          indicesToRemove.add(i);
        }
      } else if (intent.rowPosition === 'last') {
        const count = Math.min(rowCount, data.length);
        for (let i = 0; i < count; i++) {
          indicesToRemove.add(data.length - 1 - i);
        }
      }

      if (indicesToRemove.size === 0) {
        return { answer: 'Please specify which row to remove (first, last, or row number).' };
      }

      const modifiedData = data.filter((_, idx) => !indicesToRemove.has(idx));

      // Build a human-readable description
      const isKeepFirst = intent.rowPosition === 'keep_first';
      const sortedIndices = Array.from(indicesToRemove).sort((a, b) => a - b);
      const removedCount = sortedIndices.length;
      const keptCount = modifiedData.length;
      
      let description: string;
      let answerText: string;
      
      if (isKeepFirst) {
        // For "keep only first N rows", provide a clearer message
        description = `all rows except the first ${keptCount}`;
        answerText = `✅ Kept only the first ${keptCount} rows and removed ${removedCount} row${removedCount === 1 ? '' : 's'}. Dataset now has ${keptCount} row${keptCount === 1 ? '' : 's'}.`;
      } else if (removedCount === 1) {
        description = `row ${sortedIndices[0] + 1}`;
        answerText = `✅ Removed ${description}.`;
      } else if (removedCount > 1 && sortedIndices[sortedIndices.length - 1] - sortedIndices[0] + 1 === removedCount) {
        // Consecutive range
        description = `rows ${sortedIndices[0] + 1}-${sortedIndices[sortedIndices.length - 1] + 1}`;
        answerText = `✅ Removed ${description}.`;
      } else {
        description = `rows ${sortedIndices.map(i => i + 1).join(', ')}`;
        answerText = `✅ Removed ${description}.`;
      }

      // Save modified data first
      const saveResult = await saveModifiedData(
        sessionId,
        modifiedData,
        'remove_rows',
        isKeepFirst ? `Kept first ${keptCount} rows, removed ${removedCount} rows` : `Removed ${description}`,
        sessionDoc
      );

      // Only show preview if user explicitly requested it
      let previewData: Record<string, any>[] | undefined;
      
      if (shouldShowPreview) {
        previewData = await getPreviewFromSavedData(sessionId, modifiedData);
        answerText += ` Here's a preview of the updated data:`;
      }
      
      return {
        answer: answerText,
        data: modifiedData,
        preview: previewData,
        saved: true
      };
    }

    case 'add_row': {
      const template = data[0] || {};
      const newRow: Record<string, any> = {};
      for (const key of Object.keys(template)) {
        newRow[key] = null;
      }

      const modifiedData = [...data, newRow];

      // Save modified data first
      const saveResult = await saveModifiedData(
        sessionId,
        modifiedData,
        'add_row',
        'Added a new empty row at the end of the dataset',
        sessionDoc
      );

      // Only show preview if user explicitly requested it
      let previewData: Record<string, any>[] | undefined;
      let answerText = `✅ Added a new empty row at the bottom.`;
      
      if (shouldShowPreview) {
        previewData = await getPreviewFromSavedData(sessionId, modifiedData);
        answerText += ` Here's a preview:`;
      }
      
      return {
        answer: answerText,
        data: modifiedData,
        preview: previewData,
        saved: true
      };
    }
    
    case 'replace_value': {
      if (intent.oldValue === undefined) {
        return {
          answer: 'Please specify which value you want to replace. For example: "replace - with 0" or "remove the value -"'
        };
      }
      
      if (intent.newValue === undefined) {
        return {
          answer: 'Please specify what value to replace it with. For example: "replace - with 0" or "replace - with null"'
        };
      }
      
      // Replace values in the dataset
      let replacedCount = 0;
      const modifiedData = data.map(row => {
        const newRow = { ...row };
        const columnsToProcess = intent.column ? [intent.column] : Object.keys(row);
        
        for (const col of columnsToProcess) {
          if (col in row) {
            const currentValue = row[col];
            // Compare values (handle null, strings, numbers)
            let shouldReplace = false;
            
            if (intent.oldValue === null || intent.oldValue === 'null') {
              shouldReplace = (currentValue === null || currentValue === undefined || currentValue === '');
            } else if (intent.oldValue === '-') {
              // Handle dash/placeholder values (including variations with spaces)
              const currentStr = String(currentValue).trim();
              shouldReplace = (currentStr === '-' || currentStr === ' - ' || currentStr === '—' || currentStr === '–');
            } else {
              // String or number comparison
              shouldReplace = (String(currentValue).trim() === String(intent.oldValue).trim());
            }
            
            if (shouldReplace) {
              // Round numeric newValue to 2 decimal places
              if (typeof intent.newValue === 'number') {
                newRow[col] = Math.round(intent.newValue * 100) / 100;
              } else if (intent.newValue === null || intent.newValue === 'null') {
                newRow[col] = null;
              } else {
                newRow[col] = intent.newValue;
              }
              replacedCount++;
            }
          }
        }
        return newRow;
      });
      
      // Save modified data
      const saveResult = await saveModifiedData(
        sessionId,
        modifiedData,
        'replace_value',
        `Replaced ${replacedCount} occurrence(s) of "${intent.oldValue}" with "${intent.newValue}"${intent.column ? ` in column "${intent.column}"` : ' across all columns'}`,
        sessionDoc
      );
      
      // Only show preview if user explicitly requested it
      let previewData: Record<string, any>[] | undefined;
      let answerText = `✅ Replaced ${replacedCount} occurrence(s) of "${intent.oldValue}" with "${intent.newValue}"${intent.column ? ` in column "${intent.column}"` : ' across all columns'}.`;
      
      if (shouldShowPreview) {
        previewData = await getPreviewFromSavedData(sessionId, modifiedData);
        answerText += ` Here's a preview of the updated data:`;
      }
      
      return {
        answer: answerText,
        data: modifiedData,
        preview: previewData,
        saved: true
      };
    }
    
    case 'remove_column': {
      if (!intent.column) {
        return {
          answer: 'Please specify which column you want to remove. For example: "Remove column PAB nGRP Adstocked"'
        };
      }
      
      // Check if column exists
      if (data.length > 0 && !(intent.column in data[0])) {
        return {
          answer: `Column "${intent.column}" not found. Available columns: ${Object.keys(data[0] || {}).join(', ')}`
        };
      }
      
      // Remove the column
      const modifiedData = data.map(row => {
        const newRow = { ...row };
        delete newRow[intent.column!];
        return newRow;
      });
      
      // Save modified data first
      const saveResult = await saveModifiedData(
        sessionId,
        modifiedData,
        'remove_column',
        `Removed column "${intent.column}"`,
        sessionDoc
      );
      
      // Only show preview if user explicitly requested it
      let previewData: Record<string, any>[] | undefined;
      let answerText = `✅ Successfully removed column "${intent.column}".`;
      
      if (shouldShowPreview) {
        previewData = await getPreviewFromSavedData(sessionId, modifiedData);
        answerText += ` Here's a preview of the updated data:`;
      }
      
      return {
        answer: answerText,
        data: modifiedData,
        preview: previewData,
        saved: true
      };
    }
    
    case 'rename_column': {
      // Determine which column to rename
      const columnToRename = intent.oldColumnName || intent.column;
      const newName = intent.newColumnName;
      
      if (!columnToRename) {
        // Try to find from context
        const { findLastCreatedColumn } = await import('../agents/contextResolver.js');
        const lastColumn = findLastCreatedColumn(chatHistory || []);
        if (lastColumn) {
          const resolvedColumn = lastColumn;
          if (!newName) {
            return {
              answer: `I found column "${resolvedColumn}" from context. What would you like to rename it to?`
            };
          }
          
          // Use resolved column
          const modifiedData = data.map(row => {
            const newRow = { ...row };
            if (resolvedColumn in newRow) {
              newRow[newName] = newRow[resolvedColumn];
              delete newRow[resolvedColumn];
            }
            return newRow;
          });
          
          const saveResult = await saveModifiedData(
            sessionId,
            modifiedData,
            'rename_column',
            `Renamed column "${resolvedColumn}" to "${newName}"`,
            sessionDoc
          );
          
          // Update context
          if (sessionDoc) {
            const context: DataOpsContext = {
              ...(sessionDoc.dataOpsContext as DataOpsContext || {}),
              lastCreatedColumn: newName, // Update to new name
              timestamp: Date.now()
            };
            sessionDoc.dataOpsContext = context as any;
            await updateChatDocument(sessionDoc);
          }
          
          let previewData: Record<string, any>[] | undefined;
          let answerText = `✅ Successfully renamed column "${resolvedColumn}" to "${newName}".`;
          
          if (shouldShowPreview) {
            previewData = await getPreviewFromSavedData(sessionId, modifiedData);
            answerText += ` Here's a preview of the updated data:`;
          }
          
          return {
            answer: answerText,
            data: modifiedData,
            preview: previewData,
            saved: true
          };
        }
        
        return {
          answer: 'Please specify which column you want to rename. For example: "Rename column Sales to Revenue" or "Change the above column name to Two"'
        };
      }
      
      if (!newName) {
        return {
          answer: `Please specify the new name for column "${columnToRename}". For example: "Rename column ${columnToRename} to NewName"`
        };
      }
      
      // Check if column exists
      if (data.length > 0 && !(columnToRename in data[0])) {
        return {
          answer: `Column "${columnToRename}" not found. Available columns: ${Object.keys(data[0] || {}).join(', ')}`
        };
      }
      
      // Check if new name already exists
      if (data.length > 0 && newName in data[0] && newName !== columnToRename) {
        return {
          answer: `Cannot rename: Column "${newName}" already exists. Please choose a different name.`
        };
      }
      
      // Rename the column
      const modifiedData = data.map(row => {
        const newRow = { ...row };
        if (columnToRename in newRow) {
          newRow[newName] = newRow[columnToRename];
          delete newRow[columnToRename];
        }
        return newRow;
      });
      
      // Save modified data first
      const saveResult = await saveModifiedData(
        sessionId,
        modifiedData,
        'rename_column',
        `Renamed column "${columnToRename}" to "${newName}"`,
        sessionDoc
      );
      
      // Update context to track renamed column
      if (sessionDoc) {
        const context: DataOpsContext = {
          ...(sessionDoc.dataOpsContext as DataOpsContext || {}),
          lastCreatedColumn: newName, // Update to new name
          timestamp: Date.now()
        };
        sessionDoc.dataOpsContext = context as any;
        await updateChatDocument(sessionDoc);
      }
      
      // Only show preview if user explicitly requested it
      let previewData: Record<string, any>[] | undefined;
      let answerText = `✅ Successfully renamed column "${columnToRename}" to "${newName}".`;
      
      if (shouldShowPreview) {
        previewData = await getPreviewFromSavedData(sessionId, modifiedData);
        answerText += ` Here's a preview of the updated data:`;
      }
      
      return {
        answer: answerText,
        data: modifiedData,
        preview: previewData,
        saved: true
      };
    }
    
    case 'convert_type': {
      if (!intent.column || !intent.targetType) {
        return {
          answer: 'Please specify both column and target type.'
        };
      }
      
      const result = await convertDataType(data, intent.column, intent.targetType);
      
      // Save modified data first
      const saveResult = await saveModifiedData(
        sessionId,
        result.data,
        'convert_type',
        `Converted ${intent.column} to ${intent.targetType}`,
        sessionDoc
      );
      
      // Only show preview if user explicitly requested it
      let previewData: Record<string, any>[] | undefined;
      const errorMsg = result.conversion_info.errors.length > 0
        ? ` Note: ${result.conversion_info.errors.join(', ')}`
        : '';
      let answerText = `✅ Converted "${intent.column}" to ${intent.targetType}.${errorMsg}`;
      
      if (shouldShowPreview) {
        previewData = await getPreviewFromSavedData(sessionId, result.data);
        answerText += ` Here's a preview:`;
      }
      
      return {
        answer: answerText,
        data: result.data,
        preview: previewData,
        saved: true
      };
    }
    
    case 'train_model': {
      // Extract model parameters from intent or use AI to extract from message
      let modelType: 'linear' | 'log_log' | 'logistic' | 'ridge' | 'lasso' | 'random_forest' | 'decision_tree' | 'gradient_boosting' | 'elasticnet' | 'svm' | 'knn' | 'polynomial' | 'bayesian' | 'quantile' | 'poisson' | 'gamma' | 'tweedie' | 'extra_trees' | 'xgboost' | 'lightgbm' | 'catboost' | 'gaussian_process' | 'mlp' | 'multinomial_logistic' | 'naive_bayes_gaussian' | 'naive_bayes_multinomial' | 'naive_bayes_bernoulli' | 'lda' | 'qda' = intent.modelType || 'linear';
      let targetVariable = intent.targetVariable;
      let features = intent.features || [];
      
      // Get chat history to look for previous model parameters
      // Use passed chatHistory first, fallback to sessionDoc messages if available
      let chatHistoryForContext: Message[] = [];
      try {
        chatHistoryForContext = chatHistory || sessionDoc?.messages || [];
      } catch (error) {
        // If accessing sessionDoc.messages fails (e.g., CosmosDB not initialized), use empty array
        console.warn('⚠️ Could not access chat history, continuing without context:', error);
        chatHistoryForContext = chatHistory || [];
      }
      
      const previousModelParams = extractPreviousModelParams(chatHistoryForContext);
      
      if (previousModelParams) {
        console.log(`✅ Found previous model context: target="${previousModelParams.targetVariable}", features=[${previousModelParams.features?.join(', ')}], type=${previousModelParams.modelType || 'unknown'}`);
      } else {
        console.log(`⚠️ No previous model context found in ${chatHistoryForContext.length} messages`);
      }
      
      // Check if user wants less variance (Ridge/Lasso)
      const messageText = originalMessage || sessionDoc?.messages?.slice().reverse().find(m => m.role === 'user')?.content || '';
      const messageLower = messageText.toLowerCase();
      const wantsLessVariance = messageLower.includes('less variance') || 
                                messageLower.includes('reduce variance') || 
                                messageLower.includes('lower variance');
      
      console.log(`📝 Processing model request: message="${messageText}", wantsLessVariance=${wantsLessVariance}, hasPreviousParams=${!!previousModelParams}`);
      
      // If user wants less variance and we have previous model, default to Ridge
      if (wantsLessVariance && previousModelParams && !intent.modelType) {
        modelType = 'ridge';
        console.log(`🎯 User wants less variance, using Ridge model`);
        // Also use previous model params if not provided
        if (!targetVariable && previousModelParams.targetVariable) {
          targetVariable = previousModelParams.targetVariable;
          console.log(`📋 Using previous target variable: ${targetVariable}`);
        }
        if (features.length === 0 && previousModelParams.features && previousModelParams.features.length > 0) {
          features = previousModelParams.features;
          console.log(`📋 Using previous features: ${features.join(', ')}`);
        }
      }
      
      // If not provided, try to extract from message using AI
      if (!targetVariable || features.length === 0) {
        const availableColumns = sessionDoc?.dataSummary?.columns?.map(c => c.name) || Object.keys(data[0] || {});
        
        // Use AI to extract ML model parameters with context
        const extraction = await extractMLModelDetails(messageText, availableColumns, chatHistoryForContext, previousModelParams || undefined);
        if (extraction) {
          modelType = extraction.modelType || modelType;
          targetVariable = targetVariable || extraction.targetVariable;
          features = features.length > 0 ? features : (extraction.features || []);
        }
        
        // If still missing and we have previous model params, use them (strong fallback)
        if ((!targetVariable || features.length === 0) && previousModelParams) {
          console.log(`📋 Using previous model parameters from chat history as fallback`);
          targetVariable = targetVariable || previousModelParams.targetVariable;
          features = features.length > 0 ? features : (previousModelParams.features || []);
        }
      }
      
      if (!targetVariable) {
        return {
          answer: 'Please specify the target variable (dependent variable) for the model. For example: "Build a linear model choosing Sales as target variable and Price, Marketing as independent variables"'
        };
      }
      
      if (features.length === 0) {
        return {
          answer: 'Please specify the features (independent variables) for the model. For example: "Build a linear model choosing Sales as target variable and Price, Marketing as independent variables"'
        };
      }
      
      // Find matching columns
      const allColumns = sessionDoc?.dataSummary?.columns?.map(c => c.name) || Object.keys(data[0] || {});
      const { findMatchingColumn } = await import('../agents/utils/columnMatcher.js');
      
      const targetCol = findMatchingColumn(targetVariable, allColumns);
      if (!targetCol) {
        return {
          answer: `Could not find column matching "${targetVariable}". Available columns: ${allColumns.slice(0, 10).join(', ')}${allColumns.length > 10 ? '...' : ''}`
        };
      }
      
      const matchedFeatures = features
        .map(f => findMatchingColumn(f, allColumns))
        .filter((f): f is string => f !== null && f !== targetCol);
      
      if (matchedFeatures.length === 0) {
        return {
          answer: `Could not match any features to columns. Available columns: ${allColumns.slice(0, 10).join(', ')}${allColumns.length > 10 ? '...' : ''}`
        };
      }
      
      // Diagnostic: Check data quality before training
      if (data.length === 0) {
        return {
          answer: 'No data available for model training. Please ensure your dataset has been loaded correctly.'
        };
      }
      
      // Check for null values in target and features
      const targetNulls = data.filter(row => row[targetCol] === null || row[targetCol] === undefined || row[targetCol] === '').length;
      const featureNulls = matchedFeatures.map(f => ({
        feature: f,
        nulls: data.filter(row => row[f] === null || row[f] === undefined || row[f] === '').length
      }));
      
      console.log(`📊 Data quality check: Total rows=${data.length}, Target nulls=${targetNulls}, Feature nulls:`, featureNulls);
      
      if (targetNulls === data.length) {
        return {
          answer: `Cannot train model: Target variable "${targetCol}" has no valid values (all ${data.length} rows are null/empty). Please check your data.`
        };
      }
      
      const allFeaturesNull = featureNulls.every(f => f.nulls === data.length);
      if (allFeaturesNull) {
        return {
          answer: `Cannot train model: All features have no valid values (all ${data.length} rows are null/empty). Please check your data.`
        };
      }
      
      try {
        // Train the model
        const modelResult = await trainMLModel(
          data,
          modelType,
          targetCol,
          matchedFeatures
        );
        
        // Format response
        const answer = formatMLModelResponse(modelResult, modelType, targetCol, matchedFeatures);
        
        return {
          answer,
          saved: false // ML models don't modify data
        };
      } catch (error) {
        console.error('ML model training error:', error);
        const errorMessage = error instanceof Error ? error.message : String(error);
        
        // Provide helpful error message
        let helpfulMessage = `Error training model: ${errorMessage}`;
        if (errorMessage.includes('No valid data rows')) {
          helpfulMessage += `\n\nThis usually means:\n` +
            `- The target variable "${targetCol}" or features have too many null values\n` +
            `- After removing rows with null targets, no rows remain with valid feature values\n` +
            `- Try checking your data: "Count nulls in ${targetCol}" or "Show me the data"`;
        }
        
        return {
          answer: helpfulMessage
        };
      }
    }
    
    case 'identify_outliers': {
      // Validate input data
      if (!data || data.length === 0) {
        return {
          answer: '❌ No data available to process. Please ensure your dataset has been loaded correctly.',
        };
      }

      try {
        const method = intent.outlierMethod || 'iqr';
        const threshold = intent.outlierThreshold || (method === 'zscore' ? 3 : 1.5);
        
        const result = await identifyOutliers(
          data,
          intent.column,
          method,
          threshold
        );

        // Format response
        let answer = `📊 Outlier Analysis Results:\n\n`;
        answer += `**Method Used:** ${method.toUpperCase()}\n`;
        answer += `**Threshold:** ${threshold}\n`;
        answer += `**Total Outliers Found:** ${result.summary.total_outliers}\n\n`;

        if (result.summary.outliers_by_column && Object.keys(result.summary.outliers_by_column).length > 0) {
          answer += `**Outliers by Column:**\n`;
          Object.entries(result.summary.outliers_by_column).forEach(([col, count]) => {
            answer += `- ${col}: ${count} outlier(s)\n`;
          });
          answer += `\n`;
        }

        if (result.outliers.length > 0) {
          answer += `**Outlier Details (showing first 20):**\n`;
          result.outliers.slice(0, 20).forEach((outlier, idx) => {
            answer += `${idx + 1}. Row ${outlier.row_index + 1}, Column "${outlier.column}": ${outlier.value}`;
            if (outlier.z_score !== undefined) {
              answer += ` (z-score: ${outlier.z_score.toFixed(2)})`;
            }
            if (outlier.iqr_lower !== undefined && outlier.iqr_upper !== undefined) {
              answer += ` (bounds: ${outlier.iqr_lower.toFixed(2)} - ${outlier.iqr_upper.toFixed(2)})`;
            }
            answer += `\n`;
          });
          
          if (result.outliers.length > 20) {
            answer += `\n... and ${result.outliers.length - 20} more outliers.\n`;
          }
          
          answer += `\n💡 Would you like me to treat these outliers? I can remove them, cap them, or replace them with mean/median values.`;
        } else {
          answer += `✅ No outliers detected using the ${method} method with threshold ${threshold}.`;
        }

        return {
          answer,
          saved: false, // Identification doesn't modify data
        };
      } catch (error) {
        console.error('Outlier identification error:', error);
        return {
          answer: `Failed to identify outliers: ${error instanceof Error ? error.message : 'Unknown error'}. Please try again.`,
        };
      }
    }

    case 'treat_outliers': {
      // Validate input data
      if (!data || data.length === 0) {
        return {
          answer: '❌ No data available to process. Please ensure your dataset has been loaded correctly.',
        };
      }

      try {
        const method = intent.outlierMethod || 'iqr';
        const threshold = intent.outlierThreshold || (method === 'zscore' ? 3 : 1.5);
        const treatment = intent.treatmentMethod || 'remove';
        const treatmentValue = intent.treatmentValue;

        console.log(`🔍 Outlier treatment parameters:`, {
          method,
          threshold,
          treatment,
          treatmentValue,
          column: intent.column
        });

        const result = await treatOutliers(
          data,
          intent.column,
          method,
          threshold,
          treatment,
          treatmentValue
        );

        // Save modified data
        const saveResult = await saveModifiedData(
          sessionId,
          result.data,
          'treat_outliers',
          `Treated ${result.outliers_treated} outliers using ${method} method with ${treatment} treatment`,
          sessionDoc
        );

        // Format response
        let answer = `✅ Successfully treated outliers:\n\n`;
        answer += `**Method:** ${method.toUpperCase()}\n`;
        answer += `**Treatment:** ${treatment}\n`;
        if (treatmentValue) {
          answer += `**Treatment Value:** ${treatmentValue}\n`;
        }
        answer += `**Outliers Treated:** ${result.outliers_treated}\n`;
        answer += `**Rows:** ${result.rows_before} → ${result.rows_after}\n`;

        if (result.summary.outliers_by_column && Object.keys(result.summary.outliers_by_column).length > 0) {
          answer += `\n**Treated by Column:**\n`;
          Object.entries(result.summary.outliers_by_column).forEach(([col, count]) => {
            answer += `- ${col}: ${count} outlier(s)\n`;
          });
        }

        // Get preview from saved data
        const previewData = await getPreviewFromSavedData(sessionId, result.data);

        return {
          answer,
          data: result.data,
          preview: previewData,
          saved: true,
        };
      } catch (error) {
        console.error('Outlier treatment error:', error);
        return {
          answer: `Failed to treat outliers: ${error instanceof Error ? error.message : 'Unknown error'}. Please try again.`,
        };
      }
    }

    case 'revert': {
      // Load original data from blob
      if (!sessionDoc) {
        return {
          answer: 'Unable to revert: session not found. Please refresh and try again.',
        };
      }

      if (!sessionDoc.blobInfo?.blobName) {
        return {
          answer: 'Unable to revert: original data not found. The original file may have been deleted.',
        };
      }

      try {
        // Load original file from blob
        const blobBuffer = await getFileFromBlob(sessionDoc.blobInfo.blobName);
        
        // Parse the file
        let originalData = await parseFile(blobBuffer, sessionDoc.fileName);
        
        if (!originalData || originalData.length === 0) {
          return {
            answer: 'Unable to revert: original data file is empty or could not be parsed.',
          };
        }

        // Convert "-" to 0 for numeric columns (same as upload processing)
        const numericColumns = sessionDoc.dataSummary?.numericColumns || [];
        originalData = convertDashToZeroForNumericColumns(originalData, numericColumns);

        // Save the original data back to session
        const saveResult = await saveModifiedData(
          sessionId,
          originalData,
          'revert',
          'Reverted data to original form',
          sessionDoc
        );

        // Get preview from saved data
        const previewData = await getPreviewFromSavedData(sessionId, originalData);

        return {
          answer: `✅ Successfully reverted the data to its original form. The table now has ${originalData.length} rows with the original structure.`,
          data: originalData,
          preview: previewData,
          saved: true,
        };
      } catch (error) {
        console.error('Error reverting data:', error);
        return {
          answer: `Failed to revert data: ${error instanceof Error ? error.message : 'Unknown error'}. Please try again or contact support.`,
        };
      }
    }
    
    default:
      // For unknown operations, try to provide a helpful response
      console.error(`❌ Unknown operation: "${intent.operation}". Intent details:`, {
        operation: intent.operation,
        groupByColumn: intent.groupByColumn,
        aggColumns: intent.aggColumns,
        requiresClarification: intent.requiresClarification,
        clarificationMessage: intent.clarificationMessage,
      });
      return {
        answer: 'I can help you with data operations like:\n\n' +
          '• **Revert data**: "Revert to original" or "Restore original data"\n' +
          '• **Aggregate data**: "Aggregate by Month" or "Aggregate RISK_VOLUME on DEPOT"\n' +
          '• **Create pivot tables**: "Create a pivot on Brand showing Sales, Spend, ROI"\n' +
          '• **Remove columns**: "Remove column X" or "Delete column Y"\n' +
          '• **Rename columns**: "Rename column X to Y" or "Change the above column name to Two"\n' +
          '• **Create columns**: "Create column XYZ = A + B" or "Add column Status with value Active"\n' +
          '• **Adjust column values**: "Increase column X by 50" or "Reduce column Y by 100"\n' +
          '• **Normalize columns**: "Normalize column Sales" or "Standardize metric Z"\n' +
          '• **Add/Remove rows**: "Add a new row" or "Remove last row"\n' +
          '• **Count null values**: "How many null values are there?" or "Count nulls in columnX"\n' +
          '• **View data**: "Show me the data" or "Show top 100 rows"\n' +
          '• **Data summary**: "Give me a data summary" or "Show statistics"\n' +
          '• **Remove nulls**: "Remove null values" or "Delete nulls in columnX"\n' +
          '• **Convert types**: "Convert columnX to numeric/date/percentage"\n' +
          '• **Describe data**: "How many rows/columns?" or "Describe the dataset"\n\n' +
          'What would you like to do with your data?'
      };
  }
}

