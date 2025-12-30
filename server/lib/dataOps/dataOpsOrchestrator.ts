/**
 * Data Ops Orchestrator
 * Handles intent parsing, clarification flow, and coordinates data operations
 */
import { Message, DataSummary } from '../../shared/schema.js';
import { removeNulls, getDataPreview, getDataSummary, convertDataType, createDerivedColumn, trainMLModel } from './pythonService.js';
import { saveModifiedData } from './dataPersistence.js';
import { getChatBySessionIdEfficient, updateChatDocument, ChatDocument } from '../../models/chat.model.js';
import { openai } from '../openai.js';
import { getFileFromBlob } from '../blobStorage.js';
import { parseFile, convertDashToZeroForNumericColumns } from '../fileParser.js';

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
    | 'remove_rows'
    | 'add_row'
    | 'aggregate'
    | 'pivot'
    | 'train_model'
    | 'replace_value'
    | 'revert'
    | 'unknown';
  column?: string;
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
}

export interface DataOpsContext {
  pendingOperation?: {
    operation: string;
    column?: string;
    timestamp: number;
  };
  lastQuery?: string;
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
  
  // Try AI detection first for ALL operations
  try {
    const aiIntent = await detectDataOpsIntentWithAI(message, availableColumns);
    if (aiIntent && aiIntent.operation !== 'unknown') {
      console.log(`✅ AI detected intent: ${aiIntent.operation}`);
      return aiIntent;
    }
  } catch (error) {
    console.error('⚠️ AI intent detection failed, falling back to regex patterns:', error);
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
    if (lowerMessage.includes('sum') || lowerMessage.includes('add') || lowerMessage.includes('+') ||
        lowerMessage.includes('multiply') || lowerMessage.includes('*') || lowerMessage.includes('times') ||
        lowerMessage.includes('subtract') || lowerMessage.includes('-') || lowerMessage.includes('minus') ||
        lowerMessage.includes('divide') || lowerMessage.includes('/') ||
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
  availableColumns: string[]
): Promise<DataOpsIntent | null> {
  try {
    const columnsList = availableColumns.slice(0, 20).join(', '); // Limit to avoid token issues
    
    const prompt = `You are a data operations assistant. Analyze the user's question and determine what data operation they want.

User question: "${message}"

Available columns: ${columnsList}

Determine the intent and return JSON with this structure:
{
  "operation": "remove_nulls" | "preview" | "summary" | "convert_type" | "count_nulls" | "describe" | "create_derived_column" | "create_column" | "modify_column" | "normalize_column" | "remove_rows" | "add_row" | "remove_column" | "aggregate" | "pivot" | "train_model" | "replace_value" | "revert" | "unknown",
  "column": "column_name" (if specific column mentioned for single-column operations, null otherwise),
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
- "create_derived_column": User wants to create new column from expression (e.g., "create column XYZ = A + B", "add two columns X and Y", "create column XYZ with sum of PA and PAB")
  * Extract newColumnName: the name of the new column to create
  * Extract expression: formula using [ColumnName] format (e.g., "[PA nGRP Adstocked] + [PAB nGRP Adstocked]")
  * If user says "sum of X and Y", expression should be "[X] + [Y]"
  * If user says "add X and Y", expression should be "[X] + [Y]"
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
- "add_row": User wants to add/append a row (e.g., "add a new row", "append row at bottom")
- "count_nulls": User wants to count/null values (e.g., "how many nulls", "count missing values")
- "replace_value": User wants to replace a specific value with another. Handle various phrasings:
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
- "remove_nulls": User wants to remove/handle nulls. IMPORTANT: If user says "fill null with mean/median/mode" or "impute null", set method to "mean"/"median"/"mode" and requiresClarification to false. If user says "remove null" or "delete null" without specifying fill/impute, default to asking for clarification.
- "convert_type": User wants to convert column type
- "aggregate": User wants to group data by a column and summarize other columns
  * Patterns: "aggregate by X", "aggregate X on Y", "aggregate X, group by Y, order by Z DESC", "aggregate by Month column", "aggregate by Brand"
  * Extract groupByColumn: the column to group by (e.g., "Month", "Brand", "DEPOT", "SKU Desc")
  * Extract aggColumns: optional list of columns to aggregate (if not specified, use all numeric columns)
    * For "aggregate X on Y" pattern, X is the column to aggregate and Y is the group by column
    * For "aggregate X, group by Y" pattern, X is the column(s) to aggregate and Y is the group by column
  * Extract orderByColumn: optional column to sort results by (e.g., "risk value", "Sales")
  * Extract orderByDirection: "asc" or "desc" (default: "asc")
  * Extract aggFunc: default aggregation function ("sum", "avg", "mean", "min", "max", "count") - default is "sum"
  * Extract aggFuncs: per-column aggregation functions if user specifies different functions for different columns
  * Examples: 
    - "aggregate by Month" → groupByColumn: "Month"
    - "aggregate RISK_VOLUME on DEPOT" → groupByColumn: "DEPOT", aggColumns: ["RISK_VOLUME"]
    - "aggregate risk value, group by SKU Desc, order by risk value DESC" → groupByColumn: "SKU Desc", aggColumns: ["risk value"], orderByColumn: "risk value", orderByDirection: "desc"
    - "aggregate by Brand showing Total Sales (sum) and Avg Spend (avg)" → groupByColumn: "Brand", aggColumns: ["Sales", "Spend"], aggFuncs: {"Sales": "sum", "Spend": "avg"}
- "pivot": User wants to create a pivot table (e.g., "create a pivot on Brand showing Sales, Spend, ROI")
  * Extract pivotIndex: the column to use as pivot index/rows (e.g., "Brand", "Month")
  * Extract pivotValues: array of columns to show as metrics (e.g., ["Sales", "Spend", "ROI"])
  * Extract pivotFuncs: per-column aggregation functions if user specifies (e.g., {"Sales": "sum", "Spend": "sum", "ROI": "avg"})
  * Default aggregation function is "sum" if not specified
  * Examples: "create a pivot on Brand showing Sales, Spend, ROI", "pivot on Month showing Total Sales (sum) and Avg Spend (avg)"
- "revert": User wants to restore the data to its original form (e.g., "revert to original", "restore original data", "revert table", "go back to original")
  * This will load the original uploaded file and restore it, undoing all data operations
  * Examples: "revert to original", "restore original data", "revert table", "go back to original", "revert to original form"
- "unknown": Cannot determine intent

Return ONLY valid JSON, no other text.`;

    const response = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        { role: 'system', content: 'You are a data operations assistant. Return only valid JSON.' },
        { role: 'user', content: prompt }
      ],
      temperature: 0.3,
      max_tokens: 200,
    });

    const content = response.choices[0]?.message?.content?.trim();
    if (!content) return null;

    // Extract JSON from response (handle markdown code blocks)
    const jsonMatch = content.match(/\{[\s\S]*\}/);
    if (!jsonMatch) return null;

    const parsed = JSON.parse(jsonMatch[0]);
    
    // Map column name if provided
    if (parsed.column) {
      const matchedColumn = findMatchingColumn(parsed.column, availableColumns);
      parsed.column = matchedColumn || parsed.column;
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

    return {
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
      modelType: parsed.modelType,
      targetVariable: parsed.targetVariable,
      features: parsed.features,
      requiresClarification: method ? false : (parsed.requiresClarification || false),
      clarificationType: parsed.clarificationType,
      clarificationMessage: parsed.clarificationMessage,
    } as DataOpsIntent;
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

Extract:
1. newColumnName: The name of the new column to create
2. expression: The formula using [ColumnName] format

Examples:
- "create column XYZ with value of each row is the sum of PA nGRP Adstocked and PAB nGRP Adstocked"
  → columnName: "XYZ", expression: "[PA nGRP Adstocked] + [PAB nGRP Adstocked]"
- "create column Total = Price * Quantity"
  → columnName: "Total", expression: "[Price] * [Quantity]"
- "add two columns X and Y and name it Sum"
  → columnName: "Sum", expression: "[X] + [Y]"

Rules:
- Use [ColumnName] format for column references
- Default operation when multiple columns are mentioned is addition (+)
- Match column names to available columns (case-insensitive)

Return JSON:
{
  "columnName": "NewColumnName",
  "expression": "[Column1] + [Column2]"
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
      
      for (const match of matches) {
        const colRef = match[1];
        const matchedCol = findMatchingColumn(colRef, availableColumns);
        if (matchedCol) {
          expression = expression.replace(`[${colRef}]`, `[${matchedCol}]`);
        }
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
  // Check if user explicitly requested preview (except for 'preview' operation which always shows data)
  const shouldShowPreview = intent.operation === 'preview' || userRequestedPreview(originalMessage);
  
  // Detect large dataset
  const isLargeDataset = data.length > LARGE_DATASET_THRESHOLD;
  if (isLargeDataset) {
    console.log(`📊 Large dataset detected (${data.length} rows). Using streaming mode for operations.`);
  }
  
  if (intent.requiresClarification) {
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
        return {
          answer: `Please specify the formula for column "${newColumnName}". For example: "Create column ${newColumnName} = [Column A] + [Column B]"`
        };
      }
      
      const result = await createDerivedColumn(data, newColumnName, expression);
      
      if (result.errors && result.errors.length > 0) {
        return {
          answer: `Error creating column: ${result.errors.join(', ')}`
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
      // Group by a column and aggregate numeric columns with support for multiple aggregation functions
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

      // Determine which columns to aggregate: use explicit list or all numeric columns except groupBy
      const numericCols = new Set<string>();
      for (const row of data) {
        for (const [key, value] of Object.entries(row)) {
          if (key === groupBy) continue;
          if (typeof value === 'number') {
            numericCols.add(key);
          }
        }
      }

      const aggColumns =
        intent.aggColumns && intent.aggColumns.length > 0
          ? intent.aggColumns
          : Array.from(numericCols);

      if (aggColumns.length === 0) {
        return {
          answer: `I couldn't find any numeric columns to aggregate (other than "${groupBy}").`,
        };
      }

      // Parse aggregation functions from user message or use defaults
      const message = (originalMessage || '').toLowerCase();
      const aggFuncs: Record<string, 'sum' | 'avg' | 'mean' | 'min' | 'max' | 'count'> = intent.aggFuncs || {};
      
      // Try to infer aggregation functions from column names or message
      for (const col of aggColumns) {
        if (!aggFuncs[col]) {
          const colLower = col.toLowerCase();
          // Check if column name or message mentions specific aggregation
          // Patterns: "Total Sales" -> sum, "Avg Spend" -> avg, "Sales (Sum)" -> sum, etc.
          if (message.includes(`total ${colLower}`) || message.includes(`${colLower} (sum`) || 
              message.includes(`sum ${colLower}`) || message.includes(`sum of ${colLower}`)) {
            aggFuncs[col] = 'sum';
          } else if (message.includes(`avg ${colLower}`) || message.includes(`average ${colLower}`) ||
              message.includes(`${colLower} (avg`) || message.includes(`${colLower} (mean`) || 
              message.includes(`mean ${colLower}`) || message.includes(`avg of ${colLower}`)) {
            aggFuncs[col] = 'avg';
          } else if (message.includes(`min ${colLower}`) || message.includes(`minimum ${colLower}`) ||
              message.includes(`${colLower} (min`)) {
            aggFuncs[col] = 'min';
          } else if (message.includes(`max ${colLower}`) || message.includes(`maximum ${colLower}`) ||
              message.includes(`${colLower} (max`)) {
            aggFuncs[col] = 'max';
          } else if (message.includes(`count ${colLower}`) || message.includes(`count of ${colLower}`) ||
              message.includes(`${colLower} (count`)) {
            aggFuncs[col] = 'count';
          } else {
            // Default to sum
            aggFuncs[col] = intent.aggFunc || 'sum';
          }
        }
      }

      // Track aggregation data: for sum/avg we need sum and count, for min/max we track min/max values
      type AggBucket = {
        sum?: number;
        count: number;
        min?: number;
        max?: number;
        values: number[];
      };

      const aggMap = new Map<string, Record<string, AggBucket>>();

      for (const row of data) {
        const key = String(row[groupBy]);
        if (!aggMap.has(key)) {
          aggMap.set(key, {});
        }
        const bucket = aggMap.get(key)!;

        for (const col of aggColumns) {
          if (!bucket[col]) {
            bucket[col] = { count: 0, values: [] };
          }

          const rawVal = row[col];
          const numVal =
            typeof rawVal === 'number'
              ? rawVal
              : typeof rawVal === 'string' && rawVal.trim() !== ''
              ? Number(rawVal)
              : NaN;

          if (!Number.isNaN(numVal)) {
            const func = aggFuncs[col];
            const b = bucket[col];
            b.count++;
            b.values.push(numVal);

            if (func === 'sum' || func === 'avg' || func === 'mean') {
              b.sum = (b.sum || 0) + numVal;
            }
            if (func === 'min') {
              b.min = b.min === undefined ? numVal : Math.min(b.min, numVal);
            }
            if (func === 'max') {
              b.max = b.max === undefined ? numVal : Math.max(b.max, numVal);
            }
          }
        }
      }

      // Build aggregated data with proper column names showing aggregation function
      const aggregatedData: Record<string, any>[] = [];
      const columnNames: string[] = [groupBy];

      for (const [key, bucket] of aggMap.entries()) {
        const row: Record<string, any> = { [groupBy]: key };
        
        for (const col of aggColumns) {
          const b = bucket[col];
          if (!b || b.count === 0) continue;

          const func = aggFuncs[col];
          let value: number;
          let displayName: string;

          switch (func) {
            case 'avg':
            case 'mean':
              value = b.sum! / b.count;
              displayName = `${col} (Avg)`;
              break;
            case 'min':
              value = b.min!;
              displayName = `${col} (Min)`;
              break;
            case 'max':
              value = b.max!;
              displayName = `${col} (Max)`;
              break;
            case 'count':
              value = b.count;
              displayName = `${col} (Count)`;
              break;
            case 'sum':
            default:
              value = b.sum || 0;
              displayName = `${col} (Sum)`;
              break;
          }

          row[displayName] = Math.round(value * 100) / 100;
          if (!columnNames.includes(displayName)) {
            columnNames.push(displayName);
          }
        }
        aggregatedData.push(row);
      }

      // Apply sorting if orderByColumn is specified
      if (intent.orderByColumn) {
        // Find the display name for the order by column (it might be aggregated)
        let sortColumn = intent.orderByColumn;
        
        // Check if it's an aggregated column (has a display name)
        const matchingDisplayName = columnNames.find(name => 
          name.toLowerCase().includes(intent.orderByColumn!.toLowerCase())
        );
        if (matchingDisplayName) {
          sortColumn = matchingDisplayName;
        } else {
          // Fallback: try to find by exact match or partial match
          const exactMatch = columnNames.find(name => 
            name.toLowerCase() === intent.orderByColumn!.toLowerCase()
          );
          if (exactMatch) {
            sortColumn = exactMatch;
          }
        }

        const direction = intent.orderByDirection || 'asc';
        aggregatedData.sort((a, b) => {
          const aVal = a[sortColumn];
          const bVal = b[sortColumn];
          
          // Handle null/undefined
          if (aVal == null && bVal == null) return 0;
          if (aVal == null) return direction === 'asc' ? -1 : 1;
          if (bVal == null) return direction === 'asc' ? 1 : -1;
          
          // Numeric comparison
          if (typeof aVal === 'number' && typeof bVal === 'number') {
            return direction === 'asc' ? aVal - bVal : bVal - aVal;
          }
          
          // String comparison
          const aStr = String(aVal);
          const bStr = String(bVal);
          const comparison = aStr.localeCompare(bStr);
          return direction === 'asc' ? comparison : -comparison;
        });
      }

      const funcSummary = Object.values(aggFuncs).reduce((acc, f) => {
        acc[f] = (acc[f] || 0) + 1;
        return acc;
      }, {} as Record<string, number>);
      const funcDesc = Object.entries(funcSummary)
        .map(([f, c]) => `${c} ${f}`)
        .join(', ');

      // Save aggregated data to session (this changes the data structure permanently)
      const saveResult = await saveModifiedData(
        sessionId,
        aggregatedData,
        'aggregate',
        `Aggregated data by "${groupBy}" using ${funcDesc} for ${aggColumns.length} column${aggColumns.length === 1 ? '' : 's'}`,
        sessionDoc
      );

      let answer = `✅ I've aggregated the data by "${groupBy}" using ${funcDesc} for ${aggColumns.length} column${aggColumns.length === 1 ? '' : 's'}.`;
      if (intent.orderByColumn) {
        answer += ` Results are sorted by ${intent.orderByColumn} ${intent.orderByDirection === 'desc' ? 'descending' : 'ascending'}.`;
      }
      answer += ` The data structure has been updated - you now have ${aggregatedData.length} row${aggregatedData.length === 1 ? '' : 's'} grouped by "${groupBy}".`;

      // Get preview from saved data
      const previewData = await getPreviewFromSavedData(sessionId, aggregatedData);

      return {
        answer,
        data: aggregatedData,
        preview: previewData,
        saved: true,
      };
    }

    case 'pivot': {
      // Pivot is a specialized aggregate: group by index and aggregate selected value columns
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
      let valueColumns =
        intent.pivotValues && intent.pivotValues.length > 0
          ? intent.pivotValues
          : allColumns.filter(c => c !== indexCol);

      if (valueColumns.length === 0) {
        return {
          answer: `Please specify at least one value column for the pivot (e.g., "showing Sales, Spend").`,
        };
      }

      // Parse aggregation functions from user message or use defaults
      const message = (originalMessage || '').toLowerCase();
      const pivotFuncs: Record<string, 'sum' | 'avg' | 'mean' | 'min' | 'max' | 'count'> = intent.pivotFuncs || {};
      
      // Try to infer aggregation functions from column names or message
      for (const col of valueColumns) {
        if (!pivotFuncs[col]) {
          const colLower = col.toLowerCase();
          // Check if column name or message mentions specific aggregation
          // Patterns: "Total Sales" -> sum, "Avg Spend" -> avg, "Sales (Sum)" -> sum, etc.
          if (message.includes(`total ${colLower}`) || message.includes(`${colLower} (sum`) || 
              message.includes(`sum ${colLower}`) || message.includes(`sum of ${colLower}`)) {
            pivotFuncs[col] = 'sum';
          } else if (message.includes(`avg ${colLower}`) || message.includes(`average ${colLower}`) ||
              message.includes(`${colLower} (avg`) || message.includes(`${colLower} (mean`) || 
              message.includes(`mean ${colLower}`) || message.includes(`avg of ${colLower}`)) {
            pivotFuncs[col] = 'avg';
          } else if (message.includes(`min ${colLower}`) || message.includes(`minimum ${colLower}`) ||
              message.includes(`${colLower} (min`)) {
            pivotFuncs[col] = 'min';
          } else if (message.includes(`max ${colLower}`) || message.includes(`maximum ${colLower}`) ||
              message.includes(`${colLower} (max`)) {
            pivotFuncs[col] = 'max';
          } else if (message.includes(`count ${colLower}`) || message.includes(`count of ${colLower}`) ||
              message.includes(`${colLower} (count`)) {
            pivotFuncs[col] = 'count';
          } else {
            // Default to sum for pivot
            pivotFuncs[col] = 'sum';
          }
        }
      }

      // Track aggregation data: for sum/avg we need sum and count, for min/max we track min/max values
      type AggBucket = {
        sum?: number;
        count: number;
        min?: number;
        max?: number;
        values: number[];
      };

      const aggMap = new Map<string, Record<string, AggBucket>>();

      for (const row of data) {
        const key = String(row[indexCol]);
        if (!aggMap.has(key)) {
          aggMap.set(key, {});
        }
        const bucket = aggMap.get(key)!;

        for (const col of valueColumns) {
          if (!bucket[col]) {
            bucket[col] = { count: 0, values: [] };
          }

          const rawVal = row[col];
          const numVal =
            typeof rawVal === 'number'
              ? rawVal
              : typeof rawVal === 'string' && rawVal.trim() !== ''
              ? Number(rawVal)
              : NaN;

          if (!Number.isNaN(numVal)) {
            const func = pivotFuncs[col];
            const b = bucket[col];
            b.count++;
            b.values.push(numVal);

            if (func === 'sum' || func === 'avg' || func === 'mean') {
              b.sum = (b.sum || 0) + numVal;
            }
            if (func === 'min') {
              b.min = b.min === undefined ? numVal : Math.min(b.min, numVal);
            }
            if (func === 'max') {
              b.max = b.max === undefined ? numVal : Math.max(b.max, numVal);
            }
          }
        }
      }

      // Build pivot data with proper column names showing aggregation function
      const pivotData: Record<string, any>[] = [];
      const columnNames: string[] = [indexCol];

      for (const [key, bucket] of aggMap.entries()) {
        const row: Record<string, any> = { [indexCol]: key };
        
        for (const col of valueColumns) {
          const b = bucket[col];
          if (!b || b.count === 0) continue;

          const func = pivotFuncs[col];
          let value: number;
          let displayName: string;

          switch (func) {
            case 'avg':
            case 'mean':
              value = b.sum! / b.count;
              displayName = `${col} (Avg)`;
              break;
            case 'min':
              value = b.min!;
              displayName = `${col} (Min)`;
              break;
            case 'max':
              value = b.max!;
              displayName = `${col} (Max)`;
              break;
            case 'count':
              value = b.count;
              displayName = `${col} (Count)`;
              break;
            case 'sum':
            default:
              value = b.sum || 0;
              displayName = `${col} (Sum)`;
              break;
          }

          row[displayName] = Math.round(value * 100) / 100;
          if (!columnNames.includes(displayName)) {
            columnNames.push(displayName);
          }
        }
        pivotData.push(row);
      }

      const funcSummary = Object.values(pivotFuncs).reduce((acc, f) => {
        acc[f] = (acc[f] || 0) + 1;
        return acc;
      }, {} as Record<string, number>);
      const funcDesc = Object.entries(funcSummary)
        .map(([f, c]) => `${c} ${f}`)
        .join(', ');

      // Save pivot data to session (this changes the data structure permanently)
      const saveResult = await saveModifiedData(
        sessionId,
        pivotData,
        'pivot',
        `Created pivot on "${indexCol}" showing ${valueColumns.join(', ')} (aggregated using ${funcDesc})`,
        sessionDoc
      );

      let answer = `✅ I've created a pivot on "${indexCol}" showing ${valueColumns.join(
        ', ',
      )} (aggregated using ${funcDesc}).`;
      answer += ` The data structure has been updated - you now have ${pivotData.length} row${pivotData.length === 1 ? '' : 's'} grouped by "${indexCol}".`;

      // Get preview from saved data
      const previewData = await getPreviewFromSavedData(sessionId, pivotData);

      return {
        answer,
        data: pivotData,
        preview: previewData,
        saved: true,
      };
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
      return {
        answer: 'I can help you with data operations like:\n\n' +
          '• **Revert data**: "Revert to original" or "Restore original data"\n' +
          '• **Aggregate data**: "Aggregate by Month" or "Aggregate RISK_VOLUME on DEPOT"\n' +
          '• **Create pivot tables**: "Create a pivot on Brand showing Sales, Spend, ROI"\n' +
          '• **Remove columns**: "Remove column X" or "Delete column Y"\n' +
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

