/**
 * Data Ops Orchestrator
 * Handles intent parsing, clarification flow, and coordinates data operations
 */
import { Message, DataSummary } from '../../shared/schema.js';
import { removeNulls, getDataPreview, getDataSummary, convertDataType, createDerivedColumn, trainMLModel } from './pythonService.js';
import { saveModifiedData } from './dataPersistence.js';
import { getChatBySessionIdEfficient, updateChatDocument, ChatDocument } from '../../models/chat.model.js';
import { openai } from '../openai.js';

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

export interface DataOpsIntent {
  operation: 'remove_nulls' | 'preview' | 'summary' | 'convert_type' | 'count_nulls' | 'describe' | 'create_derived_column' | 'create_column' | 'modify_column' | 'normalize_column' | 'remove_column' | 'remove_rows' | 'add_row' | 'train_model' | 'unknown';
  column?: string;
  method?: 'delete' | 'mean' | 'median' | 'mode' | 'custom';
  customValue?: any;
  targetType?: 'numeric' | 'string' | 'date' | 'percentage' | 'boolean';
  limit?: number;
  newColumnName?: string;
  expression?: string;
  defaultValue?: any; // For creating columns with static values
  transformType?: 'add' | 'subtract' | 'multiply' | 'divide';
  transformValue?: number;
  rowPosition?: 'first' | 'last';
  rowIndex?: number;
  requiresClarification: boolean;
  clarificationType?: 'column' | 'method' | 'target_type';
  clarificationMessage?: string;
  // ML model fields
  modelType?: 'linear' | 'logistic' | 'ridge' | 'lasso' | 'random_forest' | 'decision_tree';
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
  // ML Model training intent (handled BEFORE clarification so new model
  // requests are not mistaken for follow-ups to other operations)
  // ---------------------------------------------------------------------------
  // Explicit model building verbs
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
  
  // Follow-up / conversational model requests that reference an existing model
  // Examples:
  // - "for the above can you choose a model with less variance"
  // - "can you give me a model with the lowest variance"
  // - "try a different model type for that"
  if (
    lowerMessage.includes('model') &&
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
  
  // Check for pending operation from context
  const dataOpsContext = sessionDoc?.dataOpsContext as DataOpsContext | undefined;
  const pendingOp = dataOpsContext?.pendingOperation;
  
  // Handle clarification responses
  if (pendingOp) {
    const age = Date.now() - pendingOp.timestamp;
    if (age < 5 * 60 * 1000) { // 5 minutes TTL
      return handleClarificationResponse(message, pendingOp, availableColumns, dataSummary);
    }
  }
  
  // Remove nulls intent
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
  
  // Preview intent
  if (lowerMessage.includes('show') && (lowerMessage.includes('data') || lowerMessage.includes('rows'))) {
    const limitMatch = lowerMessage.match(/(?:top|first|show)\s+(\d+)/i);
    const limit = limitMatch ? parseInt(limitMatch[1], 10) : 50;
    
    return {
      operation: 'preview',
      limit: Math.min(limit, 10000), // Cap at 10k
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
    const mentionedColumn = findMentionedColumn(message, availableColumns);
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
  
  // Type conversion intent
  const typeMatch = lowerMessage.match(/convert\s+(\w+)\s+to\s+(numeric|string|date|percentage|boolean|number)/i);
  if (typeMatch) {
    const columnName = typeMatch[1];
    const targetTypeRaw = typeMatch[2].toLowerCase();
    const matchedColumn = findMatchingColumn(columnName, availableColumns);
    
    if (matchedColumn) {
      const normalizedTarget = (targetTypeRaw === 'number' ? 'numeric' : targetTypeRaw) as 'numeric' | 'string' | 'date' | 'percentage' | 'boolean';
      return {
        operation: 'convert_type',
        column: matchedColumn,
        targetType: normalizedTarget,
        requiresClarification: false
      };
    } else {
      return {
        operation: 'convert_type',
        requiresClarification: true,
        clarificationType: 'column',
        clarificationMessage: `Column "${columnName}" not found. Please specify a valid column name.`
      };
    }
  }
  
  // Try AI-based intent detection for conversational queries
  try {
    const aiIntent = await detectDataOpsIntentWithAI(message, availableColumns);
    if (aiIntent && aiIntent.operation !== 'unknown') {
      return aiIntent;
    }
  } catch (error) {
    console.error('AI intent detection failed, using fallback:', error);
  }
  
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
    "operation": "remove_nulls" | "preview" | "summary" | "convert_type" | "count_nulls" | "describe" | "create_derived_column" | "create_column" | "modify_column" | "normalize_column" | "remove_rows" | "add_row" | "remove_column" | "train_model" | "unknown",
  "column": "column_name" (if specific column mentioned for single-column operations, null otherwise),
  "newColumnName": "NewColumnName" (if creating new column, null otherwise),
  "expression": "[Column1] + [Column2]" (if creating derived column, use [ColumnName] format, null otherwise),
  "defaultValue": any (if creating static column),
  "transformType": "add" | "subtract" | "multiply" | "divide" (if modifying existing column),
  "transformValue": number (if modifying existing column),
  "rowPosition": "first" | "last" (if removing rows),
  "rowIndex": number (if removing row by index),
  "requiresClarification": false,
  "clarificationMessage": null
}

Operations:
- "train_model": User wants to build/train/create a machine learning model (e.g., "build a linear model", "train a model", "create a model")
  * Extract modelType: "linear", "logistic", "ridge", "lasso", "random_forest", "decision_tree" (default: "linear")
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
- "remove_rows": User wants to remove first/last or a specific row (e.g., "remove last row", "delete row 5")
  * Extract rowPosition (first/last) or rowIndex (1-based)
- "add_row": User wants to add/append a row (e.g., "add a new row", "append row at bottom")
- "count_nulls": User wants to count/null values (e.g., "how many nulls", "count missing values")
- "describe": User wants general info about data (e.g., "how many rows", "describe the data", "what's in the dataset")
- "preview": User wants to see data (e.g., "show data", "display rows")
- "summary": User wants statistics summary
- "remove_nulls": User wants to remove/handle nulls
- "convert_type": User wants to convert column type
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

    return {
      operation: parsed.operation || 'unknown',
      column: parsed.column,
      newColumnName: parsed.newColumnName,
      expression: parsed.expression,
      defaultValue: parsed.defaultValue,
      modelType: parsed.modelType,
      targetVariable: parsed.targetVariable,
      features: parsed.features,
      requiresClarification: parsed.requiresClarification || false,
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
      // User is specifying method
      if (lowerMessage.includes('delete') || lowerMessage.includes('remove') || lowerMessage.includes('option a')) {
        return {
          operation: 'remove_nulls',
          column: pendingOp.column,
          method: 'delete',
          requiresClarification: false
        };
      } else if (lowerMessage.includes('mean') || lowerMessage.includes('average')) {
        return {
          operation: 'remove_nulls',
          column: pendingOp.column,
          method: 'mean',
          requiresClarification: false
        };
      } else if (lowerMessage.includes('median')) {
        return {
          operation: 'remove_nulls',
          column: pendingOp.column,
          method: 'median',
          requiresClarification: false
        };
      } else if (lowerMessage.includes('mode') || lowerMessage.includes('most frequent')) {
        return {
          operation: 'remove_nulls',
          column: pendingOp.column,
          method: 'mode',
          requiresClarification: false
        };
      } else if (lowerMessage.includes('custom') || lowerMessage.match(/\d+/)) {
        const numberMatch = lowerMessage.match(/(-?\d+\.?\d*)/);
        const customValue = numberMatch ? parseFloat(numberMatch[1]) : undefined;
        return {
          operation: 'remove_nulls',
          column: pendingOp.column,
          method: 'custom',
          customValue,
          requiresClarification: false
        };
      }
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
 * Find mentioned column in message
 */
function findMentionedColumn(message: string, availableColumns: string[]): string | undefined {
  const lowerMessage = message.toLowerCase();
  const sortedColumns = [...availableColumns].sort((a, b) => b.length - a.length);
  
  // Try exact/substring match, prioritizing longer column names first
  for (const col of sortedColumns) {
    if (lowerMessage.includes(col.toLowerCase())) {
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
        
        // Try multiple patterns for target variable
        const targetMatch = content.match(/Target Variable:\s*([^\n]+)/i) || 
                           content.match(/target[:\s]+([^\n]+)/i);
        
        // Try multiple patterns for features
        const featuresMatch = content.match(/Features:\s*([^\n]+)/i) ||
                             content.match(/features[:\s]+([^\n]+)/i);
        
        // Try multiple patterns for model type
        const modelTypeMatch = content.match(/trained a (\w+(?:\s+\w+)?)\s+model/i) ||
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
): Promise<{ modelType?: 'linear' | 'logistic' | 'ridge' | 'lasso' | 'random_forest' | 'decision_tree'; targetVariable?: string; features?: string[] } | null> {
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
                               messageLower.includes('that model') || 
                               messageLower.includes('previous model') ||
                               messageLower.includes('the above') ||
                               messageLower.includes('same') && messageLower.includes('model');
    
    // Determine model type based on user request
    let suggestedModelType = 'linear';
    if (messageLower.includes('less variance') || messageLower.includes('reduce variance') || messageLower.includes('lower variance')) {
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
1. modelType: "linear", "logistic", "ridge", "lasso", "random_forest", "decision_tree"
   ${messageLower.includes('less variance') || messageLower.includes('reduce variance') ? '   → If user wants "less variance", use "ridge" or "lasso" (prefer "ridge")' : ''}
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
  "modelType": "linear" | "logistic" | "ridge" | "lasso" | "random_forest" | "decision_tree",
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
}> {
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
      const result = await removeNulls(
        data,
        intent.column,
        intent.method || 'delete',
        intent.customValue
      );
      
      // Save modified data first
      const saveResult = await saveModifiedData(
        sessionId,
        result.data,
        'remove_nulls',
        `Removed nulls from ${intent.column || 'all columns'} using ${intent.method}`,
        sessionDoc
      );
      
      // Get preview from saved rawData
      const previewData = await getPreviewFromSavedData(sessionId, result.data);
      
      return {
        answer: `✅ Removed ${result.nulls_removed} null value(s). Rows: ${result.rows_before} → ${result.rows_after}. Here's a preview of the updated data:`,
        data: result.data,
        preview: previewData,
        saved: true
      };
    }
    
    case 'preview': {
      const result = await getDataPreview(data, intent.limit || 50);
      
      return {
        answer: `Showing ${result.returned_rows} of ${result.total_rows} rows:`,
        preview: result.data
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
      const modifiedData = data.map(row => ({
        ...row,
        [newColumnName!]: defaultValue !== undefined ? defaultValue : null
      }));
      
      // Save modified data first
      const saveResult = await saveModifiedData(
        sessionId,
        modifiedData,
        'create_column',
        `Created column "${newColumnName}" with default value: ${defaultValue !== undefined ? String(defaultValue) : 'null'}`,
        sessionDoc
      );
      
      // Get preview from saved rawData (reload document to get updated rawData)
      const updatedDoc = await getChatBySessionIdEfficient(sessionId);
      const previewData = updatedDoc?.rawData ? updatedDoc.rawData.slice(0, 50) : modifiedData.slice(0, 50);
      
      return {
        answer: `✅ Successfully created column "${newColumnName}"${defaultValue !== undefined ? ` with value "${defaultValue}"` : ''}. Here's a preview of the updated data:`,
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
      
      // Get preview from saved rawData
      const previewData = await getPreviewFromSavedData(sessionId, result.data);
      
      return {
        answer: `✅ Successfully created column "${newColumnName}" with expression: ${expression}\n\nHere's a preview of the updated data:`,
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

      const min = Math.min(...numericValues);
      const max = Math.max(...numericValues);
      const range = max - min;

      const modifiedData = data.map(row => {
        const newRow = { ...row };
        const currentValue = normalizeNumericValue(row[intent.column!]);
        if (currentValue === null) {
          newRow[intent.column!] = null;
        } else if (range === 0) {
          newRow[intent.column!] = 0;
        } else {
          newRow[intent.column!] = (currentValue - min) / range;
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

      // Get preview from saved rawData
      const previewData = await getPreviewFromSavedData(sessionId, modifiedData);

      return {
        answer: `✅ Normalized column "${intent.column}" using min-max scaling (0-1). Here's a preview:`,
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

        newRow[intent.column!] = updatedValue;
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

      // Get preview from saved rawData
      const previewData = await getPreviewFromSavedData(sessionId, modifiedData);

      return {
        answer: `✅ Updated column "${intent.column}" by ${intent.transformType === 'add' ? 'adding' : intent.transformType === 'subtract' ? 'subtracting' : intent.transformType === 'multiply' ? 'multiplying by' : 'dividing by'} ${intent.transformValue}. Here's a preview of the updated data:`,
        data: modifiedData,
        preview: previewData,
        saved: true
      };
    }

    case 'remove_rows': {
      if (data.length === 0) {
        return { answer: 'There are no rows to remove.' };
      }

      let targetIndex: number | null = null;
      if (intent.rowIndex && intent.rowIndex > 0 && intent.rowIndex <= data.length) {
        targetIndex = intent.rowIndex - 1;
      } else if (intent.rowPosition === 'first') {
        targetIndex = 0;
      } else if (intent.rowPosition === 'last') {
        targetIndex = data.length - 1;
      }

      if (targetIndex === null) {
        return { answer: 'Please specify which row to remove (first, last, or row number).' };
      }

      const modifiedData = data.filter((_, idx) => idx !== targetIndex);

      // Save modified data first
      const saveResult = await saveModifiedData(
        sessionId,
        modifiedData,
        'remove_rows',
        `Removed row ${targetIndex + 1}`,
        sessionDoc
      );

      // Get preview from saved rawData
      const previewData = await getPreviewFromSavedData(sessionId, modifiedData);

      return {
        answer: `✅ Removed row ${targetIndex + 1}. Here's a preview of the updated data:`,
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

      // Get preview from saved rawData
      const previewData = await getPreviewFromSavedData(sessionId, modifiedData);

      return {
        answer: `✅ Added a new empty row at the bottom. Here's a preview:`,
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
      
      // Get preview from saved rawData
      const previewData = await getPreviewFromSavedData(sessionId, modifiedData);
      
      return {
        answer: `✅ Successfully removed column "${intent.column}". Here's a preview of the updated data:`,
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
      
      // Get preview from saved rawData
      const previewData = await getPreviewFromSavedData(sessionId, result.data);
      
      const errorMsg = result.conversion_info.errors.length > 0
        ? ` Note: ${result.conversion_info.errors.join(', ')}`
        : '';
      
      return {
        answer: `✅ Converted "${intent.column}" to ${intent.targetType}.${errorMsg} Here's a preview:`,
        data: result.data,
        preview: previewData,
        saved: true
      };
    }
    
    case 'train_model': {
      // Extract model parameters from intent or use AI to extract from message
      let modelType: 'linear' | 'logistic' | 'ridge' | 'lasso' | 'random_forest' | 'decision_tree' = intent.modelType || 'linear';
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
      
      // Check if user wants less variance (Ridge/Lasso)
      const messageText = originalMessage || sessionDoc?.messages?.slice().reverse().find(m => m.role === 'user')?.content || '';
      const messageLower = messageText.toLowerCase();
      const wantsLessVariance = messageLower.includes('less variance') || 
                                messageLower.includes('reduce variance') || 
                                messageLower.includes('lower variance');
      
      // If user wants less variance and we have previous model, default to Ridge
      if (wantsLessVariance && previousModelParams && !intent.modelType) {
        modelType = 'ridge';
        console.log(`🎯 User wants less variance, using Ridge model`);
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
        
        // If still missing and we have previous model params, use them
        if ((!targetVariable || features.length === 0) && previousModelParams) {
          console.log(`📋 Using previous model parameters from chat history`);
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
    
    default:
      // For unknown operations, try to provide a helpful response
      return {
        answer: 'I can help you with data operations like:\n\n' +
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

