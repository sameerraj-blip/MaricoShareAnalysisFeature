/**
 * Mode Classifier
 * Top-level AI classifier that determines which mode (analysis, dataOps, or modeling)
 * a user query should route to. This sits above all other layers.
 */
import { z } from 'zod';
import { openai } from '../openai.js';
import { getModelForTask } from './models.js';
import { DataSummary, Message } from '../../shared/schema.js';

/**
 * Mode Classification Schema
 */
export const modeClassificationSchema = z.object({
  mode: z.enum(['analysis', 'dataOps', 'modeling']),
  confidence: z.number().min(0).max(1),
  reasoning: z.string().optional(),
});

export type ModeClassification = z.infer<typeof modeClassificationSchema>;

/**
 * Recursively remove ALL null values (Zod doesn't accept null for optional fields)
 */
function removeNulls(obj: any): any {
  if (obj === null || obj === undefined) {
    return undefined;
  }
  
  if (Array.isArray(obj)) {
    return obj.map(removeNulls).filter(item => item !== undefined);
  }
  
  if (typeof obj === 'object') {
    const cleaned: any = {};
    for (const [key, value] of Object.entries(obj)) {
      const cleanedValue = removeNulls(value);
      if (cleanedValue !== undefined) {
        cleaned[key] = cleanedValue;
      }
    }
    return Object.keys(cleaned).length > 0 ? cleaned : undefined;
  }
  
  return obj;
}

/**
 * Classify the top-level mode for a user query
 * This determines whether the query should route to analysis, dataOps, or modeling
 */
export async function classifyMode(
  question: string,
  chatHistory: Message[],
  summary: DataSummary,
  maxRetries: number = 2
): Promise<ModeClassification> {
  // Build context from chat history
  const recentHistory = chatHistory
    .slice(-5) // Use fewer messages for mode classification (faster)
    .filter(msg => msg.content && msg.content.length < 500)
    .map((msg) => `${msg.role}: ${msg.content}`)
    .join('\n');

  const historyContext = recentHistory ? `\n\nCONVERSATION HISTORY:\n${recentHistory}` : '';

  // Build available columns context
  const allColumns = summary.columns.map(c => c.name).join(', ');

  const prompt = `You are a mode classifier for a data analysis AI assistant. Your job is to determine which top-level mode a user query should route to.

QUESTION: ${question}
${historyContext}

AVAILABLE DATA:
- Total rows: ${summary.rowCount}
- Total columns: ${summary.columnCount}
- Columns: ${allColumns}

CLASSIFICATION RULES:

1. "dataOps" - User wants to manipulate, transform, or modify the dataset itself, OR view/explore the data structure
   * HIGH PRIORITY: Questions about adding, removing, or modifying columns/rows, OR viewing data structure/preview
   * Patterns: "add column", "remove column", "delete column", "filter rows", "remove rows", 
     "transform", "clean data", "merge", "join", "split", "rename column", "change column type",
     "replace values", "fill missing", "drop duplicates", "sort data", "group by", "pivot",
     "data preview", "data summary", "show me data", "display data", "view data", "see data",
     "show columns", "list columns", "data structure", "data overview", "preview data",
     "show rows", "display rows", "data sample", "sample data"
   * Examples:
     - "Add a new column called X"
     - "Remove the column Y"
     - "Filter rows where Z > 100"
     - "Replace all - with 0"
     - "Merge this dataset with another"
     - "Clean the data"
     - "Give me data preview"
     - "Show me data summary"
     - "Display the data"
     - "Show me the columns"
     - "Preview the data"
   * Set confidence to 0.9+ for clear data operation requests

2. "modeling" - User wants to build, train, or create a machine learning model
   * HIGH PRIORITY: Questions about building/training ML models
   * Patterns: "build a model", "train a model", "create a model", "predict", "machine learning",
     "linear model", "logistic model", "random forest", "decision tree", "regression", "classification"
   * Examples:
     - "Build a linear model"
     - "Train a model to predict X"
     - "Create a random forest model"
     - "Build a model choosing Y as target and Z, W as independent variables"
   * Set confidence to 0.9+ for clear modeling requests

3. "analysis" - Everything else (default mode)
   * This includes: correlation analysis, statistical queries, chart requests, comparisons,
     trend analysis, insights, exploratory data analysis, "what affects", "show me", etc.
   * Examples:
     - "What affects revenue?"
     - "Show me a chart of sales over time"
     - "What is the correlation between X and Y?"
     - "Which product performs best?"
     - "Tell me about the trends"
   * This is the default mode when the query doesn't clearly fit dataOps or modeling

IMPORTANT DISTINCTIONS:
- "What affects X?" → analysis (correlation/relationship analysis)
- "Add a column that shows X" → dataOps (data manipulation)
- "Build a model to predict X" → modeling (ML model creation)
- "Show me trends" → analysis (data visualization/analysis)
- "Filter the data" → dataOps (data manipulation)
- "Train a model" → modeling (ML model creation)
- "Give me data preview" → dataOps (viewing data structure/preview)
- "Show me data summary" → dataOps (viewing data overview/structure)
- "Display the data" → dataOps (viewing data)
- "What is the summary of the data?" → dataOps (viewing data structure)
- "Show me the columns" → dataOps (viewing data structure)

OUTPUT FORMAT (JSON only, no markdown):
{
  "mode": "analysis" | "dataOps" | "modeling",
  "confidence": 0.0-1.0,
  "reasoning": "Brief explanation of why this mode was chosen" (optional)
}`;

  let lastError: Error | null = null;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const model = getModelForTask('intent');
      
      const response = await openai.chat.completions.create({
        model,
        messages: [
          {
            role: 'system',
            content: 'You are a mode classifier. Output only valid JSON. Be precise in determining the correct mode.',
          },
          {
            role: 'user',
            content: prompt,
          },
        ],
        response_format: { type: 'json_object' },
        temperature: 0.2, // Lower temperature for more consistent classification
        max_tokens: 200,
      });

      const content = response.choices[0].message.content;
      if (!content) {
        throw new Error('Empty response from LLM');
      }

      // Parse JSON
      let parsed: any;
      try {
        parsed = JSON.parse(content);
      } catch (parseError) {
        // Try to extract JSON from markdown code blocks
        const jsonMatch = content.match(/```(?:json)?\s*(\{[\s\S]*\})\s*```/);
        if (jsonMatch) {
          parsed = JSON.parse(jsonMatch[1]);
        } else {
          throw parseError;
        }
      }

      // Recursively remove ALL null values
      const cleaned = removeNulls(parsed);
      
      // Validate with Zod schema
      if (!cleaned || typeof cleaned !== 'object') {
        throw new Error('Cleaned parsed result is invalid');
      }
      
      // Ensure required fields exist
      if (!cleaned.mode || typeof cleaned.confidence !== 'number') {
        throw new Error('Missing required fields: mode or confidence');
      }
      
      const validated = modeClassificationSchema.parse(cleaned);
      
      console.log(`✅ Mode classified: ${validated.mode} (confidence: ${validated.confidence.toFixed(2)})${validated.reasoning ? ` - ${validated.reasoning}` : ''}`);
      
      return validated;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      console.warn(`⚠️ Mode classification attempt ${attempt + 1} failed:`, lastError.message);
      
      if (attempt < maxRetries - 1) {
        console.log(`🔄 Retrying mode classification...`);
      }
    }
  }

  // If all retries failed, use fallback logic
  console.error('❌ Mode classification failed after retries, using fallback');
  
  const questionLower = question.toLowerCase();
  let fallbackMode: 'analysis' | 'dataOps' | 'modeling' = 'analysis';
  let fallbackConfidence = 0.3;
  
  // Simple pattern matching for fallback
  if (questionLower.match(/\b(add|remove|delete|filter|transform|clean|merge|join|split|rename|replace|fill|drop|sort|group|pivot)\s+(column|row|data|dataset|values?)\b/) ||
      questionLower.match(/\b(data\s+preview|data\s+summary|show\s+me\s+data|display\s+data|view\s+data|see\s+data|show\s+columns|list\s+columns|data\s+structure|data\s+overview|preview\s+data|show\s+rows|display\s+rows|data\s+sample|sample\s+data|give\s+me\s+data)\b/)) {
    fallbackMode = 'dataOps';
    fallbackConfidence = 0.7;
  } else if (questionLower.match(/\b(build|train|create|predict|machine learning|linear model|logistic|random forest|decision tree|regression|classification)\b/)) {
    fallbackMode = 'modeling';
    fallbackConfidence = 0.6;
  }

  return {
    mode: fallbackMode,
    confidence: fallbackConfidence,
    reasoning: 'Fallback classification based on keyword matching',
  };
}

