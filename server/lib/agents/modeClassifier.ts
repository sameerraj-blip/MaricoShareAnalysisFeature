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

CURRENT QUESTION: ${question}
${historyContext}

AVAILABLE DATA:
- Total rows: ${summary.rowCount}
- Total columns: ${summary.columnCount}
- Columns: ${allColumns}

CRITICAL: CONTEXT-AWARE CLASSIFICATION
The conversation history is EXTREMELY important. Short responses like "yes", "ok", "sure", "do it", "proceed", "go ahead", "that one", "the first one", "try it" are FOLLOW-UP responses that should be routed based on the PREVIOUS conversation context.

CONTEXT RULES:
- If the previous messages discuss MODELING (models, predictions, training, linear/logistic/random forest, polynomial regression), route to "modeling"
- If the previous messages discuss DATA OPERATIONS (adding columns, filtering, cleaning), route to "dataOps"  
- If the previous messages discuss ANALYSIS (correlations, charts, statistics, insights), route to "analysis"
- Short affirmative responses (yes, ok, sure, proceed, go ahead) should ALWAYS use the context from previous messages
- Responses like "create it for all variables", "use all variables", "all variables", "no create it for all variables" after a modeling question → route to "modeling"

CLASSIFICATION RULES:

1. "dataOps" - User wants to manipulate, transform, or modify the dataset itself, OR view/explore the data structure
   * HIGH PRIORITY: Questions about adding, removing, or modifying columns/rows, OR viewing data structure/preview
   * Patterns: "add column", "remove column", "delete column", "filter rows", "remove rows", 
     "transform", "clean data", "merge", "join", "split", "rename column", "change column type",
     "replace values", "fill missing", "drop duplicates", "sort data", "group by", "aggregate",
     "aggregate by", "aggregate X on Y", "pivot", "create pivot", "revert", "revert to original",
     "restore original", "data preview", "data summary", "show me data", "display data", "view data", "see data",
     "show columns", "list columns", "data structure", "data overview", "preview data",
     "show rows", "display rows", "data sample", "sample data"
   * Set confidence to 0.9+ for clear data operation requests

2. "modeling" - User wants to build, train, or create a machine learning model
   * HIGH PRIORITY: Questions about building/training ML models
   * Patterns: "build a model", "train a model", "create a model", "predict", "machine learning",
     "linear model", "logistic model", "random forest", "decision tree", "regression", "classification",
     "which model", "best model", "compare models", "evaluate model", "model performance"
   * ALSO applies to follow-up questions in a modeling conversation:
     - "yes", "ok", "sure", "do it", "proceed" (after model training question)
     - "create it for all variables", "use all variables", "all variables", "all features", "for all", "no create it for all variables" (after model training question)
   * Set confidence to 0.9+ for clear modeling requests or follow-ups in modeling context

3. "analysis" - Everything else (default mode)
   * This includes: correlation analysis, statistical queries, chart requests, comparisons,
     trend analysis, insights, exploratory data analysis, "what affects", "show me", etc.
   * This is the default mode when the query doesn't clearly fit dataOps or modeling

IMPORTANT: For short/ambiguous queries, ALWAYS check the conversation history to determine the correct mode.

Examples with context:
- Previous: "Build a linear model" → Current: "yes" → Route to "modeling" (continuing modeling conversation)
- Previous: "Train a polynomial regression model for PA TOM" → Current: "no create it for all variables" → Route to "modeling" (user wants to proceed with all variables as features)
- Previous: "Train a model for X" → Current: "create it for all variables" → Route to "modeling" (user wants to use all variables as features)
- Previous: "What's the correlation?" → Current: "show me a chart" → Route to "analysis"
- Previous: "Add a column X" → Current: "ok do it" → Route to "dataOps"
- Previous: "Which model is best?" → Current: "try the random forest" → Route to "modeling"

OUTPUT FORMAT (JSON only, no markdown):
{
  "mode": "analysis" | "dataOps" | "modeling",
  "confidence": 0.0-1.0,
  "reasoning": "Brief explanation including context consideration" (optional)
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
  
  // Check if this is a short follow-up response
  const isShortResponse = question.trim().length < 20 && 
    /^(yes|no|ok|okay|sure|do it|proceed|go ahead|try it|that one|the first|the second|sounds good|perfect|great)\b/i.test(questionLower);
  
  // If short response, check chat history for context
  if (isShortResponse && chatHistory.length > 0) {
    // Look at recent messages for context
    const recentContent = chatHistory.slice(-4).map(m => m.content.toLowerCase()).join(' ');
    
    if (recentContent.match(/\b(model|train|predict|linear|logistic|random forest|decision tree|regression|classification|machine learning|modeling|best model|which model)\b/)) {
      fallbackMode = 'modeling';
      fallbackConfidence = 0.8;
      console.log('📌 Fallback detected modeling context from chat history');
    } else if (recentContent.match(/\b(add column|remove column|filter|transform|clean|data preview|data summary|show data|display data)\b/)) {
      fallbackMode = 'dataOps';
      fallbackConfidence = 0.8;
      console.log('📌 Fallback detected dataOps context from chat history');
    }
  } else {
    // Simple pattern matching for fallback
    if (questionLower.match(/\b(add|remove|delete|filter|transform|clean|merge|join|split|rename|replace|fill|drop|sort|group|pivot)\s+(column|row|data|dataset|values?)\b/) ||
        questionLower.match(/\b(data\s+preview|data\s+summary|show\s+me\s+data|display\s+data|view\s+data|see\s+data|show\s+columns|list\s+columns|data\s+structure|data\s+overview|preview\s+data|show\s+rows|display\s+rows|data\s+sample|sample\s+data|give\s+me\s+data)\b/)) {
      fallbackMode = 'dataOps';
      fallbackConfidence = 0.7;
    } else if (questionLower.match(/\b(build|train|create|predict|machine learning|linear model|logistic|random forest|decision tree|regression|classification|which model|best model|compare model)\b/)) {
      fallbackMode = 'modeling';
      fallbackConfidence = 0.6;
    }
  }

  return {
    mode: fallbackMode,
    confidence: fallbackConfidence,
    reasoning: isShortResponse ? 'Fallback classification based on chat history context' : 'Fallback classification based on keyword matching',
  };
}

