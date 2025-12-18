import { z } from 'zod';
import { openai } from '../openai.js';
import { getModelForTask } from './models.js';
import { DataSummary, Message } from '../../shared/schema.js';

/**
 * Analysis Intent Schema
 * Defines the structure of an analyzed user intent
 */
export const analysisIntentSchema = z.object({
  type: z.enum(['correlation', 'chart', 'statistical', 'conversational', 'comparison', 'ml_model', 'custom']),
  confidence: z.number().min(0).max(1),
  targetVariable: z.string().optional(),
  variables: z.array(z.string()).optional(),
  chartType: z.enum(['line', 'bar', 'scatter', 'pie', 'area']).optional(),
  filters: z.object({
    correlationSign: z.enum(['positive', 'negative', 'all']).optional(),
    excludeVariables: z.array(z.string()).optional(),
    includeOnly: z.array(z.string()).optional(),
    exceptions: z.array(z.string()).optional(),
    minCorrelation: z.number().optional(),
    maxCorrelation: z.number().optional(),
  }).optional(),
  axisMapping: z.object({
    x: z.string().optional(),
    y: z.string().optional(),
    y2: z.string().optional(),
  }).optional(),
  customRequest: z.string().optional(),
  requiresClarification: z.boolean().optional(),
  modelType: z.enum(['linear', 'logistic', 'ridge', 'lasso', 'random_forest', 'decision_tree', 'gradient_boosting', 'elasticnet', 'svm', 'knn']).optional(),
});

export type AnalysisIntent = z.infer<typeof analysisIntentSchema> & {
  originalQuestion?: string; // Added by orchestrator
  // Extended fields used by Data Ops mode
  operation?: string;
  column?: string;
  targetType?: string;
  limit?: number;
  // Extended fields for ML model requests
  modelType?: 'linear' | 'logistic' | 'ridge' | 'lasso' | 'random_forest' | 'decision_tree' | 'gradient_boosting' | 'elasticnet' | 'svm' | 'knn';
};

/**
 * Normalize question for caching
 */
function normalizeQuestion(question: string): string {
  return question
    .toLowerCase()
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/[^\w\s]/g, '');
}

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
 * Classify user intent using AI with structured output
 * Includes retry logic and validation
 */
export async function classifyIntent(
  question: string,
  chatHistory: Message[],
  summary: DataSummary,
  maxRetries: number = 2
): Promise<AnalysisIntent> {
  // Build context from chat history
  const recentHistory = chatHistory
    .slice(-10)
    .filter(msg => msg.content && msg.content.length < 500)
    .map((msg) => `${msg.role}: ${msg.content}`)
    .join('\n');

  const historyContext = recentHistory ? `\n\nCONVERSATION HISTORY:\n${recentHistory}` : '';

  // Build available columns context
  const allColumns = summary.columns.map(c => c.name).join(', ');
  const numericColumns = (summary.numericColumns || []).join(', ');
  const dateColumns = (summary.dateColumns || []).join(', ');

  const prompt = `You are an intent classifier for a data analysis AI assistant. Analyze the user's question and extract their intent.

CURRENT QUESTION: ${question}
${historyContext}

AVAILABLE DATA:
- Total rows: ${summary.rowCount}
- Total columns: ${summary.columnCount}
- All columns: ${allColumns}
- Numeric columns: ${numericColumns}
${dateColumns ? `- Date columns: ${dateColumns}` : ''}

CRITICAL: USE CONVERSATION HISTORY FOR CONTEXT
The conversation history is ESSENTIAL for understanding follow-up questions. If the previous messages discuss a specific topic (modeling, analysis, etc.), the current question likely relates to that context.

CONTEXT-AWARE CLASSIFICATION:
- If previous messages show MODEL RESULTS (coefficients, accuracy, R², RMSE, predictions), and user asks about "improving", "testing features", "alternative features", "better accuracy" → classify as "ml_model"
- Questions about model performance, feature selection, improving metrics after a model was built → classify as "ml_model"
- Short affirmative responses ("yes", "ok", "try it") → use the context from previous messages

CLASSIFICATION RULES:
1. "ml_model" - User wants to build/train/create a machine learning model OR improve/modify an existing model (including advice questions about model performance)
   * HIGH PRIORITY: Questions like "build a linear model", "train a model", "create a [model type] model", "build a model choosing X as target and Y, Z as independent variables"
   * ALSO HIGH PRIORITY (follow-ups and advice): "test alternative features", "improve accuracy", "try different features", "which features should we use", "can we improve the model", "how can we improve the model performance", "how can we improve the random forest model performance"
   * Patterns: "build a [model type] model", "train a [model type] model", "create a [model type] model", "build a model", "train a model", "test features", "try features", "improve the model"
   * Model types: linear, logistic, ridge, lasso, random forest, decision tree, gradient boosting, elasticnet, svm, knn
   * Set confidence to 0.9+ for these patterns (including advice-style questions about models)
2. "correlation" - User asks about relationships, what affects/influences something, or correlation between variables
   * HIGH PRIORITY: Questions like "what impacts X?", "what affects X?", "what influences X?", "correlation of X with all other variables" should ALWAYS be classified as "correlation"
   * These are correlation queries even if the target variable (X) is not immediately recognizable
   * Set confidence to 0.9+ for these patterns
3. "chart" - User explicitly requests a chart/visualization (line, bar, scatter, pie, area)
4. "statistical" - User asks for statistics (mean, median, average, sum, count, max, min, highest, lowest, best, worst) OR asks "which month/row/period has the [highest/lowest/best/worst] [variable]" - these are statistical queries, NOT comparison queries
5. "comparison" - User wants to compare variables, find "best" option, rank items, or asks "which is better/best" (vs, and, between, best competitor/product/brand, ranking)
6. "conversational" - Greetings, thanks, casual chat, questions about the bot
7. "custom" - Doesn't fit other categories
   * Also includes general yes/no questions that are not clearly about modeling, correlation, charts, or statistics

IMPORTANT: Questions like "what is the best competitor to X?" or "which product is best for Y?" should be classified as "comparison", NOT "correlation" or "custom".

IMPORTANT: Questions like "which month had the highest X?", "which was the best month for X?", "what is the maximum value of X?", or "which month had the best X?" should be classified as "statistical", NOT "correlation" or "comparison". The word "best" in the context of "which month/row/period" means highest/maximum value, which is a statistical query.

EXTRACTION RULES (GENERAL-PURPOSE - NO DOMAIN ASSUMPTIONS):
- For ML_MODEL intent type:
  * Extract modelType from phrases. Supported types:
    - "linear" - Linear Regression
    - "logistic" - Logistic Regression (classification)
    - "ridge" - Ridge Regression (L2 regularization)
    - "lasso" - Lasso Regression (L1 regularization)
    - "random_forest" - Random Forest
    - "decision_tree" - Decision Tree
    - "gradient_boosting" - Gradient Boosting (also matches "gbm", "xgboost")
    - "elasticnet" - ElasticNet (L1+L2 regularization)
    - "svm" - Support Vector Machine (also matches "support vector")
    - "knn" - K-Nearest Neighbors (also matches "k-nearest", "nearest neighbor")
  * If no model type specified, default to "linear"
  * Extract targetVariable: The variable to predict (from phrases like "X as target", "predicting X", "target variable X", "dependent variable X")
  * Extract variables array: Independent variables/features (from phrases like "a, b, c as independent variables", "using X, Y, Z as features", "predictors: X, Y, Z")
  * Look for patterns:
    - "build a [MODEL_TYPE] model choosing [TARGET] as target variable and [FEATURES] as independent variables"
    - "train a [MODEL_TYPE] model to predict [TARGET] using [FEATURES]"
    - "create a [MODEL_TYPE] model with [TARGET] as target and [FEATURES] as features"
- Extract targetVariable: Any entity/variable the user wants to analyze (extract from natural language, don't assume domain)
  * For questions like "what impacts X" or "what affects X", extract X as the targetVariable
  * For questions like "what influences Y", extract Y as the targetVariable
  * For questions like "correlation of X with all the other variables", extract X as the targetVariable
  * Target variables can be multi-word (e.g., "PAB nGRP", "PA TOM", "Revenue Growth")
  * Match the EXACT phrase from the question, preserving spaces and capitalization
  * Look for patterns: 
    - "what impacts/affects/influences [TARGET]"
    - "correlation of [TARGET] with all (the other) variables"
    - "correlation between [TARGET] and [OTHER]"
  * IMPORTANT: When user says "correlation of X with all the other variables", extract X as targetVariable (not "X with all the other variables")
- Extract variables array: Any related entities/variables mentioned
- Extract chartType: If user explicitly requests a chart type
- Extract filters (GENERAL constraint system):
  * correlationSign: "positive" if user wants only positive relationships (any phrasing: "only positive", "don't include negative", "exclude negative", "no negative impact", etc.)
  * correlationSign: "negative" if user wants only negative relationships
  * excludeVariables: ANY variables user wants to exclude (extract from phrases like "don't include X", "exclude Y", "not Z", "don't want X", etc.)
  * includeOnly: ANY variables user only wants to see (extract from "only show X", "just Y", "only X", etc.)
  * exceptions: Variables to exclude from "all" (extract from "all except X", "everything but Y", etc.)
  * minCorrelation/maxCorrelation: If user mentions correlation strength thresholds
- Extract relationships (GENERAL - works for ANY domain):
  * Primary entity: Extract from patterns like "X is my [entity]", "X is the [entity]", "X is [entity]" (entity can be brand, company, product, category, etc. - AI learns from context)
  * Related entities: Extract from patterns like "Y, Z are [relationship] [entities]", "Y and Z are [relationship]" (relationship can be sister, competitor, category, etc. - AI learns from context)
  * Relationship constraints: If user says "don't want [relationship] to have negative impact", extract:
    - The relationship type (sister, competitor, category, etc.)
    - The constraint (exclude negative correlations for those entities)
    - Store in excludeVariables with constraint metadata
- Extract constraints (GENERAL boolean logic):
  * Conditional filters: "if X is negative", "where Y > threshold", "above average"
  * Temporal filters: "last N months", "rolling average", "month-over-month"
  * Grouping filters: Any grouping the user defines (learned from context, not hardcoded)
- Extract axisMapping if user specifies axis assignments:
  * x: Column for X-axis (time, date, category, etc.)
  * y: Column for primary Y-axis (left axis)
  * y2: Column for secondary Y-axis (right axis) - extract from phrases like "add X on secondary Y axis", "X on secondary Y axis", "secondary Y axis: X", "add X to secondary axis"
- Set confidence: 0.9+ if clear intent, 0.7-0.9 if somewhat clear, <0.7 if ambiguous
- Set requiresClarification: true if confidence < 0.5

CRITICAL: Do NOT assume domain-specific terminology. Extract relationships and constraints GENERALLY. The AI should understand "X is my brand" and "X is my company" the same way - as defining a primary entity.

OUTPUT FORMAT (JSON only, no markdown):
{
  "type": "correlation" | "chart" | "statistical" | "conversational" | "comparison" | "ml_model" | "custom",
  "confidence": 0.0-1.0,
  "targetVariable": "column_name" | null,
  "variables": ["col1", "col2"] | null,
  "chartType": "line" | "bar" | "scatter" | "pie" | "area" | null,
  "filters": {
    "correlationSign": "positive" | "negative" | "all" | null,
    "excludeVariables": ["col1"] | null,
    "includeOnly": ["col2"] | null,
    "exceptions": ["col3"] | null,
    "minCorrelation": 0.5 | null,
    "maxCorrelation": 0.9 | null
  } | null,
  "axisMapping": {
    "x": "col1" | null,
    "y": "col2" | null,
    "y2": "col3" | null  // Secondary Y-axis (right axis) - extract from "add X on secondary Y axis", "X on secondary axis", etc.
  } | null,
  "customRequest": "original question" | null,
  "requiresClarification": true | false,
  "modelType": "linear" | "logistic" | "ridge" | "lasso" | "random_forest" | "decision_tree" | "gradient_boosting" | "elasticnet" | "svm" | "knn" | null  // Only for ml_model type
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
            content: 'You are an intent classifier. Output only valid JSON. Be precise and extract all relevant information from the user query.',
          },
          {
            role: 'user',
            content: prompt,
          },
        ],
        response_format: { type: 'json_object' },
        temperature: 0.3, // Lower temperature for more consistent classification
        max_tokens: 500,
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

      // Recursively remove ALL null values (Zod doesn't accept null for optional fields)
      const cleaned = removeNulls(parsed);
      
      // Validate with Zod schema (cleaned should have no nulls)
      if (!cleaned || typeof cleaned !== 'object') {
        throw new Error('Cleaned parsed result is invalid');
      }
      
      // Ensure required fields exist
      if (!cleaned.type || typeof cleaned.confidence !== 'number') {
        throw new Error('Missing required fields: type or confidence');
      }
      
      // Remove embeddings and other large arrays before logging
      const cleanedForLog = JSON.parse(JSON.stringify(cleaned, (key, value) => {
        if (key === 'embedding' || (Array.isArray(value) && value.length > 100)) {
          return `[Array(${Array.isArray(value) ? value.length : 'large'})]`;
        }
        return value;
      }));
      console.log('🧹 Cleaned parsed result (removed nulls):', JSON.stringify(cleanedForLog, null, 2));
      
      const validated = analysisIntentSchema.parse(cleaned);
      
      console.log(`✅ Intent classified: ${validated.type} (confidence: ${validated.confidence.toFixed(2)})`);
      
      return validated;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      console.warn(`⚠️ Intent classification attempt ${attempt + 1} failed:`, lastError.message);
      
      if (attempt < maxRetries - 1) {
        // Enhance prompt for retry
        console.log(`🔄 Retrying with enhanced prompt...`);
        // Could add more context or examples here
      }
    }
  }

  // If all retries failed, return fallback intent
  console.error('❌ Intent classification failed after retries, using fallback');
  
  // Determine fallback type based on question content
  const questionLower = question.toLowerCase();
  let fallbackType: AnalysisIntent['type'] = 'custom';
  
  if (questionLower.match(/\b(hi|hello|hey|thanks|thank you|bye)\b/)) {
    fallbackType = 'conversational';
  } else if (questionLower.match(/\b(build|train|create).*model\b/)) {
    fallbackType = 'ml_model';
  } else if (questionLower.match(/\b(what affects|correlation|relationship|influence)\b/)) {
    fallbackType = 'correlation';
  } else if (questionLower.match(/\b(chart|graph|plot|visualize|show)\b/)) {
    fallbackType = 'chart';
  }

  return {
    type: fallbackType,
    confidence: 0.3, // Low confidence for fallback
    requiresClarification: fallbackType !== 'conversational', // Don't ask for clarification on greetings
    customRequest: question,
  };
}

