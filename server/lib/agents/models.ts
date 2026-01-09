/**
 * Model Selection Strategy
 * 
 * Optimizes cost and performance by using appropriate models for different tasks:
 * - Faster/cheaper models for classification
 * - More powerful models for generation
 */

export const MODELS = {
  // Intent classification - faster and cheaper
  intent: process.env.AZURE_OPENAI_INTENT_MODEL || process.env.AZURE_OPENAI_DEPLOYMENT_NAME || 'gpt-4o-mini',
  
  // Text generation - more powerful
  generation: process.env.AZURE_OPENAI_DEPLOYMENT_NAME || 'gpt-4o',
  
  // Embeddings - Azure OpenAI compatible models
  embeddings: process.env.AZURE_OPENAI_EMBEDDING_DEPLOYMENT_NAME || 'text-embedding-3-small',
} as const;

export type ModelType = keyof typeof MODELS;

/**
 * Get the appropriate model for a given task
 */
export function getModelForTask(task: 'intent' | 'generation' | 'embeddings'): string {
  return MODELS[task];
}

/**
 * Check if we should use a faster model for classification
 */
export function shouldUseFastModel(): boolean {
  return MODELS.intent !== MODELS.generation;
}

