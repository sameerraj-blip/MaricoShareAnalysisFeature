import OpenAI from "openai";

// Azure OpenAI configuration - lazy initialization
let openaiInstance: OpenAI | null = null;
let modelName: string | null = null;

/**
 * Initialize Azure OpenAI client (lazy initialization)
 * This allows the module to load even if env vars aren't set yet
 * But will fail with clear error when actually trying to use OpenAI
 */
function getOpenAIClient(): OpenAI {
  if (openaiInstance) {
    return openaiInstance;
  }

  console.log("🔧 Initializing Azure OpenAI...");

  // Check for required Azure OpenAI environment variables
  const requiredEnvVars = [
    'AZURE_OPENAI_API_KEY',
    'AZURE_OPENAI_ENDPOINT', 
    'AZURE_OPENAI_DEPLOYMENT_NAME'
  ];

  const missingVars = requiredEnvVars.filter(varName => !process.env[varName]);

  if (missingVars.length > 0) {
    const errorMsg = `Missing required Azure OpenAI environment variables: ${missingVars.join(', ')}. Please set AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT, and AZURE_OPENAI_DEPLOYMENT_NAME in Vercel environment variables.`;
    console.error("❌", errorMsg);
    throw new Error(errorMsg);
  }

  // Create Azure OpenAI client
  openaiInstance = new OpenAI({
    apiKey: process.env.AZURE_OPENAI_API_KEY!,
    baseURL: `${process.env.AZURE_OPENAI_ENDPOINT}/openai/deployments/${process.env.AZURE_OPENAI_DEPLOYMENT_NAME}`,
    defaultQuery: { 
      'api-version': process.env.AZURE_OPENAI_API_VERSION || '2024-02-15-preview' 
    },
    defaultHeaders: {
      'api-key': process.env.AZURE_OPENAI_API_KEY!,
    },
  });

  // Use the deployment name as the model
  modelName = process.env.AZURE_OPENAI_DEPLOYMENT_NAME!;

  console.log("✅ Azure OpenAI initialized successfully");
  console.log(`   Endpoint: ${process.env.AZURE_OPENAI_ENDPOINT}`);
  console.log(`   Deployment: ${modelName}`);
  console.log(`   API Version: ${process.env.AZURE_OPENAI_API_VERSION || '2024-02-15-preview'}`);

  return openaiInstance;
}

// Export openai object with lazy initialization
export const openai = {
  get chat() {
    return getOpenAIClient().chat;
  },
  get embeddings() {
    return getOpenAIClient().embeddings;
  },
  get models() {
    return getOpenAIClient().models;
  },
  get images() {
    return getOpenAIClient().images;
  },
  get audio() {
    return getOpenAIClient().audio;
  },
  get files() {
    return getOpenAIClient().files;
  },
  get beta() {
    return getOpenAIClient().beta;
  },
} as OpenAI;

// Export MODEL with lazy initialization
export const MODEL = (() => {
  if (!modelName) {
    getOpenAIClient();
  }
  return modelName || process.env.AZURE_OPENAI_DEPLOYMENT_NAME || '';
})() as string;

