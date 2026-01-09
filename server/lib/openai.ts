import OpenAI from "openai";

// Azure OpenAI configuration - lazy initialization
let openaiInstance: OpenAI | null = null;
let openaiEmbeddingsInstance: OpenAI | null = null;
let modelName: string | null = null;

/**
 * Initialize Azure OpenAI client for chat/completions (lazy initialization)
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

  // Create Azure OpenAI client for chat/completions
  // Note: For Azure OpenAI, baseURL should be: {endpoint}/openai/deployments/{deployment-name}
  // The SDK will append the appropriate path (e.g., /chat/completions, /embeddings)
  const chatBaseURL = `${process.env.AZURE_OPENAI_ENDPOINT}/openai/deployments/${process.env.AZURE_OPENAI_DEPLOYMENT_NAME}`;
  const apiVersion = process.env.AZURE_OPENAI_API_VERSION || '2023-05-15';
  
  console.log(`   Chat baseURL: ${chatBaseURL}`);
  console.log(`   API Version: ${apiVersion}`);
  
  openaiInstance = new OpenAI({
    apiKey: process.env.AZURE_OPENAI_API_KEY!,
    baseURL: chatBaseURL,
    defaultQuery: { 
      'api-version': apiVersion
    },
    defaultHeaders: {
      'api-key': process.env.AZURE_OPENAI_API_KEY!,
    },
  });

  // Use the deployment name as the model
  modelName = process.env.AZURE_OPENAI_DEPLOYMENT_NAME!;

  console.log("✅ Azure OpenAI initialized successfully");
  console.log(`   Endpoint: ${process.env.AZURE_OPENAI_ENDPOINT}`);
  console.log(`   Chat Deployment: ${modelName}`);
  console.log(`   API Version: ${apiVersion}`);

  return openaiInstance;
}

/**
 * Initialize separate Azure OpenAI client for embeddings
 * Embeddings require a different deployment than chat models
 * Supports separate endpoint for embeddings if deployments are in different Azure resources
 */
function getOpenAIEmbeddingsClient(): OpenAI {
  if (openaiEmbeddingsInstance) {
    return openaiEmbeddingsInstance;
  }

  console.log("🔧 Initializing Azure OpenAI Embeddings client...");

  // Check for required Azure OpenAI environment variables
  // Use separate embedding endpoint if provided, otherwise use the main endpoint
  const embeddingEndpoint = process.env.AZURE_OPENAI_EMBEDDING_ENDPOINT || process.env.AZURE_OPENAI_ENDPOINT;
  // Use separate embedding API key if provided, otherwise use the main API key
  const embeddingApiKey = process.env.AZURE_OPENAI_EMBEDDING_API_KEY || process.env.AZURE_OPENAI_API_KEY;
  
  const requiredEnvVars: string[] = [];

  if (!embeddingApiKey) {
    requiredEnvVars.push('AZURE_OPENAI_API_KEY or AZURE_OPENAI_EMBEDDING_API_KEY');
  }

  if (!embeddingEndpoint) {
    requiredEnvVars.push('AZURE_OPENAI_ENDPOINT or AZURE_OPENAI_EMBEDDING_ENDPOINT');
  }

  if (requiredEnvVars.length > 0) {
    const errorMsg = `Missing required Azure OpenAI environment variables: ${requiredEnvVars.join(', ')}. Please set these variables.`;
    console.error("❌", errorMsg);
    throw new Error(errorMsg);
  }

  // Get embedding deployment name (default to text-embedding-3-small)
  const embeddingDeployment = process.env.AZURE_OPENAI_EMBEDDING_DEPLOYMENT_NAME || 'text-embedding-3-small';
  
  console.log(`📊 Embedding Client Configuration:`);
  console.log(`   AZURE_OPENAI_EMBEDDING_ENDPOINT: ${process.env.AZURE_OPENAI_EMBEDDING_ENDPOINT || 'not set (using AZURE_OPENAI_ENDPOINT)'}`);
  console.log(`   Using endpoint: ${embeddingEndpoint}`);
  console.log(`   AZURE_OPENAI_EMBEDDING_API_KEY: ${process.env.AZURE_OPENAI_EMBEDDING_API_KEY ? '***set***' : 'not set (using AZURE_OPENAI_API_KEY)'}`);
  console.log(`   AZURE_OPENAI_EMBEDDING_DEPLOYMENT_NAME: ${process.env.AZURE_OPENAI_EMBEDDING_DEPLOYMENT_NAME || 'not set (using default)'}`);
  console.log(`   Resolved embedding deployment: ${embeddingDeployment}`);

  // Validate embedding model name
  if (embeddingDeployment.includes('gpt-4') || embeddingDeployment.includes('gpt-3.5')) {
    console.error(`❌ Invalid embedding deployment: ${embeddingDeployment}. Chat models cannot be used for embeddings.`);
    console.error(`   Falling back to: text-embedding-3-small`);
    const fallbackDeployment = 'text-embedding-3-small';
    
    const baseURL = `${embeddingEndpoint}/openai/deployments/${fallbackDeployment}`;
    console.log(`   Creating embeddings client with baseURL: ${baseURL}`);
    
    const apiVersion = process.env.AZURE_OPENAI_EMBEDDING_API_VERSION || process.env.AZURE_OPENAI_API_VERSION || '2023-05-15';
    
    openaiEmbeddingsInstance = new OpenAI({
      apiKey: embeddingApiKey!,
      baseURL: baseURL,
      defaultQuery: { 
        'api-version': apiVersion
      },
      defaultHeaders: {
        'api-key': embeddingApiKey!,
      },
    });
  } else {
    // Create Azure OpenAI client for embeddings with embedding deployment
    const baseURL = `${embeddingEndpoint}/openai/deployments/${embeddingDeployment}`;
    const apiVersion = process.env.AZURE_OPENAI_EMBEDDING_API_VERSION || process.env.AZURE_OPENAI_API_VERSION || '2023-05-15';
    
    console.log(`   Creating embeddings client with baseURL: ${baseURL}`);
    console.log(`   API Version: ${apiVersion}`);
    console.log(`   ⚠️ IMPORTANT: Ensure this endpoint matches where your deployment was created!`);
    console.log(`   ⚠️ If you see "DeploymentNotFound" error, check:`);
    console.log(`      1. The endpoint (AZURE_OPENAI_EMBEDDING_ENDPOINT or AZURE_OPENAI_ENDPOINT) matches the Azure resource where deployment exists`);
    console.log(`      2. The API key (AZURE_OPENAI_EMBEDDING_API_KEY or AZURE_OPENAI_API_KEY) is correct for this resource`);
    console.log(`      3. The deployment name "${embeddingDeployment}" matches exactly (case-sensitive)`);
    console.log(`      4. The deployment is in "Succeeded" state in Azure Portal`);
    
    openaiEmbeddingsInstance = new OpenAI({
      apiKey: embeddingApiKey!,
      baseURL: baseURL,
      defaultQuery: { 
        'api-version': apiVersion
      },
      defaultHeaders: {
        'api-key': embeddingApiKey!,
      },
    });
  }

  console.log("✅ Azure OpenAI Embeddings client initialized");
  console.log(`   Embedding Deployment: ${embeddingDeployment}`);
  console.log(`   Base URL: ${openaiEmbeddingsInstance.baseURL}`);
  console.log(`   Expected full embeddings URL: ${openaiEmbeddingsInstance.baseURL}/embeddings?api-version=${process.env.AZURE_OPENAI_EMBEDDING_API_VERSION || process.env.AZURE_OPENAI_API_VERSION || '2023-05-15'}`);

  return openaiEmbeddingsInstance;
}

// Export openai object with lazy initialization
// Use separate client for embeddings to avoid deployment name conflicts
export const openai = {
  get chat() {
    return getOpenAIClient().chat;
  },
  get embeddings() {
    // Use separate embeddings client with embedding deployment
    return getOpenAIEmbeddingsClient().embeddings;
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

