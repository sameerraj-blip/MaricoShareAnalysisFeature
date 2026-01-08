import { openai } from './openai.js';
import { DataSummary, Message } from '../shared/schema.js';
import { getRedisClient } from './redisClient.js';

// Embedding model configuration
const EMBEDDING_DEPLOYMENT_NAME = process.env.AZURE_OPENAI_EMBEDDING_DEPLOYMENT_NAME || 'text-embedding-3-large';
const EMBEDDING_DIMENSION = parseInt(process.env.AZURE_OPENAI_EMBEDDING_DIMENSION || '3072', 10);

// Chunk interface
export interface DataChunk {
  id: string;
  type: 'row_group' | 'pattern' | 'column' | 'statistical' | 'past_qa';
  content: string;
  metadata: Record<string, any>;
  embedding?: number[];
  score?: number; // Relevance score
}

// In-memory vector store (fallback when Redis unavailable)
class InMemoryVectorStore {
  private chunks: Map<string, DataChunk> = new Map();
  private embeddings: Map<string, number[]> = new Map();

  addChunk(chunk: DataChunk) {
    this.chunks.set(chunk.id, chunk);
    if (chunk.embedding) {
      this.embeddings.set(chunk.id, chunk.embedding);
    }
  }

  getChunk(id: string): DataChunk | undefined {
    return this.chunks.get(id);
  }

  getAllChunks(): DataChunk[] {
    return Array.from(this.chunks.values());
  }

  search(queryEmbedding: number[], topK: number = 5): DataChunk[] {
    const results: Array<{ chunk: DataChunk; score: number }> = [];

    for (const [id, embedding] of Array.from(this.embeddings.entries())) {
      const chunk = this.chunks.get(id);
      if (!chunk) continue;

      const similarity = cosineSimilarity(queryEmbedding, embedding);
      results.push({ chunk, score: similarity });
    }

    return results
      .sort((a, b) => b.score - a.score)
      .slice(0, topK)
      .map(r => ({ ...r.chunk, score: r.score }));
  }

  clear() {
    this.chunks.clear();
    this.embeddings.clear();
  }

  size(): number {
    return this.chunks.size;
  }
}

// Redis-based persistent vector store
class PersistentVectorStore {
  private sessionId: string;

  constructor(sessionId: string) {
    this.sessionId = sessionId;
  }

  async addChunk(chunk: DataChunk): Promise<void> {
    const redis = await getRedisClient();
    if (!redis) {
      // Fallback to in-memory
      const inMemoryStore = getInMemoryStore(this.sessionId);
      inMemoryStore.addChunk(chunk);
      return;
    }

    try {
      const key = `rag:${this.sessionId}:chunk:${chunk.id}`;
      await redis.set(
        key,
        JSON.stringify({
          ...chunk,
          embedding: chunk.embedding,
        }),
        { EX: 86400 * 30 } // 30 days expiry
      );

      // Maintain index for fast search
      const indexKey = `rag:${this.sessionId}:index`;
      await redis.sAdd(indexKey, chunk.id);
    } catch (error) {
      console.error('Failed to store chunk in Redis, using in-memory fallback:', error);
      const inMemoryStore = getInMemoryStore(this.sessionId);
      inMemoryStore.addChunk(chunk);
    }
  }

  async search(queryEmbedding: number[], topK: number = 5): Promise<DataChunk[]> {
    const redis = await getRedisClient();
    if (!redis) {
      const inMemoryStore = getInMemoryStore(this.sessionId);
      return inMemoryStore.search(queryEmbedding, topK);
    }

    try {
      const indexKey = `rag:${this.sessionId}:index`;
      const chunkIds = await redis.sMembers(indexKey);

      const results: Array<{ chunk: DataChunk; score: number }> = [];

      for (const chunkId of chunkIds) {
        const key = `rag:${this.sessionId}:chunk:${chunkId}`;
        const chunkData = await redis.get(key);
        if (!chunkData) continue;

        const chunk: DataChunk = JSON.parse(chunkData);
        if (!chunk.embedding) continue;

        const similarity = cosineSimilarity(queryEmbedding, chunk.embedding);
        results.push({ chunk, score: similarity });
      }

      return results
        .sort((a, b) => b.score - a.score)
        .slice(0, topK)
        .map(r => ({ ...r.chunk, score: r.score }));
    } catch (error) {
      console.error('Failed to search Redis, using in-memory fallback:', error);
      const inMemoryStore = getInMemoryStore(this.sessionId);
      return inMemoryStore.search(queryEmbedding, topK);
    }
  }

  async getAllChunks(): Promise<DataChunk[]> {
    const redis = await getRedisClient();
    if (!redis) {
      const inMemoryStore = getInMemoryStore(this.sessionId);
      return inMemoryStore.getAllChunks();
    }

    try {
      const indexKey = `rag:${this.sessionId}:index`;
      const chunkIds = await redis.sMembers(indexKey);

      const chunks: DataChunk[] = [];
      for (const chunkId of chunkIds) {
        const key = `rag:${this.sessionId}:chunk:${chunkId}`;
        const chunkData = await redis.get(key);
        if (chunkData) {
          chunks.push(JSON.parse(chunkData));
        }
      }

      return chunks;
    } catch (error) {
      console.error('Failed to get chunks from Redis, using in-memory fallback:', error);
      const inMemoryStore = getInMemoryStore(this.sessionId);
      return inMemoryStore.getAllChunks();
    }
  }

  async clear(): Promise<void> {
    const redis = await getRedisClient();
    if (!redis) {
      const inMemoryStore = getInMemoryStore(this.sessionId);
      inMemoryStore.clear();
      return;
    }

    try {
      const indexKey = `rag:${this.sessionId}:index`;
      const chunkIds = await redis.sMembers(indexKey);

      // Delete all chunks
      for (const chunkId of chunkIds) {
        const key = `rag:${this.sessionId}:chunk:${chunkId}`;
        await redis.del(key);
      }

      // Delete index
      await redis.del(indexKey);
    } catch (error) {
      console.error('Failed to clear Redis, clearing in-memory fallback:', error);
      const inMemoryStore = getInMemoryStore(this.sessionId);
      inMemoryStore.clear();
    }
  }

  async size(): Promise<number> {
    const redis = await getRedisClient();
    if (!redis) {
      const inMemoryStore = getInMemoryStore(this.sessionId);
      return inMemoryStore.size();
    }

    try {
      const indexKey = `rag:${this.sessionId}:index`;
      return await redis.sCard(indexKey);
    } catch (error) {
      console.error('Failed to get size from Redis, using in-memory fallback:', error);
      const inMemoryStore = getInMemoryStore(this.sessionId);
      return inMemoryStore.size();
    }
  }
}

// In-memory stores (fallback)
const inMemoryStores = new Map<string, InMemoryVectorStore>();

function getInMemoryStore(sessionId: string): InMemoryVectorStore {
  if (!inMemoryStores.has(sessionId)) {
    inMemoryStores.set(sessionId, new InMemoryVectorStore());
  }
  return inMemoryStores.get(sessionId)!;
}

// Get vector store (Redis-based with in-memory fallback)
function getVectorStore(sessionId: string): PersistentVectorStore {
  return new PersistentVectorStore(sessionId);
}

// Cosine similarity calculation
function cosineSimilarity(a: number[], b: number[]): number {
  if (a.length !== b.length) return 0;
  
  let dotProduct = 0;
  let normA = 0;
  let normB = 0;
  
  for (let i = 0; i < a.length; i++) {
    dotProduct += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }
  
  const denominator = Math.sqrt(normA) * Math.sqrt(normB);
  return denominator === 0 ? 0 : dotProduct / denominator;
}

// Generate embedding for text
export async function generateEmbedding(text: string): Promise<number[]> {
  try {
    // Azure OpenAI embeddings endpoint
    // Use deployment name for Azure OpenAI
    const response = await openai.embeddings.create({
      model: EMBEDDING_DEPLOYMENT_NAME, // Use deployment name
      input: text,
      dimensions: EMBEDDING_DIMENSION, // Specify dimensions
    });
    
    return response.data[0].embedding;
  } catch (error) {
    console.error('Embedding generation error:', error);
    // Return zero vector as fallback
    return new Array(EMBEDDING_DIMENSION).fill(0);
  }
}

// Chunk data intelligently
export async function chunkData(
  data: Record<string, any>[],
  summary: DataSummary,
  sessionId: string
): Promise<DataChunk[]> {
  const chunks: DataChunk[] = [];
  const store = getVectorStore(sessionId);
  
  // 1. Column description chunks
  summary.columns.forEach((col, idx) => {
    const sampleValues = data
      .slice(0, 10)
      .map(row => row[col.name])
      .filter(v => v !== null && v !== undefined)
      .slice(0, 5);
    
    const chunk: DataChunk = {
      id: `column_${idx}_${col.name}`,
      type: 'column',
      content: `Column "${col.name}" (${col.type}): ${col.type === 'numeric' ? 'Numeric values' : 'Categorical values'}. Sample values: ${sampleValues.join(', ')}. ${summary.numericColumns.includes(col.name) ? 'This is a numeric column suitable for calculations and correlations.' : ''}`,
      metadata: {
        columnName: col.name,
        columnType: col.type,
        isNumeric: summary.numericColumns.includes(col.name),
        isDate: summary.dateColumns.includes(col.name),
      },
    };
    chunks.push(chunk);
  });

  // 2. Statistical pattern chunks (for numeric columns)
  summary.numericColumns.slice(0, 5).forEach(col => {
    const values = data
      .map(row => Number(row[col]))
      .filter(v => !isNaN(v) && isFinite(v));
    
    if (values.length === 0) return;
    
    const sorted = [...values].sort((a, b) => a - b);
    const min = sorted[0];
    const max = sorted[sorted.length - 1];
    const median = sorted[Math.floor(sorted.length / 2)];
    const avg = values.reduce((a, b) => a + b, 0) / values.length;
    
    const chunk: DataChunk = {
      id: `stat_${col}`,
      type: 'statistical',
      content: `Statistical summary for "${col}": Range from ${min.toFixed(2)} to ${max.toFixed(2)}, average ${avg.toFixed(2)}, median ${median.toFixed(2)}. Total ${values.length} valid data points.`,
      metadata: {
        columnName: col,
        min,
        max,
        avg,
        median,
        count: values.length,
      },
    };
    chunks.push(chunk);
  });

  // 3. Row group chunks (sample rows with context)
  const chunkSize = 50; // Group rows in chunks of 50
  for (let i = 0; i < Math.min(data.length, 200); i += chunkSize) {
    const chunkRows = data.slice(i, i + chunkSize);
    const rowDescriptions = chunkRows.slice(0, 3).map(row => {
      const keyValues = Object.entries(row)
        .slice(0, 5)
        .map(([key, value]) => `${key}: ${value}`)
        .join(', ');
      return `{${keyValues}}`;
    }).join('; ');
    
    const chunk: DataChunk = {
      id: `rows_${i}_${i + chunkSize}`,
      type: 'row_group',
      content: `Data rows ${i + 1} to ${Math.min(i + chunkSize, data.length)}: Sample rows - ${rowDescriptions}. This represents ${chunkRows.length} data points from the dataset.`,
      metadata: {
        startIndex: i,
        endIndex: Math.min(i + chunkSize, data.length),
        rowCount: chunkRows.length,
      },
    };
    chunks.push(chunk);
  }

  // Store chunks (embeddings will be generated lazily)
  for (const chunk of chunks) {
    await store.addChunk(chunk);
  }
  
  return chunks;
}

// Generate embeddings for all chunks (lazy loading)
export async function generateChunkEmbeddings(sessionId: string): Promise<void> {
  const store = getVectorStore(sessionId);
  const chunks = await store.getAllChunks();
  
  if (chunks.length === 0) {
    console.log('No chunks to generate embeddings for');
    return;
  }

  console.log(`📊 Generating embeddings for ${chunks.length} chunks...`);
  
  // Generate embeddings in batches to avoid rate limits
  const batchSize = 10;
  let processed = 0;
  
  for (let i = 0; i < chunks.length; i += batchSize) {
    const batch = chunks.slice(i, i + batchSize);
    
    await Promise.all(
      batch.map(async (chunk) => {
        if (!chunk.embedding) {
          try {
            const embedding = await generateEmbedding(chunk.content);
            chunk.embedding = embedding;
            await store.addChunk(chunk);
            processed++;
            if (processed % 10 === 0) {
              console.log(`  Processed ${processed}/${chunks.length} chunks...`);
            }
          } catch (error) {
            console.error(`Failed to generate embedding for chunk ${chunk.id}:`, error);
            // Continue with other chunks
          }
        }
      })
    );
    
    // Small delay to avoid rate limits
    if (i + batchSize < chunks.length) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }
  
  console.log(`✅ Completed embedding generation for ${processed} chunks`);
}

// Retrieve relevant context for a question
export async function retrieveRelevantContext(
  question: string,
  data: Record<string, any>[],
  summary: DataSummary,
  chatHistory: Message[],
  sessionId: string,
  topK: number = 5
): Promise<DataChunk[]> {
  const store = getVectorStore(sessionId);
  
  // If store is empty, initialize it
  const storeSize = await store.size();
  if (storeSize === 0) {
    console.log('📊 Initializing RAG store for session:', sessionId);
    await chunkData(data, summary, sessionId);
    await generateChunkEmbeddings(sessionId);
  }
  
  // Generate embedding for the question
  const questionEmbedding = await generateEmbedding(question);
  
  // Semantic search
  const semanticResults = await store.search(questionEmbedding, topK * 2);
  
  // Hybrid search: also do keyword matching
  const questionLower = question.toLowerCase();
  const keywordResults: DataChunk[] = [];
  const allChunks = await store.getAllChunks();
  
  allChunks.forEach(chunk => {
    const contentLower = chunk.content.toLowerCase();
    const keywords = questionLower.split(/\s+/).filter(w => w.length > 3);
    const matches = keywords.filter(kw => contentLower.includes(kw)).length;
    
    if (matches > 0) {
      keywordResults.push({
        ...chunk,
        score: matches / keywords.length, // Normalize by keyword count
      });
    }
  });
  
  // Combine and deduplicate results
  const combined = new Map<string, DataChunk>();
  
  // Add semantic results (weight: 0.7)
  semanticResults.forEach(chunk => {
    combined.set(chunk.id, {
      ...chunk,
      score: (chunk.score || 0) * 0.7,
    });
  });
  
  // Add keyword results (weight: 0.3)
  keywordResults.forEach(chunk => {
    const existing = combined.get(chunk.id);
    if (existing) {
      existing.score = (existing.score || 0) + (chunk.score || 0) * 0.3;
    } else {
      combined.set(chunk.id, {
        ...chunk,
        score: (chunk.score || 0) * 0.3,
      });
    }
  });
  
  // Sort by combined score and return top K
  const results = Array.from(combined.values())
    .sort((a, b) => (b.score || 0) - (a.score || 0))
    .slice(0, topK);
  
  console.log(`📊 RAG retrieved ${results.length} relevant chunks for query`);
  return results;
}

// Clear vector store for a session (when new file uploaded)
export async function clearVectorStore(sessionId: string): Promise<void> {
  const store = getVectorStore(sessionId);
  await store.clear();
  inMemoryStores.delete(sessionId);
  console.log(`🗑️ Cleared RAG store for session: ${sessionId}`);
}

// Store conversation context (Q&A pairs) in vector store
export async function storeConversationContext(
  question: string,
  answer: string,
  sessionId: string
): Promise<void> {
  try {
    const store = getVectorStore(sessionId);
    
    // Create Q&A chunk
    const qaChunk: DataChunk = {
      id: `qa_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      type: 'past_qa',
      content: `Question: "${question}"\nAnswer: "${answer.substring(0, 500)}"`, // Limit answer length
      metadata: {
        question,
        answer,
        timestamp: Date.now(),
      },
    };
    
    // Generate embedding for the Q&A pair
    const embedding = await generateEmbedding(qaChunk.content);
    qaChunk.embedding = embedding;
    
    // Store in vector store
    await store.addChunk(qaChunk);
    
    console.log(`✅ Stored conversation context: ${qaChunk.id}`);
  } catch (error) {
    console.error('Failed to store conversation context:', error);
    // Don't throw - this is optional
  }
}

// Retrieve similar past questions/answers (if we have chat history)
export async function retrieveSimilarPastQA(
  question: string,
  chatHistory: Message[],
  topK: number = 2,
  sessionId?: string
): Promise<DataChunk[]> {
  // First, try to get from vector store (if stored)
  if (sessionId) {
    try {
      const store = getVectorStore(sessionId);
      const allChunks = await store.getAllChunks();
      const pastQAChunks = allChunks.filter(c => c.type === 'past_qa');
      
      if (pastQAChunks.length > 0) {
        // Use semantic search on stored Q&A chunks
        const questionEmbedding = await generateEmbedding(question);
        const results = await store.search(questionEmbedding, topK * 2);
        const qaResults = results.filter(r => r.type === 'past_qa');
        
        if (qaResults.length > 0) {
          return qaResults.slice(0, topK);
        }
      }
    } catch (error) {
      console.error('Failed to retrieve from vector store, falling back to history:', error);
    }
  }
  
  // Fallback: Extract from chat history and search
  if (chatHistory.length < 2) return [];
  
  // Extract Q&A pairs from history
  const qaPairs: Array<{ question: string; answer: string }> = [];
  for (let i = 0; i < chatHistory.length - 1; i++) {
    if (chatHistory[i].role === 'user' && chatHistory[i + 1].role === 'assistant') {
      qaPairs.push({
        question: chatHistory[i].content,
        answer: chatHistory[i + 1].content,
      });
    }
  }
  
  if (qaPairs.length === 0) return [];
  
  // Generate embedding for current question
  const questionEmbedding = await generateEmbedding(question);
  
  // Find similar past questions
  const similarities: Array<{ qa: typeof qaPairs[0]; score: number }> = [];
  
  for (const qa of qaPairs) {
    const pastQEmbedding = await generateEmbedding(qa.question);
    const similarity = cosineSimilarity(questionEmbedding, pastQEmbedding);
    similarities.push({ qa, score: similarity });
  }
  
  // Return top K similar Q&A pairs
  return similarities
    .sort((a, b) => b.score - a.score)
    .slice(0, topK)
    .map(({ qa, score }) => ({
      id: `past_qa_${qa.question.substring(0, 20)}`,
      type: 'past_qa' as const,
      content: `Previous similar question: "${qa.question}"\nAnswer: "${qa.answer.substring(0, 200)}..."`,
      metadata: {
        originalQuestion: qa.question,
        originalAnswer: qa.answer,
      },
      score,
    }));
}

