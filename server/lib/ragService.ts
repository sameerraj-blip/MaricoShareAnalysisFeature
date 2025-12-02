import { openai } from './openai.js';
import { DataSummary, Message } from '../shared/schema.js';

// Embedding model (Azure OpenAI supports text-embedding-ada-002 or text-embedding-3-small)
const EMBEDDING_MODEL = 'text-embedding-ada-002';
const EMBEDDING_DIMENSION = 1536;

// Chunk interface
export interface DataChunk {
  id: string;
  type: 'row_group' | 'pattern' | 'column' | 'statistical' | 'past_qa';
  content: string;
  metadata: Record<string, any>;
  embedding?: number[];
  score?: number; // Relevance score
}

// Vector store (in-memory, can be upgraded to CosmosDB later)
class VectorStore {
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

  // Cosine similarity search
  search(queryEmbedding: number[], topK: number = 5): DataChunk[] {
    const results: Array<{ chunk: DataChunk; score: number }> = [];

    for (const [id, embedding] of Array.from(this.embeddings.entries())) {
      const chunk = this.chunks.get(id);
      if (!chunk) continue;

      const similarity = cosineSimilarity(queryEmbedding, embedding);
      results.push({ chunk, score: similarity });
    }

    // Sort by score descending and return top K
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

// Global vector store (session-based, cleared on new upload)
const vectorStores = new Map<string, VectorStore>();

function getVectorStore(sessionId: string): VectorStore {
  if (!vectorStores.has(sessionId)) {
    vectorStores.set(sessionId, new VectorStore());
  }
  return vectorStores.get(sessionId)!;
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
    // Note: Azure OpenAI uses the same API but may need deployment name
    // If embeddings don't work, check your Azure OpenAI deployment
    const response = await openai.embeddings.create({
      model: EMBEDDING_MODEL,
      input: text,
    });
    
    return response.data[0].embedding;
  } catch (error) {
    // Silently fail - RAG will be disabled for this chunk
    // (Logging removed to reduce console noise - will enable RAG later)
    // Return zero vector as fallback (RAG will be disabled for this chunk)
    return new Array(EMBEDDING_DIMENSION).fill(0);
  }
}

// Chunk data intelligently
export function chunkData(
  data: Record<string, any>[],
  summary: DataSummary,
  sessionId: string
): DataChunk[] {
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
  chunks.forEach(chunk => store.addChunk(chunk));
  
  return chunks;
}

// Generate embeddings for all chunks (lazy loading)
export async function generateChunkEmbeddings(sessionId: string): Promise<void> {
  const store = getVectorStore(sessionId);
  const chunks = store.getAllChunks();
  
  // Skip embedding generation for now (RAG disabled)
  // Will enable later when RAG is fully configured
  return;
  
  // Generate embeddings in batches to avoid rate limits
  const batchSize = 10;
  for (let i = 0; i < chunks.length; i += batchSize) {
    const batch = chunks.slice(i, i + batchSize);
    
    await Promise.all(
      batch.map(async (chunk) => {
        if (!chunk.embedding) {
          const embedding = await generateEmbedding(chunk.content);
          chunk.embedding = embedding;
          store.addChunk(chunk);
        }
      })
    );
    
    // Small delay to avoid rate limits
    if (i + batchSize < chunks.length) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }
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
  // RAG disabled for now - return empty array
  // Will enable later when RAG is fully configured
  return [];
  
  const store = getVectorStore(sessionId);
  
  // If store is empty, initialize it
  if (store.size() === 0) {
    chunkData(data, summary, sessionId);
    await generateChunkEmbeddings(sessionId);
  }
  
  // Generate embedding for the question
  const questionEmbedding = await generateEmbedding(question);
  
  // Semantic search
  const semanticResults = store.search(questionEmbedding, topK * 2);
  
  // Hybrid search: also do keyword matching
  const questionLower = question.toLowerCase();
  const keywordResults: DataChunk[] = [];
  
  store.getAllChunks().forEach(chunk => {
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
  return Array.from(combined.values())
    .sort((a, b) => (b.score || 0) - (a.score || 0))
    .slice(0, topK);
}

// Clear vector store for a session (when new file uploaded)
export function clearVectorStore(sessionId: string): void {
  vectorStores.delete(sessionId);
}

// Retrieve similar past questions/answers (if we have chat history)
export async function retrieveSimilarPastQA(
  question: string,
  chatHistory: Message[],
  topK: number = 2
): Promise<DataChunk[]> {
  // RAG disabled for now - return empty array
  // Will enable later when RAG is fully configured
  return [];
  
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

