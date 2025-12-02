import { retrieveRelevantContext, retrieveSimilarPastQA } from '../ragService.js';
import { DataSummary, Message } from '../../shared/schema.js';
import { RetrievedContext } from './handlers/baseHandler.js';

/**
 * Retrieve context for a query using RAG
 * Falls back to data summary if RAG fails
 */
export async function retrieveContext(
  question: string,
  data: Record<string, any>[],
  summary: DataSummary,
  chatHistory: Message[],
  sessionId: string
): Promise<RetrievedContext> {
  try {
    // Retrieve relevant data chunks
    const relevantChunks = await retrieveRelevantContext(
      question,
      data,
      summary,
      chatHistory,
      sessionId,
      5 // Top 5 most relevant chunks
    );

    // Retrieve similar past queries
    const similarQA = await retrieveSimilarPastQA(question, chatHistory, 2);

    // Extract mentioned columns from question
    const mentionedColumns = extractMentionedColumns(question, summary);

    // Build context string
    const dataChunks = relevantChunks.map(chunk => chunk.content);
    // DataChunk content should contain Q&A info, or we extract from chatHistory
    const pastQueries = similarQA.map(chunk => chunk.content).filter(Boolean);

    return {
      dataChunks,
      pastQueries,
      mentionedColumns,
    };
  } catch (error) {
    // Sanitize error to remove any large arrays/embeddings
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error('RAG retrieval error, using fallback:', errorMessage);
    
    // Fallback to basic context from data summary
    return {
      dataChunks: [
        `Dataset has ${summary.rowCount} rows and ${summary.columnCount} columns`,
        `Numeric columns: ${summary.numericColumns.join(', ')}`,
        `Date columns: ${summary.dateColumns.join(', ') || 'none'}`,
      ],
      pastQueries: [],
      mentionedColumns: extractMentionedColumns(question, summary),
    };
  }
}

/**
 * Extract mentioned column names from question
 */
function extractMentionedColumns(question: string, summary: DataSummary): string[] {
  const mentioned: string[] = [];
  const questionLower = question.toLowerCase();
  
  for (const col of summary.columns) {
    const colLower = col.name.toLowerCase();
    // Check if column name appears in question
    if (questionLower.includes(colLower) || colLower.includes(questionLower.split(' ')[0])) {
      mentioned.push(col.name);
    }
  }
  
  return mentioned;
}

/**
 * Hybrid search combining semantic and keyword matching
 * (Enhanced version - can be added later)
 */
export async function hybridSearch(
  query: string,
  sessionId: string
): Promise<string[]> {
  // For now, use existing RAG service
  // Can be enhanced with keyword matching later
  return [];
}

