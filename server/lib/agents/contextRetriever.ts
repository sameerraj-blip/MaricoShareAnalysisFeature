import { retrieveRelevantContext, retrieveSimilarPastQA } from '../ragService.js';
import { DataSummary, Message } from '../../shared/schema.js';
import { RetrievedContext } from './handlers/baseHandler.js';

/**
 * Retrieve context for a query using RAG
 * Falls back to data summary if RAG fails
 * Skips RAG for specific questions about particular columns/rows
 */
export async function retrieveContext(
  question: string,
  data: Record<string, any>[],
  summary: DataSummary,
  chatHistory: Message[],
  sessionId: string
): Promise<RetrievedContext> {
  // Check if this is a specific question - if so, skip RAG
  const isSpecific = isSpecificQuestion(question, summary);
  
  if (isSpecific) {
    console.log(`📌 Specific question detected - skipping RAG retrieval`);
    const mentionedColumns = extractMentionedColumns(question, summary);
    return {
      dataChunks: [], // No RAG chunks needed for specific questions
      pastQueries: [], // No past queries needed
      mentionedColumns, // Just return mentioned columns
    };
  }
  
  console.log(`🔍 General/exploratory question - using RAG retrieval`);
  
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
    const similarQA = await retrieveSimilarPastQA(question, chatHistory, 2, sessionId);

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
 * Check if question is specific about particular columns/rows
 * Specific questions mention exact column names and ask about direct relationships
 * These don't need RAG - they can be answered directly from the data
 */
export function isSpecificQuestion(question: string, summary: DataSummary): boolean {
  const questionLower = question.toLowerCase();
  const mentionedColumns = extractMentionedColumns(question, summary);
  
  // If question mentions 2+ specific columns, it's definitely specific
  if (mentionedColumns.length >= 2) {
    console.log(`   ✅ Specific: Mentions ${mentionedColumns.length} columns: ${mentionedColumns.join(', ')}`);
    return true;
  }
  
  // Check for specific patterns that indicate direct column analysis
  const specificPatterns = [
    // Correlation between specific columns
    /correlation\s+between\s+[\w\s]+\s+and\s+[\w\s]+/i,
    /correlation\s+of\s+[\w\s]+\s+with\s+[\w\s]+/i,
    // Direct comparisons with specific columns
    /\b(?:does|how|what)\s+[\w\s]+\s+(?:impact|affect|influence)\s+[\w\s]+/i,
    // Specific column mentions with operations
    /[\w\s]+\s+(?:vs|versus|compared to|compared with)\s+[\w\s]+/i,
    // Chart requests with specific columns mentioned
    /(?:show|create|generate|make)\s+(?:me\s+)?(?:a\s+)?(?:chart|graph|plot)\s+(?:of|for|with|showing)\s+[\w\s]+/i,
    // Questions about specific columns
    /(?:what|how|show)\s+(?:is|are|the)\s+[\w\s]+\s+(?:for|of|in)\s+[\w\s]+/i,
    // Seasonal/pattern questions with specific columns
    /(?:seasonal|pattern|trend)\s+(?:in|for|of)\s+[\w\s]+/i,
  ];
  
  // If question matches specific patterns AND mentions at least one column
  if (mentionedColumns.length >= 1) {
    const matchesPattern = specificPatterns.some(pattern => pattern.test(question));
    if (matchesPattern) {
      console.log(`   ✅ Specific: Matches pattern and mentions column: ${mentionedColumns[0]}`);
      return true;
    }
  }
  
  // Questions about specific rows/filters are also specific
  if (/\b(?:rows?|records?|entries?|data points?)\s+(?:where|with|that|which)/i.test(question)) {
    console.log(`   ✅ Specific: Mentions specific rows/filters`);
    return true;
  }
  
  // General/exploratory questions need RAG
  console.log(`   🔍 General/exploratory: Will use RAG for context`);
  return false;
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

