import { Message, ChartSpec } from '../../shared/schema.js';

/**
 * Resolved Reference
 */
export interface ResolvedReference {
  type: 'chart' | 'insight' | 'variable' | 'column' | 'unknown';
  value: string;
  index: number; // Index in chat history
}

/**
 * Extract column name from assistant message about column creation
 * Looks for patterns like "created column X", "Successfully created column X", etc.
 */
function extractColumnNameFromMessage(message: string): string | null {
  // Pattern 1: "Successfully created column "XYZ""
  const pattern1 = /(?:successfully\s+)?created\s+column\s+["']([^"']+)["']/i;
  const match1 = message.match(pattern1);
  if (match1) return match1[1];

  // Pattern 2: "created column XYZ" (without quotes)
  const pattern2 = /(?:successfully\s+)?created\s+column\s+([^\s\n,\.]+)/i;
  const match2 = message.match(pattern2);
  if (match2) return match2[1];

  // Pattern 3: "Created derived column "XYZ""
  const pattern3 = /created\s+derived\s+column\s+["']([^"']+)["']/i;
  const match3 = message.match(pattern3);
  if (match3) return match3[1];

  // Pattern 4: Look for column name in quotes after "column"
  const pattern4 = /column\s+["']([^"']+)["']/i;
  const match4 = message.match(pattern4);
  if (match4) return match4[1];

  return null;
}

/**
 * Find the most recently created column from chat history
 */
export function findLastCreatedColumn(chatHistory: Message[]): string | null {
  if (!chatHistory || chatHistory.length === 0) {
    return null;
  }

  // Search backwards through assistant messages
  for (let i = chatHistory.length - 1; i >= 0; i--) {
    const message = chatHistory[i];
    if (message.role === 'assistant' && message.content) {
      const columnName = extractColumnNameFromMessage(message.content);
      if (columnName) {
        console.log(`✅ Found last created column: "${columnName}" at message index ${i}`);
        return columnName;
      }
    }
  }

  return null;
}

/**
 * Resolve contextual references in question
 * Replaces "that", "it", "the previous one", "above", "now do this" with explicit references
 */
export function resolveContextReferences(
  question: string,
  chatHistory: Message[]
): string {
  const questionLower = question.toLowerCase();
  
  // Expanded patterns that indicate context references
  const contextPatterns = [
    /\bthat\b/gi,
    /\bit\b/gi,
    /\bthe\s+previous\s+one\b/gi,
    /\bthe\s+last\s+one\b/gi,
    /\bthe\s+above\b/gi,
    /\babove\b/gi,
    /\bthe\s+chart\b/gi,
    /\bthat\s+chart\b/gi,
    /\bnow\s+do\s+this\b/gi,
    /\bdo\s+this\b/gi,
    /\bdo\s+that\b/gi,
    /\bdo\s+it\b/gi,
    /\bchange\s+that\b/gi,
    /\bchange\s+it\b/gi,
    /\bmodify\s+that\b/gi,
    /\bmodify\s+it\b/gi,
    /\bupdate\s+that\b/gi,
    /\bupdate\s+it\b/gi,
    /\bthe\s+previous\s+column\b/gi,
    /\bthe\s+last\s+column\b/gi,
    /\bthat\s+column\b/gi,
    /\bthe\s+above\s+column\b/gi,
  ];

  // Check if question contains context references
  const hasContextReference = contextPatterns.some(pattern => pattern.test(question));
  
  if (!hasContextReference || chatHistory.length === 0) {
    return question; // No resolution needed
  }

  let resolvedQuestion = question;
  
  // Priority 1: Look for column references (for rename/modify operations)
  const isColumnOperation = /\b(rename|change|modify|update|remove|delete|normalize|convert)\s+(?:the\s+)?(?:above|that|it|previous|last|column)/i.test(question);
  
  if (isColumnOperation) {
    const lastColumn = findLastCreatedColumn(chatHistory);
    if (lastColumn) {
      // Replace various column reference patterns
      resolvedQuestion = resolvedQuestion.replace(/\bthe\s+above\s+column\b/gi, `"${lastColumn}"`);
      resolvedQuestion = resolvedQuestion.replace(/\bthat\s+column\b/gi, `"${lastColumn}"`);
      resolvedQuestion = resolvedQuestion.replace(/\bthe\s+previous\s+column\b/gi, `"${lastColumn}"`);
      resolvedQuestion = resolvedQuestion.replace(/\bthe\s+last\s+column\b/gi, `"${lastColumn}"`);
      resolvedQuestion = resolvedQuestion.replace(/\bthe\s+above\b/gi, `"${lastColumn}"`);
      resolvedQuestion = resolvedQuestion.replace(/\babove\b/gi, `"${lastColumn}"`);
      resolvedQuestion = resolvedQuestion.replace(/\bthat\b/gi, `"${lastColumn}"`);
      resolvedQuestion = resolvedQuestion.replace(/\bit\b/gi, `"${lastColumn}"`);
      resolvedQuestion = resolvedQuestion.replace(/\bthe\s+previous\s+one\b/gi, `"${lastColumn}"`);
      resolvedQuestion = resolvedQuestion.replace(/\bthe\s+last\s+one\b/gi, `"${lastColumn}"`);
      
      console.log(`✅ Resolved column context reference: "${question}" → "${resolvedQuestion}"`);
      return resolvedQuestion;
    }
  }
  
  // Priority 2: Look for most recent chart
  for (let i = chatHistory.length - 1; i >= 0; i--) {
    const message = chatHistory[i];
    if (message.role === 'assistant' && message.charts && message.charts.length > 0) {
      const lastChart = message.charts[message.charts.length - 1];
      const chartRef = `the "${lastChart.title}" chart`;
      
      // Replace context references with explicit chart reference
      resolvedQuestion = resolvedQuestion.replace(/\bthat\s+chart\b/gi, chartRef);
      resolvedQuestion = resolvedQuestion.replace(/\bthe\s+chart\b/gi, chartRef);
      resolvedQuestion = resolvedQuestion.replace(/\bthat\b/gi, chartRef);
      resolvedQuestion = resolvedQuestion.replace(/\bit\b/gi, chartRef);
      resolvedQuestion = resolvedQuestion.replace(/\bthe\s+previous\s+one\b/gi, chartRef);
      resolvedQuestion = resolvedQuestion.replace(/\bthe\s+last\s+one\b/gi, chartRef);
      
      console.log(`✅ Resolved context reference: "${question}" → "${resolvedQuestion}"`);
      return resolvedQuestion;
    }
  }

  // Priority 3: Look for most recent insight
  for (let i = chatHistory.length - 1; i >= 0; i--) {
    const message = chatHistory[i];
    if (message.role === 'assistant' && message.insights && message.insights.length > 0) {
      const lastInsight = message.insights[message.insights.length - 1];
      const insightRef = `the "${lastInsight.text.substring(0, 50)}..." insight`;
      
      resolvedQuestion = resolvedQuestion.replace(/\bthat\b/gi, insightRef);
      resolvedQuestion = resolvedQuestion.replace(/\bit\b/gi, insightRef);
      
      console.log(`✅ Resolved context reference to insight: "${question}" → "${resolvedQuestion}"`);
      return resolvedQuestion;
    }
  }

  // Priority 4: Try to find any column reference if operation seems column-related
  if (isColumnOperation) {
    const lastColumn = findLastCreatedColumn(chatHistory);
    if (lastColumn) {
      resolvedQuestion = resolvedQuestion.replace(/\bnow\s+do\s+this\b/gi, `rename "${lastColumn}"`);
      resolvedQuestion = resolvedQuestion.replace(/\bdo\s+this\b/gi, `rename "${lastColumn}"`);
      resolvedQuestion = resolvedQuestion.replace(/\bdo\s+that\b/gi, `rename "${lastColumn}"`);
      resolvedQuestion = resolvedQuestion.replace(/\bdo\s+it\b/gi, `rename "${lastColumn}"`);
      console.log(`✅ Resolved generic action reference: "${question}" → "${resolvedQuestion}"`);
      return resolvedQuestion;
    }
  }

  // If no match found, return original
  return question;
}

/**
 * Resolve a specific context reference
 */
export function resolveContextReference(
  reference: string,
  chatHistory: Message[]
): ResolvedReference | null {
  // Look for most recent column (for column operations)
  const lastColumn = findLastCreatedColumn(chatHistory);
  if (lastColumn) {
    // Find the message index where column was created
    for (let i = chatHistory.length - 1; i >= 0; i--) {
      const message = chatHistory[i];
      if (message.role === 'assistant' && message.content) {
        const columnName = extractColumnNameFromMessage(message.content);
        if (columnName === lastColumn) {
          return {
            type: 'column',
            value: lastColumn,
            index: i,
          };
        }
      }
    }
  }

  // Look for most recent chart
  for (let i = chatHistory.length - 1; i >= 0; i--) {
    const message = chatHistory[i];
    if (message.role === 'assistant' && message.charts && message.charts.length > 0) {
      const lastChart = message.charts[message.charts.length - 1];
      return {
        type: 'chart',
        value: lastChart.title,
        index: i,
      };
    }
  }

  // Look for most recent insight
  for (let i = chatHistory.length - 1; i >= 0; i--) {
    const message = chatHistory[i];
    if (message.role === 'assistant' && message.insights && message.insights.length > 0) {
      const lastInsight = message.insights[message.insights.length - 1];
      return {
        type: 'insight',
        value: lastInsight.text,
        index: i,
      };
    }
  }

  return null;
}

