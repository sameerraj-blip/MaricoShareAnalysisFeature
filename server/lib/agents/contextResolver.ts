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
 * Detect AI suggestions in previous messages
 * Looks for patterns like "Would you like me to...", "I can...", "Should I...", etc.
 * Also extracts context from the conversation to preserve target variables and relationships
 */
export function detectAISuggestion(chatHistory: Message[]): { suggestion: string; action: string; context?: { targetVariable?: string; question?: string } } | null {
  if (!chatHistory || chatHistory.length < 2) {
    return null;
  }

  // Look at the last assistant message for suggestions
  for (let i = chatHistory.length - 1; i >= 0; i--) {
    const message = chatHistory[i];
    if (message.role === 'assistant' && message.content) {
      const content = message.content.toLowerCase();
      
      // Try to find the original user question that led to this suggestion
      let originalQuestion = '';
      let targetVariable = '';
      
      // Look backwards for the user's question before this assistant message
      // Look up to 5 messages back to find the original question
      for (let j = i - 1; j >= 0 && j >= i - 5; j--) {
        const prevMessage = chatHistory[j];
        if (prevMessage.role === 'user' && prevMessage.content) {
          const userContent = prevMessage.content.trim();
          // Skip very short responses like "yes", "ok" - these are not the original question
          if (userContent.length > 3 && !/^(yes|ok|sure|yeah)$/i.test(userContent)) {
            originalQuestion = userContent;
            // Try to extract target variable from the original question
            // Pattern: "what impacts X" or "does Y impact X"
            const impactMatch = originalQuestion.match(/(?:what\s+)?(?:impacts?|affects?|influences?)\s+([a-zA-Z0-9_\s]+?)(?:\s+significantly|\s+significantly\?|\?|$)/i);
            if (impactMatch) {
              targetVariable = impactMatch[1].trim();
            } else {
              // Pattern: "does Y impact X"
              const doesMatch = originalQuestion.match(/does\s+([a-zA-Z0-9_\s]+?)\s+impact\s+([a-zA-Z0-9_\s]+?)(?:\s+significantly|\?|$)/i);
              if (doesMatch) {
                targetVariable = doesMatch[2].trim(); // X is the target
              }
            }
            console.log(`   Found original question: "${originalQuestion}"`);
            if (targetVariable) {
              console.log(`   Extracted target variable: "${targetVariable}"`);
            }
            break;
          }
        }
      }
      
      // Pattern 1: "Would you like me to create a chart to visualize..."
      const chartSuggestionMatch = content.match(/would\s+you\s+like\s+me\s+to\s+(?:create|generate|show|make|draw|provide)\s+(?:a\s+)?(?:chart|visualization|graph|plot)\s+(?:to\s+)?(?:visualize|show|display|see)\s+(.+?)(?:\?|\.|$)/i);
      if (chartSuggestionMatch) {
        const action = chartSuggestionMatch[1].trim();
        // If action is vague like "these relationships", use the original question context
        if (/these\s+relationships|these\s+correlations|the\s+relationships|the\s+correlations|these\s+results/i.test(action)) {
          if (originalQuestion) {
            // Reconstruct the query - preserve correlation language from original question
            // Check if original question is a correlation question
            const isCorrelationQuestion = /(?:does|what|how)\s+.*?\s+(?:impact|affect|influence|correlation)/i.test(originalQuestion);
            
            if (isCorrelationQuestion) {
              // For correlation questions, keep the correlation language and add chart request
              // This ensures proper intent classification as "correlation"
              const correlationQuery = originalQuestion.toLowerCase().replace(/\?$/, '');
              console.log(`   Reconstructed correlation query from original question: "${correlationQuery}"`);
              // Use "show correlation chart" to make intent clear
              const chartQuery = `show correlation chart for ${correlationQuery}`;
              console.log(`   Final query: "${chartQuery}"`);
              return {
                suggestion: message.content,
                action: chartQuery,
                context: { targetVariable, question: originalQuestion }
              };
            } else {
              // For non-correlation questions, use generic chart visualization
              const chartQuery = `show me a chart to visualize ${originalQuestion.toLowerCase().replace(/\?$/, '')}`;
              console.log(`   Reconstructed chart query from original question: "${chartQuery}"`);
              return {
                suggestion: message.content,
                action: chartQuery,
                context: { targetVariable, question: originalQuestion }
              };
            }
          } else {
            // Fallback: use a generic correlation chart request
            return {
              suggestion: message.content,
              action: `show me a chart to visualize the correlations`,
              context: { targetVariable, question: originalQuestion }
            };
          }
        }
        // If action is specific, use it directly
        return {
          suggestion: message.content,
          action: `show me a chart to visualize ${action}`,
          context: { targetVariable, question: originalQuestion }
        };
      }
      
      // Pattern 2: "I can create a chart..." or "I can show you..."
      const canSuggestionMatch = content.match(/i\s+can\s+(?:create|generate|show|make|draw|provide)\s+(?:a\s+)?(?:chart|visualization|graph|plot)\s+(?:to\s+)?(?:visualize|show|display|see)\s+(.+?)(?:\.|$)/i);
      if (canSuggestionMatch) {
        const action = canSuggestionMatch[1].trim();
        if (/these\s+relationships|these\s+correlations|the\s+relationships|the\s+correlations|these\s+results/i.test(action)) {
          if (originalQuestion) {
            const isCorrelationQuestion = /(?:does|what|how)\s+.*?\s+(?:impact|affect|influence|correlation)/i.test(originalQuestion);
            const chartQuery = isCorrelationQuestion 
              ? `show correlation chart for ${originalQuestion.toLowerCase().replace(/\?$/, '')}`
              : `show me a chart to visualize ${originalQuestion.toLowerCase().replace(/\?$/, '')}`;
            return {
              suggestion: message.content,
              action: chartQuery,
              context: { targetVariable, question: originalQuestion }
            };
          }
        }
        return {
          suggestion: message.content,
          action: `show me a chart to visualize ${action}`,
          context: { targetVariable, question: originalQuestion }
        };
      }
      
      // Pattern 3: "Should I create a chart..."
      const shouldSuggestionMatch = content.match(/should\s+i\s+(?:create|generate|show|make|draw|provide)\s+(?:a\s+)?(?:chart|visualization|graph|plot)\s+(?:to\s+)?(?:visualize|show|display|see)\s+(.+?)(?:\?|\.|$)/i);
      if (shouldSuggestionMatch) {
        const action = shouldSuggestionMatch[1].trim();
        if (/these\s+relationships|these\s+correlations|the\s+relationships|the\s+correlations|these\s+results/i.test(action)) {
          if (originalQuestion) {
            const isCorrelationQuestion = /(?:does|what|how)\s+.*?\s+(?:impact|affect|influence|correlation)/i.test(originalQuestion);
            const chartQuery = isCorrelationQuestion 
              ? `show correlation chart for ${originalQuestion.toLowerCase().replace(/\?$/, '')}`
              : `show me a chart to visualize ${originalQuestion.toLowerCase().replace(/\?$/, '')}`;
            return {
              suggestion: message.content,
              action: chartQuery,
              context: { targetVariable, question: originalQuestion }
            };
          }
        }
        return {
          suggestion: message.content,
          action: `show me a chart to visualize ${action}`,
          context: { targetVariable, question: originalQuestion }
        };
      }
      
      // Pattern 4: Generic "Would you like me to..." followed by action
      const genericSuggestionMatch = content.match(/would\s+you\s+like\s+me\s+to\s+(.+?)(?:\?|\.|$)/i);
      if (genericSuggestionMatch) {
        const action = genericSuggestionMatch[1].trim();
        // Check if it's about charts/visualizations
        if (/chart|visualization|graph|plot|visualize|show|display/i.test(action)) {
          // If action references "these relationships" and we have original question, use it
          if (/these\s+relationships|these\s+correlations|the\s+relationships|the\s+correlations|these\s+results/i.test(action)) {
            if (originalQuestion) {
              const isCorrelationQuestion = /(?:does|what|how)\s+.*?\s+(?:impact|affect|influence|correlation)/i.test(originalQuestion);
              const chartQuery = isCorrelationQuestion 
                ? `show correlation chart for ${originalQuestion.toLowerCase().replace(/\?$/, '')}`
                : `show me a chart to visualize ${originalQuestion.toLowerCase().replace(/\?$/, '')}`;
              return {
                suggestion: message.content,
                action: chartQuery,
                context: { targetVariable, question: originalQuestion }
              };
            }
          }
          return {
            suggestion: message.content,
            action: action,
            context: { targetVariable, question: originalQuestion }
          };
        }
      }
    }
  }
  
  return null;
}

/**
 * Resolve contextual references in question
 * Replaces "that", "it", "the previous one", "above", "now do this" with explicit references
 * Also handles "yes"/"ok" responses to AI suggestions
 */
export function resolveContextReferences(
  question: string,
  chatHistory: Message[]
): string {
  const questionLower = question.toLowerCase().trim();
  
  // Check if this is a confirmation response to an AI suggestion
  const isConfirmation = /^(yes|yeah|yep|yup|ok|okay|sure|absolutely|definitely|go ahead|proceed|do it|please do|that would be great|sounds good)$/i.test(questionLower);
  
  if (isConfirmation && chatHistory.length >= 2) {
    // Look for AI suggestions in previous messages
    const suggestion = detectAISuggestion(chatHistory);
    if (suggestion) {
      console.log(`✅ Detected confirmation to AI suggestion`);
      console.log(`   Original question: "${question}"`);
      console.log(`   AI suggestion: "${suggestion.suggestion}"`);
      console.log(`   Extracted action: "${suggestion.action}"`);
      
      // Use the extracted action as the new question
      // This will be processed as if the user explicitly asked for this
      const enrichedQuestion = suggestion.action;
      console.log(`   Enriched question: "${enrichedQuestion}"`);
      return enrichedQuestion;
    } else {
      console.log(`⚠️ User said "${question}" but no AI suggestion found in recent messages`);
    }
  }
  
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

