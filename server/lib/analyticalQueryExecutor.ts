/**
 * Analytical Query Executor
 * Detects analytical questions and executes queries on CSV data before sending to Azure AI
 * This ensures the AI receives actual query results to answer accurately
 */

import { parseUserQuery } from './queryParser.js';
import { applyQueryTransformations } from './dataTransform.js';
import { DataSummary, Message } from '../shared/schema.js';
import type { ParsedQuery } from '../shared/queryTypes.js';

export interface AnalyticalQueryResult {
  isAnalytical: boolean;
  queryResults?: {
    data: Record<string, any>[];
    summary: string;
    formattedResults: string;
  };
  parsedQuery?: ParsedQuery;
}

/**
 * Detects if a question is an analytical question that requires querying CSV data
 */
function isAnalyticalQuestion(question: string): boolean {
  const lowerQuestion = question.toLowerCase().trim();
  
  // Skip very short responses that are likely conversational
  if (lowerQuestion.length < 10 && /^(no|yes|ok|okay|sure|alright|thanks|thank you)$/i.test(lowerQuestion)) {
    return false;
  }
  
  // Patterns that indicate analytical questions
  const analyticalPatterns = [
    // Questions asking for specific values (even if prefixed with "no" or "please")
    /\b(what|which|how many|how much|show me|give me|tell me|find|calculate|compute|please take|use|help)\b/i,
    // Questions with aggregations
    /\b(total|sum|average|mean|count|maximum|minimum|max|min|aggregate|aggregated|generated)\b/i,
    // Questions with time periods
    /\b(during|in|from|to|between|year|month|quarter|week|day|period|time|before|after|since)\b/i,
    // Questions with filters
    /\b(for|where|with|having|that|which|specific|from|column)\b/i,
    // Questions asking for comparisons
    /\b(compare|comparison|versus|vs|difference|more than|less than|above|below|exceeding|exceed)\b/i,
    // Questions mentioning columns explicitly
    /\b(column|columns|region|category|product|customer|order|date)\b/i,
  ];
  
  // Check if question matches analytical patterns
  const hasAnalyticalPattern = analyticalPatterns.some(pattern => pattern.test(lowerQuestion));
  
  // Special case: Questions that mention columns and ask for analysis
  const hasColumnMention = /\b(column|region|category|product|customer|order|date|value|total|revenue|sales)\b/i.test(lowerQuestion);
  const hasAnalysisRequest = /\b(find|which|what|show|give|tell|calculate|generated|more than|less than|exceeding)\b/i.test(lowerQuestion);
  
  // Exclude purely conversational questions
  const conversationalPatterns = [
    /^(hi|hello|hey|thanks|thank you|ok|okay|yes|no|sure|alright)$/i,
    /^(what is|what are|explain|tell me about|how does|how do|why)\s+(the|a|an)\s+/i,
  ];
  
  const isConversational = conversationalPatterns.some(pattern => pattern.test(lowerQuestion.trim()));
  
  // If it mentions columns and asks for analysis, it's analytical
  if (hasColumnMention && hasAnalysisRequest) {
    return true;
  }
  
  return hasAnalyticalPattern && !isConversational;
}

/**
 * Formats query results into a readable string for the AI
 */
function formatQueryResults(
  data: Record<string, any>[],
  parsedQuery: ParsedQuery,
  summary: DataSummary
): { summary: string; formattedResults: string } {
  if (data.length === 0) {
    return {
      summary: 'No data matches the query criteria.',
      formattedResults: 'No results found.',
    };
  }

  // Build summary
  const summaryParts: string[] = [];
  
  if (parsedQuery.aggregations && parsedQuery.aggregations.length > 0) {
    const aggDescriptions = parsedQuery.aggregations.map(agg => {
      const alias = agg.alias || `${agg.column}_${agg.operation}`;
      return `${agg.operation}(${agg.column}) as ${alias}`;
    });
    summaryParts.push(`Aggregated: ${aggDescriptions.join(', ')}`);
  }
  
  if (parsedQuery.groupBy && parsedQuery.groupBy.length > 0) {
    summaryParts.push(`Grouped by: ${parsedQuery.groupBy.join(', ')}`);
  }
  
  if (parsedQuery.timeFilters && parsedQuery.timeFilters.length > 0) {
    const timeDesc = parsedQuery.timeFilters.map(tf => {
      if (tf.type === 'year' && tf.years) {
        return `Year: ${tf.years.join(', ')}`;
      }
      if (tf.type === 'month' && tf.months) {
        return `Month: ${tf.months.join(', ')}`;
      }
      if (tf.type === 'dateRange' && tf.startDate && tf.endDate) {
        return `Date Range: ${tf.startDate} to ${tf.endDate}`;
      }
      return `Time filter: ${tf.type}`;
    }).join('; ');
    summaryParts.push(timeDesc);
  }
  
  if (parsedQuery.valueFilters && parsedQuery.valueFilters.length > 0) {
    const valueDesc = parsedQuery.valueFilters.map(vf => {
      const ref = vf.reference ? ` (${vf.reference})` : '';
      return `${vf.column} ${vf.operator} ${vf.value || vf.reference}${ref}`;
    }).join('; ');
    summaryParts.push(`Filtered: ${valueDesc}`);
  }
  
  const summaryText = summaryParts.length > 0 
    ? `Query executed: ${summaryParts.join(' | ')}. Found ${data.length} result(s).`
    : `Query executed. Found ${data.length} result(s).`;

  // Format results
  let formattedResults = '';
  
  if (data.length <= 20) {
    // Show all results for small datasets
    formattedResults = JSON.stringify(data, null, 2);
  } else {
    // Show first 10 and last 10 for large datasets
    const first10 = data.slice(0, 10);
    const last10 = data.slice(-10);
    formattedResults = `First 10 results:\n${JSON.stringify(first10, null, 2)}\n\n... (${data.length - 20} more rows) ...\n\nLast 10 results:\n${JSON.stringify(last10, null, 2)}`;
  }

  return {
    summary: summaryText,
    formattedResults,
  };
}

/**
 * Executes analytical query on CSV data
 * Returns query results that can be injected into the AI prompt
 * @param preParsedQuery - Optional already parsed query to avoid duplicate parsing
 */
export async function executeAnalyticalQuery(
  question: string,
  data: Record<string, any>[],
  summary: DataSummary,
  chatHistory: Message[] = [],
  preParsedQuery?: ParsedQuery | null
): Promise<AnalyticalQueryResult> {
  // Check if this is an analytical question
  if (!isAnalyticalQuestion(question)) {
    return {
      isAnalytical: false,
    };
  }

  try {
    console.log('🔍 Detected analytical question, executing query...');
    
    // Use pre-parsed query if available, otherwise parse
    let parsedQuery: ParsedQuery | null = preParsedQuery ?? null;
    
    if (!parsedQuery) {
      parsedQuery = await parseUserQuery(question, summary, chatHistory);
    }
    
    if (!parsedQuery || parsedQuery.confidence < 0.3) {
      console.log('⚠️ Low confidence in query parsing, skipping query execution');
      return {
        isAnalytical: true,
        parsedQuery,
      };
    }

    console.log('📊 Parsed query:', JSON.stringify(parsedQuery, null, 2));
    
    // Apply query transformations
    const { data: queryResults, descriptions } = applyQueryTransformations(
      data,
      summary,
      parsedQuery
    );
    
    console.log(`✅ Query executed: ${data.length} → ${queryResults.length} rows`);
    console.log(`📝 Transformations: ${descriptions.join('; ')}`);
    
    // Format results
    const { summary: resultSummary, formattedResults } = formatQueryResults(
      queryResults,
      parsedQuery,
      summary
    );
    
    return {
      isAnalytical: true,
      queryResults: {
        data: queryResults,
        summary: resultSummary,
        formattedResults,
      },
      parsedQuery,
    };
  } catch (error) {
    console.error('❌ Error executing analytical query:', error);
    return {
      isAnalytical: true,
    };
  }
}

/**
 * Generates a formatted context string to inject into AI prompt
 * This provides the AI with actual query results before generating the answer
 */
export function generateQueryContextForAI(
  queryResult: AnalyticalQueryResult
): string {
  if (!queryResult.isAnalytical || !queryResult.queryResults) {
    return '';
  }

  const { summary, formattedResults, data } = queryResult.queryResults;
  
  // Build context string
  let context = `\n\n🚨🚨🚨 CRITICAL: QUERY RESULTS FOR ANALYTICAL QUESTION 🚨🚨🚨\n`;
  context += `The user asked an analytical question that requires querying the CSV data.\n`;
  context += `I have executed the query and here are the ACTUAL RESULTS:\n\n`;
  context += `QUERY SUMMARY:\n${summary}\n\n`;
  context += `QUERY RESULTS (use these EXACT values in your answer):\n${formattedResults}\n\n`;
  context += `🚨 ABSOLUTE REQUIREMENTS - READ CAREFULLY:\n`;
  context += `1. DO NOT generate a chart - this is an analytical question that needs a direct answer\n`;
  context += `2. DO NOT say "No valid data points found" - the query has been executed above\n`;
  context += `3. DO NOT ask for clarification - use the query results provided above\n`;
  context += `4. DO NOT describe "how you would calculate it" - use the calculated results from above\n`;
  context += `5. ANSWER DIRECTLY with the actual values from the query results\n`;
  context += `6. If the results show regions/categories with totals, list them clearly\n`;
  context += `7. If results are empty, say "No regions/categories meet the criteria"\n`;
  context += `8. Format your answer as a clear, direct response with actual data values\n\n`;
  context += `EXAMPLE FORMAT:\n`;
  context += `- If query results show regions with totals: "The regions that generated more than ₹2 crore are: Region A (₹2.5 crore), Region B (₹3.1 crore)"\n`;
  context += `- If query results are empty: "No regions meet the specified criteria"\n`;
  context += `- DO NOT say: "I'll create a chart" or "Let me analyze the data" - use the results above\n\n`;
  
  // Add specific guidance based on query type
  if (queryResult.parsedQuery) {
    const pq = queryResult.parsedQuery;
    
    if (pq.aggregations && pq.aggregations.length > 0) {
      context += `AGGREGATION DETAILS:\n`;
      pq.aggregations.forEach(agg => {
        const alias = agg.alias || `${agg.column}_${agg.operation}`;
        context += `- ${agg.operation}(${agg.column}) = ${alias} column in results\n`;
      });
      context += `\n`;
    }
    
    if (pq.groupBy && pq.groupBy.length > 0) {
      context += `GROUPING:\n`;
      context += `- Data is grouped by: ${pq.groupBy.join(', ')}\n`;
      context += `- Each row represents one group\n\n`;
    }
    
    if (pq.timeFilters && pq.timeFilters.length > 0) {
      context += `TIME FILTERS APPLIED:\n`;
      pq.timeFilters.forEach(tf => {
        if (tf.type === 'year' && tf.years) {
          context += `- Filtered to year(s): ${tf.years.join(', ')}\n`;
        } else if (tf.type === 'month' && tf.months) {
          context += `- Filtered to month(s): ${tf.months.join(', ')}\n`;
        } else if (tf.type === 'dateRange' && tf.startDate && tf.endDate) {
          context += `- Filtered to date range: ${tf.startDate} to ${tf.endDate}\n`;
        }
      });
      context += `\n`;
    }
  }
  
  context += `END OF QUERY RESULTS\n`;
  
  return context;
}
