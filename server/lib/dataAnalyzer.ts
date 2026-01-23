import { ChartSpec, Insight, DataSummary, Message } from '../shared/schema.js';
import { openai, MODEL } from './openai.js';
import { processChartData } from './chartGenerator.js';
import { optimizeChartData } from './chartDownsampling.js';
import { analyzeCorrelations } from './correlationAnalyzer.js';
import { generateChartInsights } from './insightGenerator.js';
import { parseUserQuery } from './queryParser.js';
import { applyQueryTransformations } from './dataTransform.js';
import type { ParsedQuery } from '../shared/queryTypes.js';
import { executeAnalyticalQuery, generateQueryContextForAI } from './analyticalQueryExecutor.js';
import { 
  isAnalyticalQuery,
  isInformationSeekingQuery,
  identifyRelevantColumnsAndFilterData,
  generateExecutionPlan, 
  executePlan, 
  generateExplanation,
  generateQueryPlanOnly
} from './analyticalQueryEngine.js';
import { calculateSmartDomainsForChart } from './axisScaling.js';

export async function analyzeUpload(
  data: Record<string, any>[],
  summary: DataSummary,
  fileName?: string,
  skipChartInsights: boolean = false
): Promise<{ charts: ChartSpec[]; insights: Insight[] }> {
  // Use AI generation for all file types (Excel and CSV)
  console.log('📊 Using AI chart generation for all file types');

  // OPTIMIZATION: For large files, use faster model to speed up AI calls
  // gpt-4o-mini is 2-3x faster and much cheaper while maintaining good quality
  const useFastModel = data.length > 100000;
  if (useFastModel) {
    console.log(`⚡ Performance optimization: Using faster AI model (gpt-4o-mini) for large file analysis`);
  }

  // OPTIMIZATION: Generate chart specs and insights in parallel (saves ~60-120 seconds)
  const [chartSpecs, insights] = await Promise.all([
    generateChartSpecs(summary, useFastModel),
    generateInsights(data, summary, useFastModel)
  ]);

  // OPTIMIZATION: Skip chart insights generation during upload for faster processing
  // Chart insights can be generated lazily when charts are viewed
  if (skipChartInsights) {
    console.log('⚡ Performance mode: Skipping chart insights generation during upload (will be generated on-demand)');
  }

  // Process data for each chart
  const charts = await Promise.all(chartSpecs.map(async (spec) => {
    let processedData = processChartData(data, spec);
    
    // Apply optimization to ensure max points limit (server-side downsampling)
    processedData = optimizeChartData(processedData, spec);
    
    // Calculate smart axis domains based on statistical measures
    const smartDomains = calculateSmartDomainsForChart(
      processedData,
      spec.x,
      spec.y,
      spec.y2 || undefined,
      {
        yOptions: { useIQR: true, paddingPercent: 5, includeOutliers: true },
        y2Options: spec.y2 ? { useIQR: true, paddingPercent: 5, includeOutliers: true } : undefined,
      }
    );
    
    // OPTIMIZATION: Skip chart insights generation during upload for large files
    // This saves significant time (20-30% faster) as each chart insight requires an AI call
    // Insights will be generated lazily when charts are viewed
    let keyInsight: string | undefined;
    if (!skipChartInsights) {
      const chartInsights = await generateChartInsights(spec, processedData, summary);
      keyInsight = chartInsights.keyInsight;
    }
    
    return {
      ...spec,
      xLabel: spec.x,
      yLabel: spec.y,
      data: processedData, // Already optimized/downsampled
      ...smartDomains, // Add smart domains
      keyInsight: keyInsight, // Will be undefined if skipped, generated on-demand later
    };
  }));

  return { charts, insights };
}

// Helper to clean numeric values (strip %, commas, etc.)
function toNumber(value: any): number {
  if (value === null || value === undefined || value === '') return NaN;
  const cleaned = String(value).replace(/[%,]/g, '').trim();
  return Number(cleaned);
}

// Helper function to find matching column name (case-insensitive, handles spaces/underscores, partial matching)
function findMatchingColumn(searchName: string, availableColumns: string[]): string | null {
  if (!searchName) return null;
  
  const normalized = searchName.toLowerCase().replace(/[\s_-]/g, '');
  
  // First try exact match
  for (const col of availableColumns) {
    const colNormalized = col.toLowerCase().replace(/[\s_-]/g, '');
    if (colNormalized === normalized) {
      return col;
    }
  }
  
  // Then try prefix match (search term is prefix of column name) - e.g., "PAEC" matches "PAEC nGRP Adstocked"
  for (const col of availableColumns) {
    const colNormalized = col.toLowerCase().replace(/[\s_-]/g, '');
    if (colNormalized.startsWith(normalized) && normalized.length >= 3) {
      return col;
    }
  }
  
  // Then try partial match (search term contained in column name)
  for (const col of availableColumns) {
    const colNormalized = col.toLowerCase().replace(/[\s_-]/g, '');
    if (colNormalized.includes(normalized)) {
      return col;
    }
  }
  
  // Try word-boundary matching (search term matches as a word in column name)
  const searchWords = searchName.toLowerCase().split(/\s+/).filter(w => w.length > 0);
  for (const col of availableColumns) {
    const colLower = col.toLowerCase();
    let allWordsMatch = true;
    for (const word of searchWords) {
      const wordRegex = new RegExp(`\\b${word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
      if (!wordRegex.test(colLower)) {
        allWordsMatch = false;
        break;
      }
    }
    if (allWordsMatch && searchWords.length > 0) {
      return col;
    }
  }
  
  // Finally try reverse partial match (column name contained in search term)
  for (const col of availableColumns) {
    const colNormalized = col.toLowerCase().replace(/[\s_-]/g, '');
    if (normalized.includes(colNormalized)) {
      return col;
    }
  }
  
  return null;
}

export async function answerQuestion(
  data: Record<string, any>[],
  question: string,
  chatHistory: Message[],
  summary: DataSummary,
  sessionId?: string,
  chatInsights?: Insight[],
  onThinkingStep?: (step: { step: string; status: 'pending' | 'active' | 'completed' | 'error'; timestamp: number; details?: string }) => void,
  mode?: 'analysis' | 'dataOps' | 'modeling',
  permanentContext?: string
): Promise<{ answer: string; charts?: ChartSpec[]; insights?: Insight[]; table?: any; operationResult?: any }> {
  // CRITICAL: This log should ALWAYS appear first
  console.log('🚀 answerQuestion() CALLED with question:', question);
  console.log('📋 SessionId:', sessionId);
  console.log('📊 Data rows:', data?.length);
  
  // PRIORITY CHECK: For information-seeking queries, route directly to query execution
  // This bypasses the agent system to prevent automatic data operations (aggregation, table creation)
  // Only execute queries and return results - don't modify data structure unless explicitly asked
  const { isInformationSeekingQuery } = await import('./analyticalQueryEngine.js');
  if (isInformationSeekingQuery(question) && mode !== 'dataOps') {
    console.log('🔍 Detected information-seeking query - routing directly to query execution (bypassing agent system)');
    try {
      const result = await generateGeneralAnswer(
        data,
        question,
        chatHistory,
        summary,
        sessionId,
        null,
        [],
        chatInsights
      );
      
      if (result && result.answer && result.answer.trim().length > 0) {
        console.log('✅ Information-seeking query executed successfully');
        return {
          answer: result.answer,
          charts: result.charts,
          insights: result.insights,
        };
      }
    } catch (error) {
      console.error('❌ Error executing information-seeking query:', error);
      // Fall through to agent system as backup
    }
  }
  
  // Try new agent system first
  console.log('🔍 Attempting to use new agent system for query:', question);
  try {
    console.log('📦 Importing agent system...');
    
    // Use dynamic import with error handling for module initialization errors
    let agentModule;
    try {
      agentModule = await import('./agents/index.js');
    } catch (importError) {
      console.error('❌ Failed to import agent module:', importError);
      throw importError; // Re-throw to be caught by outer catch
    }
    
    console.log('✅ Agent module imported, exports:', Object.keys(agentModule));
    
    const { getInitializedOrchestrator } = agentModule;
    console.log('📞 Getting initialized orchestrator...');
    
    let orchestrator;
    try {
      orchestrator = getInitializedOrchestrator();
    } catch (initError) {
      console.error('❌ Failed to initialize orchestrator:', initError);
      throw initError;
    }
    
    console.log('✅ Orchestrator obtained');
    
    console.log('🤖 Using new agent system');
    const result = await orchestrator.processQuery(
      question,
      chatHistory,
      data,
      summary,
      sessionId || 'unknown',
      chatInsights,
      onThinkingStep,
      mode,
      permanentContext
    );
    
    console.log('📤 Agent system result:', { 
      hasAnswer: !!result?.answer, 
      answerLength: result?.answer?.length,
      hasCharts: !!result?.charts,
      chartsCount: result?.charts?.length 
    });
    
    // Ensure we have an answer
    if (result && result.answer && result.answer.trim().length > 0) {
      console.log('✅ Agent system returned response');
      return result;
    } else {
      console.warn('⚠️ Agent system returned empty response, falling back');
      console.warn('⚠️ Result:', JSON.stringify(result, null, 2));
      throw new Error('Empty response from agent system');
    }
  } catch (agentError) {
    console.error('❌ Agent system error, falling back to legacy system');
    console.error('Error type:', agentError?.constructor?.name);
    console.error('Error message:', agentError instanceof Error ? agentError.message : String(agentError));
    if (agentError instanceof Error && agentError.stack) {
      console.error('Stack trace (first 500 chars):', agentError.stack.substring(0, 500));
    }
    // Fall through to legacy implementation
  }

  // Parse query for filters, aggregations, and other transformations
  // This MUST happen before any specific handlers so all paths get filtered data
  let parsedQuery: ParsedQuery | null = null;
  let transformationNotes: string[] = [];
  let workingData = data;
  
  try {
    parsedQuery = await parseUserQuery(question, summary, chatHistory);
    console.log('🧠 Parsed query:', parsedQuery);
    
    if (parsedQuery) {
      const { data: transformedData, descriptions } = applyQueryTransformations(data, summary, parsedQuery);
      transformationNotes = descriptions;
      if (transformedData.length > 0 || descriptions.length > 0) {
        workingData = transformedData;
        console.log(`✅ Applied filters: ${descriptions.join('; ')}`);
        console.log(`📊 Data filtered: ${data.length} → ${workingData.length} rows`);
      }
    }
  } catch (error) {
    console.error('⚠️ Query parsing failed, continuing without structured filters:', error);
  }

  const withNotes = <T extends { answer: string }>(result: T): T => {
    // Return result as-is without appending filters applied text
    return result;
  };

  // Legacy implementation (existing code)
  // Utility: parse two-series line intent like "A and B over months" or "A vs B"
  // This should be checked FIRST for "and" queries, before detectVsEarly
  const detectTwoSeriesLine = (q: string) => {
    console.log('🔍 detectTwoSeriesLine - checking query:', q);
    const ql = q.toLowerCase();
    
    // Skip if user explicitly wants a scatter plot
    const wantsScatter = /\b(scatter\s+plot|scatterplot|scatter)\b/i.test(q);
    if (wantsScatter) {
      console.log('❌ detectTwoSeriesLine - user wants scatter plot, skipping line chart detection');
      return null;
    }
    
    // More flexible detection: look for "and", "vs", or "with" with mention of line chart, plot, or "over months/time"
    // Also detect "in one trend line" or "in one chart" as indicators
    const mentionsLine = /\bline\b|\bline\s*chart\b|\bover\s+(?:time|months?|weeks?|days?)\b|\bplot\b|\bgraph\b|\btrends?\b|\bcompare\b|\bin\s+one\s+(?:trend\s+)?(?:line|chart)\b/i.test(ql);
    
    // Also check if it mentions two variables (check for common patterns)
    const hasAnd = /\sand\s+/.test(ql);
    const hasVs = /\s+vs\s+/.test(ql);
    const hasWith = /\s+with\s+/.test(ql);
    const hasCompare = /\bcompare\s+/.test(ql);
    const hasInOne = /\bin\s+one\s+(?:trend\s+)?(?:line|chart|graph)\b/i.test(ql);
    
    // Check for "two separate axes" which indicates dual-axis line chart
    const wantsDualAxis = /\b(two\s+separates?\s+axes?|separates?\s+axes?|dual\s+axis|dual\s+y)\b/i.test(q);
    
    console.log('🔍 detectTwoSeriesLine - flags:', { mentionsLine, hasAnd, hasVs, hasWith, hasCompare, hasInOne, wantsDualAxis });
    
    // If it mentions "and", "vs", "with", "compare", or "in one trend line" with plot/chart keywords, OR if it explicitly wants dual axes, proceed
    // Special case: "A with B" pattern should always be considered for dual-axis chart even without explicit chart keywords
    if (!mentionsLine && !hasAnd && !hasVs && !hasWith && !hasCompare && !hasInOne && !wantsDualAxis) {
      console.log('❌ detectTwoSeriesLine - does not match criteria');
      return null;
    }
    
    // Special handling: "A with B" pattern is a strong indicator of dual-axis request
    if (hasWith && !mentionsLine && !hasInOne) {
      console.log('✅ detectTwoSeriesLine - "with" pattern detected, treating as dual-axis request');
    }
    
    // split on ' vs ', ' and ', ' with ', or 'compare ... with'
    let parts: string[] = [];
    if (ql.includes(' vs ')) {
      parts = q.split(/\s+vs\s+/i);
    } else if (ql.includes(' and ')) {
      parts = q.split(/\s+and\s+/i);
    } else if (hasCompare && hasWith) {
      // Handle "compare A with B" pattern
      const compareMatch = q.match(/compare\s+(.+?)\s+with\s+(.+)/i);
      if (compareMatch && compareMatch.length >= 3) {
        parts = [compareMatch[1].trim(), compareMatch[2].trim()];
      }
    } else if (hasWith) {
      // Handle "A with B" pattern (for trend comparisons)
      parts = q.split(/\s+with\s+/i);
    }
    
    if (parts.length < 2) return null;
    
    // Clean up parts: remove chart-related words and "over months" phrases
    // Also handle "on two separate axes" (including typo "separates")
    // Handle "in one trend line" or "in one chart" patterns
    const candidates = parts.map(p => 
      p.replace(/over\s+(?:time|months?|weeks?|days?|.*)/i, '')
       .replace(/\b(line\s*chart|plot|graph|show|display|create)\b/gi, '')
       .replace(/\bon\s+(?:two\s+)?(?:separates?\s+)?axes?\b/gi, '')  // Fixed: added 's?' to handle "separates"
       .replace(/\s+axes?\s*$/i, '')  // Remove trailing "axes"
       .replace(/\bin\s+one\s+(?:trend\s+)?(?:line|chart|graph)\b/gi, '')  // Remove "in one trend line" or "in one chart"
       .replace(/\bin\s+one\s+/gi, '')  // Remove "in one" as fallback
       .trim()
    ).filter(Boolean);
    
    if (candidates.length < 2) return null;
    
    const allCols = summary.columns.map(c => c.name);
    const a = findMatchingColumn(candidates[0], allCols);
    const b = findMatchingColumn(candidates[1], allCols);
    
    console.log('🔍 detectTwoSeriesLine - candidates:', candidates);
    console.log('🔍 detectTwoSeriesLine - matched columns:', { a, b });
    
    if (!a || !b) {
      console.log('❌ detectTwoSeriesLine - could not match columns');
      return null;
    }
    
    const aNum = summary.numericColumns.includes(a);
    const bNum = summary.numericColumns.includes(b);
    
    if (!aNum || !bNum) {
      console.log('❌ detectTwoSeriesLine - columns not both numeric');
      return null;
    }
    
    // Choose x as first date column or a column named Month/Date/Week if present
    const x = summary.dateColumns[0] || 
              findMatchingColumn('Month', allCols) || 
              findMatchingColumn('Date', allCols) ||
              findMatchingColumn('Week', allCols) ||
              summary.columns[0].name;
    
    console.log('✅ detectTwoSeriesLine - detected:', { x, y: a, y2: b });
    return { x, y: a, y2: b };
  };

  // Detect "against" queries like "plot A against B"
  // Convention: second variable (after "against") -> X-axis, first variable -> Y-axis
  const detectAgainstQuery = (q: string): { yVar: string | null; xVar: string | null } | null => {
    const ql = q.toLowerCase();
    if (!/\bagainst\b/i.test(ql)) return null;

    const match = q.match(/(.+?)\s+against\s+(.+)/i);
    if (!match) return null;

    let yRaw = match[1].trim();
    let xRaw = match[2].trim();

    // Clean leading/trailing chart words
    yRaw = yRaw.replace(/^(?:can\s+you\s+)?(?:please\s+)?(?:plot|graph|chart|show|display)\s+/i, '').trim();
    xRaw = xRaw.replace(/\s+(?:on|with|using|separate|axes|axis|chart|graph|plot).*$/i, '').trim();

    const allCols = summary.columns.map(c => c.name);
    const yVar = findMatchingColumn(yRaw, allCols);
    const xVar = findMatchingColumn(xRaw, allCols);
    if (!yVar || !xVar) return null;
    if (!summary.numericColumns.includes(yVar) || !summary.numericColumns.includes(xVar)) return null;
    return { yVar, xVar };
  };

  // Detect "vs" queries early - when user asks to plot X vs Y (especially with "two separate axes")
  const detectVsEarly = (q: string): { var1: string | null; var2: string | null } | null => {
    console.log('🔍 Early vs detection for:', q);
    const ql = q.toLowerCase();
    // Look for "vs" in the question, especially with "plot", "two separate axes", etc.
    if (!ql.includes(' vs ')) {
      console.log('❌ No "vs" found in question');
      return null;
    }
    
    const allCols = summary.columns.map(c => c.name);
    
    // Match pattern: "plot X vs Y" or "X vs Y on two separate axes" etc.
    const vsMatch = q.match(/(.+?)\s+vs\s+(.+)/i);
    if (!vsMatch) {
      console.log('❌ No vs match pattern found');
      return null;
    }
    
    let var1Raw = vsMatch[1].trim();
    let var2Raw = vsMatch[2].trim();
    
    console.log('📝 Raw extracted:', { var1Raw, var2Raw });
    
    // Clean up variable names
    var1Raw = var1Raw.replace(/^(?:can\s+you\s+)?(?:plot|graph|chart|show|display)\s+/i, '').trim();
    var2Raw = var2Raw.replace(/\s+(?:on|with|using|separate|axes|axis|chart|graph|plot).*$/i, '').trim();
    
    console.log('🧹 Cleaned variables:', { var1Raw, var2Raw });
    console.log('📊 Available columns:', allCols);
    console.log('🔢 Numeric columns:', summary.numericColumns);
    
    const var1 = findMatchingColumn(var1Raw, allCols);
    const var2 = findMatchingColumn(var2Raw, allCols);
    
    console.log('🎯 Column matches:', { var1, var2 });
    
    if (!var1 || !var2) {
      console.log('❌ Could not match columns. var1:', var1, 'var2:', var2);
      return null;
    }
    
    // Check if both are numeric
    const bothNumeric = summary.numericColumns.includes(var1) && summary.numericColumns.includes(var2);
    if (!bothNumeric) {
      console.log('❌ Not both numeric. var1 numeric:', summary.numericColumns.includes(var1), 'var2 numeric:', summary.numericColumns.includes(var2));
      return null;
    }
    
    console.log('✅ Valid vs query detected early:', { var1, var2 });
    return { var1, var2 };
  };

  // Generalized scatter plot detection - handles ANY variation of scatter plot requests
  // Examples: "scatter chart between X and Y", "scatter plot of X and Y", "plot X and Y as scatter", etc.
  const detectScatterPlotQuery = (q: string): { var1: string | null; var2: string | null } | null => {
    console.log('🔍 detectScatterPlotQuery - checking query:', q);
    const ql = q.toLowerCase();
    
    // Check for ANY scatter-related keywords - very permissive
    const scatterPatterns = [
      /\bscatter\s+chart\b/i,
      /\bscatter\s+plot\b/i,
      /\bscatterplot\b/i,
      /\bscatter\b/i
    ];
    
    const hasScatterKeyword = scatterPatterns.some(pattern => pattern.test(q));
    if (!hasScatterKeyword) {
      console.log('❌ No scatter plot keyword found');
      return null;
    }
    
    console.log('✅ Scatter keyword detected, extracting variables...');
    const allCols = summary.columns.map(c => c.name);
    console.log('📊 Available columns:', allCols);
    
    // Generalized extraction: Remove scatter keywords and extract variables
    // Step 1: Remove scatter keywords and surrounding phrases
    let cleanedQuery = q;
    // Remove phrases like "scatter chart", "scatter plot", "scatterplot", "scatter"
    cleanedQuery = cleanedQuery.replace(/\b(?:scatter\s+chart|scatter\s+plot|scatterplot|scatter)\b/gi, '');
    // Remove common intro phrases
    cleanedQuery = cleanedQuery.replace(/^(?:can\s+you\s+)?(?:please\s+)?(?:plot|graph|chart|show|display|create|draw|generate)\s+/i, '');
    // Remove common connector phrases
    cleanedQuery = cleanedQuery.replace(/\s+(?:between|of|for|with|using|as)\s+/gi, ' ');
    cleanedQuery = cleanedQuery.replace(/\s+(?:in\s+a\s+)?(?:scatter|plot|chart|graph).*$/i, '');
    cleanedQuery = cleanedQuery.trim();
    
    console.log('🧹 Cleaned query:', cleanedQuery);
    
    // Step 2: Extract variables using multiple strategies
    let parts: string[] = [];
    
    // Strategy 1: Split by "and" (most common)
    if (cleanedQuery.includes(' and ')) {
      parts = cleanedQuery.split(/\s+and\s+/i).map(p => p.trim()).filter(p => p.length > 0);
      console.log('📝 Strategy 1 (and):', parts);
    }
    // Strategy 2: Split by "vs"
    else if (cleanedQuery.includes(' vs ')) {
      parts = cleanedQuery.split(/\s+vs\s+/i).map(p => p.trim()).filter(p => p.length > 0);
      console.log('📝 Strategy 2 (vs):', parts);
    }
    // Strategy 3: Split by comma
    else if (cleanedQuery.includes(',')) {
      parts = cleanedQuery.split(',').map(p => p.trim()).filter(p => p.length > 0);
      console.log('📝 Strategy 3 (comma):', parts);
    }
    // Strategy 4: Try to find two column names by matching against available columns
    else {
      // Split by spaces and try different combinations
      const tokens = cleanedQuery.split(/\s+/).filter(t => t.trim().length > 0);
      console.log('📝 Strategy 4 (tokens):', tokens);
      
      if (tokens.length >= 2) {
        // Try different splits to find column matches
        for (let i = 1; i < tokens.length; i++) {
          const part1 = tokens.slice(0, i).join(' ');
          const part2 = tokens.slice(i).join(' ');
          
          // Try to match against columns
          const match1 = findMatchingColumn(part1, allCols);
          const match2 = findMatchingColumn(part2, allCols);
          
          if (match1 && match2) {
            parts = [part1, part2];
            console.log('📝 Strategy 4 found match:', parts);
            break;
          }
        }
        
        // If no match found, try matching individual tokens
        if (parts.length < 2 && tokens.length >= 2) {
          // Try first token and rest
          const firstToken = tokens[0];
          const restTokens = tokens.slice(1).join(' ');
          const match1 = findMatchingColumn(firstToken, allCols);
          const match2 = findMatchingColumn(restTokens, allCols);
          
          if (match1 && match2) {
            parts = [firstToken, restTokens];
            console.log('📝 Strategy 4 fallback match:', parts);
          }
        }
      }
    }
    
    if (parts.length < 2) {
      console.log('❌ Could not extract two variables from query');
      console.log('   Cleaned query:', cleanedQuery);
      return null;
    }
    
    // Clean up variable names further
    const candidates = parts.slice(0, 2).map(p => 
      p.replace(/^(?:the\s+)?(?:column\s+)?/i, '')
       .replace(/\s+(?:column|variable|field|data).*$/i, '')
       .replace(/[,\s]+$/g, '')
       .trim()
    ).filter(p => p.length > 0);
    
    if (candidates.length < 2) {
      console.log('❌ Could not clean variables properly. Candidates:', candidates);
      return null;
    }
    
    console.log('📝 Final candidates:', candidates);
    
    // Try to match columns - use improved findMatchingColumn
    let var1 = findMatchingColumn(candidates[0], allCols);
    let var2 = findMatchingColumn(candidates[1], allCols);
    
    // If exact match fails, try more aggressive matching
    if (!var1) {
      console.log('⚠️ First variable not matched, trying aggressive matching for:', candidates[0]);
      // Try prefix matching
      for (const col of allCols) {
        const colLower = col.toLowerCase().replace(/[\s_-]/g, '');
        const candLower = candidates[0].toLowerCase().replace(/[\s_-]/g, '');
        if (colLower.startsWith(candLower) && candLower.length >= 3) {
          var1 = col;
          console.log('✅ Prefix match found:', col);
          break;
        }
      }
    }
    
    if (!var2) {
      console.log('⚠️ Second variable not matched, trying aggressive matching for:', candidates[1]);
      for (const col of allCols) {
        const colLower = col.toLowerCase().replace(/[\s_-]/g, '');
        const candLower = candidates[1].toLowerCase().replace(/[\s_-]/g, '');
        if (colLower.startsWith(candLower) && candLower.length >= 3) {
          var2 = col;
          console.log('✅ Prefix match found:', col);
          break;
        }
      }
    }
    
    console.log('🎯 Column matches (scatter):', { 
      var1, 
      var2, 
      search1: candidates[0], 
      search2: candidates[1] 
    });
    
    if (!var1 || !var2) {
      console.log('❌ Could not match columns for scatter plot');
      console.log('   Available columns:', allCols);
      console.log('   Searched for:', candidates);
      return null;
    }
    
    // Check if both are numeric (required for scatter plots)
    const bothNumeric = summary.numericColumns.includes(var1) && summary.numericColumns.includes(var2);
    if (!bothNumeric) {
      console.log('❌ Not both numeric for scatter plot. var1 numeric:', summary.numericColumns.includes(var1), 'var2 numeric:', summary.numericColumns.includes(var2));
      return null;
    }
    
    console.log('✅ Valid scatter plot query detected:', { var1, var2 });
    return { var1, var2 };
  };

  // Detect "correlation between X and Y" queries - should generate scatter plot directly
  const detectCorrelationBetween = (q: string): { var1: string | null; var2: string | null } | null => {
    console.log('🔍 detectCorrelationBetween - checking query:', q);
    const ql = q.toLowerCase();
    
    // Look for "correlation between" or "correlation of" patterns
    const correlationPatterns = [
      /\bcorrelation\s+between\s+(.+?)\s+and\s+(.+)/i,
      /\bcorrelation\s+of\s+(.+?)\s+and\s+(.+)/i,
      /\bcorrelation\s+between\s+(.+?)\s+with\s+(.+)/i,
    ];
    
    for (const pattern of correlationPatterns) {
      const match = q.match(pattern);
      if (match && match.length >= 3) {
        const allCols = summary.columns.map(c => c.name);
        let var1Raw = match[1].trim();
        let var2Raw = match[2].trim();
        
        // Clean up variable names
        var1Raw = var1Raw.replace(/^(?:the\s+)?/i, '').trim();
        var2Raw = var2Raw.replace(/\s+(?:and|with|versus|vs).*$/i, '').trim();
        
        console.log('📝 Raw extracted (correlation between):', { var1Raw, var2Raw });
        
        const var1 = findMatchingColumn(var1Raw, allCols);
        const var2 = findMatchingColumn(var2Raw, allCols);
        
        console.log('🎯 Column matches (correlation between):', { var1, var2 });
        
        if (var1 && var2 && summary.numericColumns.includes(var1) && summary.numericColumns.includes(var2)) {
          console.log('✅ Valid correlation between query detected:', { var1, var2 });
          return { var1, var2 };
        }
      }
    }
    
    console.log('❌ No correlation between pattern found');
    return null;
  };

  // Check for "correlation between X and Y" FIRST (before scatter plot detection)
  console.log('🔍 Starting detection for question:', question);
  const correlationBetween = detectCorrelationBetween(question);
  if (correlationBetween && correlationBetween.var1 && correlationBetween.var2) {
    console.log('✅ Correlation between query detected:', correlationBetween);
    
    // Verify columns exist in data
    const firstRow = data[0];
    if (!firstRow) {
      console.error('❌ No data rows available');
      return { answer: 'No data available to create charts. Please upload a data file first.' };
    }
    
    const actualColumns = Object.keys(firstRow || {}).map(col => col.trim());
    const resolvedVar1 = findMatchingColumn(correlationBetween.var1, actualColumns);
    const resolvedVar2 = findMatchingColumn(correlationBetween.var2, actualColumns);
    
    if (!resolvedVar1) {
      console.error(`❌ Column "${correlationBetween.var1}" not found in data (after flexible matching)`);
      const allCols = summary.columns.map(c => c.name);
      return { answer: `Column "${correlationBetween.var1}" not found in the data. Available columns: ${allCols.join(', ')}` };
    }
    
    if (!resolvedVar2) {
      console.error(`❌ Column "${correlationBetween.var2}" not found in data (after flexible matching)`);
      const allCols = summary.columns.map(c => c.name);
      return { answer: `Column "${correlationBetween.var2}" not found in the data. Available columns: ${allCols.join(', ')}` };
    }
    
    // Ensure resolved columns are numeric (use flexible matching against summary numeric columns)
    const resolvedNumeric1 = findMatchingColumn(resolvedVar1, summary.numericColumns);
    const resolvedNumeric2 = findMatchingColumn(resolvedVar2, summary.numericColumns);
    
    if (!resolvedNumeric1 || !resolvedNumeric2) {
      console.error('❌ Resolved columns are not numeric:', { resolvedVar1, resolvedVar2 });
      return { answer: `Both "${resolvedVar1}" and "${resolvedVar2}" must be numeric for correlation analysis.` };
    }
    
    if (resolvedVar1 !== correlationBetween.var1 || resolvedVar2 !== correlationBetween.var2) {
      console.log('🔄 Resolved correlation columns (after flexible matching):', { resolvedVar1, resolvedVar2 });
    }
    
    // Create scatter plot directly
    const scatterSpec: ChartSpec = {
      type: 'scatter',
      title: `Correlation: ${resolvedVar1} vs ${resolvedVar2}`,
      x: resolvedVar1,
      y: resolvedVar2,
      xLabel: resolvedVar1,
      yLabel: resolvedVar2,
      aggregate: 'none',
    };
    
    console.log('🔄 Processing correlation scatter plot data...');
    const scatterData = processChartData(workingData, scatterSpec);
    console.log(`✅ Scatter data: ${scatterData.length} points`);
    
    if (scatterData.length === 0) {
      const allCols = summary.columns.map(c => c.name);
      return { 
        answer: `No valid data points found for scatter plot. Please check that columns "${resolvedVar1}" and "${resolvedVar2}" exist and contain numeric data. Available columns: ${allCols.join(', ')}` 
      };
    }
    
    const scatterInsights = await generateChartInsights(scatterSpec, scatterData, summary, chatInsights);
    
    return withNotes({ 
      answer: `Created a scatter plot showing the correlation between ${resolvedVar1} and ${resolvedVar2}: X = ${resolvedVar1}, Y = ${resolvedVar2}.`,
      charts: [{ ...scatterSpec, data: scatterData, keyInsight: scatterInsights.keyInsight }]
    });
  }

  // Check for explicit scatter plot requests (after correlation between detection)
  const scatterPlot = detectScatterPlotQuery(question);
  if (scatterPlot && scatterPlot.var1 && scatterPlot.var2) {
    console.log('✅ Explicit scatter plot request detected:', scatterPlot);
    
    // Verify columns exist in data
    const firstRow = data[0];
    if (!firstRow) {
      console.error('❌ No data rows available');
      return { answer: 'No data available to create charts. Please upload a data file first.' };
    }
    const actualColumns = Object.keys(firstRow || {}).map(col => col.trim());
    const resolvedVar1 = findMatchingColumn(scatterPlot.var1, actualColumns);
    const resolvedVar2 = findMatchingColumn(scatterPlot.var2, actualColumns);
    
    if (!resolvedVar1) {
      console.error(`❌ Column "${scatterPlot.var1}" not found in data (after flexible matching)`);
      const allCols = summary.columns.map(c => c.name);
      return { answer: `Column "${scatterPlot.var1}" not found in the data. Available columns: ${allCols.join(', ')}` };
    }
    
    if (!resolvedVar2) {
      console.error(`❌ Column "${scatterPlot.var2}" not found in data (after flexible matching)`);
      const allCols = summary.columns.map(c => c.name);
      return { answer: `Column "${scatterPlot.var2}" not found in the data. Available columns: ${allCols.join(', ')}` };
    }
    
    if (resolvedVar1 !== scatterPlot.var1 || resolvedVar2 !== scatterPlot.var2) {
      console.log('🔄 Resolved scatter plot columns (after flexible matching):', { resolvedVar1, resolvedVar2 });
    }
    
    // Create scatter plot - use flexible title format
    const scatterSpec: ChartSpec = {
      type: 'scatter',
      title: `Scatter Chart: ${resolvedVar1} vs ${resolvedVar2}`,
      x: resolvedVar1,
      y: resolvedVar2,
      xLabel: resolvedVar1,
      yLabel: resolvedVar2,
      aggregate: 'none',
    };
    
    console.log('🔄 Processing scatter plot data...');
    const scatterData = processChartData(workingData, scatterSpec);
    console.log(`✅ Scatter data: ${scatterData.length} points`);
    
    if (scatterData.length === 0) {
      const allCols = summary.columns.map(c => c.name);
      return { 
        answer: `No valid data points found for scatter plot. Please check that columns "${resolvedVar1}" and "${resolvedVar2}" exist and contain numeric data. Available columns: ${allCols.join(', ')}` 
      };
    }
    
    const scatterInsights = await generateChartInsights(scatterSpec, scatterData, summary);
    
    return withNotes({ 
      answer: `Created a scatter plot: X = ${resolvedVar1}, Y = ${resolvedVar2}.`,
      charts: [{ ...scatterSpec, data: scatterData, keyInsight: scatterInsights.keyInsight }]
    });
  }

  // Check for two-series line chart (handles "and" queries) - AFTER scatter plot check
  const twoSeries = detectTwoSeriesLine(question);
  if (twoSeries) {
    console.log('✅ detectTwoSeriesLine matched! Result:', twoSeries);
    
    // Verify columns exist in data
    const firstRow = data[0];
    if (!firstRow) {
      console.error('❌ No data rows available');
      return { answer: 'No data available to create charts. Please upload a data file first.' };
    }
    
    if (!firstRow.hasOwnProperty(twoSeries.y)) {
      console.error(`❌ Column "${twoSeries.y}" not found in data`);
      const allCols = summary.columns.map(c => c.name);
      return { answer: `Column "${twoSeries.y}" not found in the data. Available columns: ${allCols.join(', ')}` };
    }
    
    if (!firstRow.hasOwnProperty(twoSeries.y2)) {
      console.error(`❌ Column "${twoSeries.y2}" not found in data`);
      const allCols = summary.columns.map(c => c.name);
      return { answer: `Column "${twoSeries.y2}" not found in the data. Available columns: ${allCols.join(', ')}` };
    }
    
    // Check for explicit "two separate axes" request
    const wantsDualAxis = /\b(two\s+separates?\s+axes?|separates?\s+axes?|dual\s+axis|dual\s+y)\b/i.test(question);
    
    // For "A and B over months" or "A and B on two separate axes", always create dual-axis line chart
    const spec: ChartSpec = {
      type: 'line',
      title: `${twoSeries.y} and ${twoSeries.y2} over ${twoSeries.x}`,
      x: twoSeries.x,
      y: twoSeries.y,
      y2: twoSeries.y2,
      xLabel: twoSeries.x,
      yLabel: twoSeries.y,
      y2Label: twoSeries.y2,
      aggregate: 'none',
    } as any;
    
    console.log('🔄 Processing dual-axis line chart data...');
    const processed = processChartData(workingData, spec);
    console.log(`✅ Dual-axis line data: ${processed.length} points`);
    
    if (processed.length === 0) {
      const allCols = summary.columns.map(c => c.name);
      return { 
        answer: `No valid data points found. Please check that columns "${twoSeries.y}" and "${twoSeries.y2}" exist and contain numeric data. Available columns: ${allCols.join(', ')}` 
      };
    }
    
    const insights = await generateChartInsights(spec, processed, summary, chatInsights);
    const chart: ChartSpec = { 
      ...spec, 
      data: processed, 
      keyInsight: insights.keyInsight
    };
    
    const answer = wantsDualAxis 
      ? `I've created a line chart with ${twoSeries.y} on the left axis and ${twoSeries.y2} on the right axis, plotted over ${twoSeries.x}.`
      : `Plotted two lines over ${twoSeries.x} with ${twoSeries.y} on the left axis and ${twoSeries.y2} on the right axis.`;
    
    return withNotes({ answer, charts: [chart] });
  }

  // Handle "against" queries next (scatter by default; line if time-series context)
  const against = detectAgainstQuery(question);
  if (against && against.xVar && against.yVar) {
    const firstRow = data[0];
    if (!firstRow) {
      return { answer: 'No data available to create charts. Please upload a data file first.' };
    }
    if (!firstRow.hasOwnProperty(against.xVar) || !firstRow.hasOwnProperty(against.yVar)) {
      const allCols = summary.columns.map(c => c.name);
      return { answer: `Columns not found. Available columns: ${allCols.join(', ')}` };
    }

    // Decide time-series vs scatter
    const mentionsTime = /\b(time|trend|over\s+(?:time|months?|weeks?|days?))\b/i.test(question);
    const hasDate = summary.dateColumns && summary.dateColumns.length > 0;
    if (mentionsTime && hasDate) {
      const xTime = summary.dateColumns[0] || findMatchingColumn('Month', summary.columns.map(c => c.name)) || summary.columns[0].name;
      const spec: ChartSpec = {
        type: 'line',
        title: `${against.yVar} against ${against.xVar} over ${xTime}`,
        x: xTime,
        y: against.yVar,
        y2: against.xVar,
        xLabel: xTime,
        yLabel: against.yVar,
        y2Label: against.xVar,
        aggregate: 'none',
      } as any;
      const dataProcessed = processChartData(workingData, spec);
      if (dataProcessed.length === 0) {
        return { answer: `No valid data points found for line chart using ${xTime}.` };
      }
      const insights = await generateChartInsights(spec, dataProcessed, summary, chatInsights);
      return withNotes({ 
        answer: `Created a dual-axis line chart: X = ${xTime}, left Y = ${against.yVar}, right Y = ${against.xVar}.`,
        charts: [{ ...spec, data: dataProcessed, keyInsight: insights.keyInsight }]
      });
    }

    // Scatter plot default
    const scatter: ChartSpec = {
      type: 'scatter',
      title: `Scatter: ${against.yVar} vs ${against.xVar}`,
      x: against.xVar,
      y: against.yVar,
      xLabel: against.xVar,
      yLabel: against.yVar,
      aggregate: 'none',
    };
    const scatterData = processChartData(workingData, scatter);
    if (scatterData.length === 0) {
      return { answer: `No valid data points found for scatter plot with X=${against.xVar}, Y=${against.yVar}.` };
    }
    const scatterInsights = await generateChartInsights(scatter, scatterData, summary, chatInsights);
    return withNotes({ 
      answer: `Created a scatter plot: X = ${against.xVar}, Y = ${against.yVar}.`,
      charts: [{ ...scatter, data: scatterData, keyInsight: scatterInsights.keyInsight }]
    });
  }

  // Then check for "vs" queries (for scatter plots and comparisons)
  const vsEarly = detectVsEarly(question);
  if (vsEarly && vsEarly.var1 && vsEarly.var2) {
    console.log('🎯 Early vs detection triggered:', vsEarly);
    
    // Check if user wants "two separate axes" (dual-axis line chart) or just a comparison
    // Also handle typo "separates axes"
    const wantsDualAxis = /\b(two\s+separates?\s+axes?|separates?\s+axes?|dual\s+axis|dual\s+y)\b/i.test(question);
    const wantsLineChart = /\b(line\s*chart|plot|graph)\b/i.test(question);
    
    // Verify columns exist in data
    const firstRow = data[0];
    if (!firstRow) {
      console.error('❌ No data rows available');
      return { answer: 'No data available to create charts. Please upload a data file first.' };
    }
    
    if (!firstRow.hasOwnProperty(vsEarly.var1)) {
      console.error(`❌ Column "${vsEarly.var1}" not found in data`);
      const allCols = summary.columns.map(c => c.name);
      return { answer: `Column "${vsEarly.var1}" not found in the data. Available columns: ${allCols.join(', ')}` };
    }
    
    if (!firstRow.hasOwnProperty(vsEarly.var2)) {
      console.error(`❌ Column "${vsEarly.var2}" not found in data`);
      const allCols = summary.columns.map(c => c.name);
      return { answer: `Column "${vsEarly.var2}" not found in the data. Available columns: ${allCols.join(', ')}` };
    }
    
    // Determine X-axis for line charts: prefer date column
    const allCols = summary.columns.map(c => c.name);
    const lineChartX = summary.dateColumns[0] || 
                      findMatchingColumn('Month', allCols) || 
                      findMatchingColumn('Date', allCols) ||
                      findMatchingColumn('Week', allCols) ||
                      allCols[0];
    
    console.log('📈 Line chart X-axis:', lineChartX);
    console.log('📊 Wants dual axis:', wantsDualAxis, 'Wants line chart:', wantsLineChart);
    
    // If user wants dual-axis line chart, create one line chart with y and y2
    if (wantsDualAxis || wantsLineChart) {
      const dualAxisLineSpec: ChartSpec = {
        type: 'line',
        title: `${vsEarly.var1} and ${vsEarly.var2} over ${lineChartX}`,
        x: lineChartX,
        y: vsEarly.var1,
        y2: vsEarly.var2,
        xLabel: lineChartX,
        yLabel: vsEarly.var1,
        y2Label: vsEarly.var2,
        aggregate: 'none',
      };
      
      console.log('🔄 Processing dual-axis line chart data...');
      const dualAxisLineData = processChartData(workingData, dualAxisLineSpec);
      console.log(`✅ Dual-axis line data: ${dualAxisLineData.length} points`);
      
      if (dualAxisLineData.length === 0) {
        return { 
          answer: `No valid data points found. Please check that columns "${vsEarly.var1}" and "${vsEarly.var2}" exist and contain numeric data. Available columns: ${allCols.join(', ')}` 
        };
      }
      
      // Calculate smart axis domains for dual-axis chart
      const smartDomains = calculateSmartDomainsForChart(
        dualAxisLineData,
        dualAxisLineSpec.x,
        dualAxisLineSpec.y,
        dualAxisLineSpec.y2,
        {
          yOptions: { useIQR: true, paddingPercent: 5, includeOutliers: true },
          y2Options: { useIQR: true, paddingPercent: 5, includeOutliers: true },
        }
      );
      
      const dualAxisInsights = await generateChartInsights(dualAxisLineSpec, dualAxisLineData, summary, chatInsights);
      
      const charts: ChartSpec[] = [{
        ...dualAxisLineSpec,
        data: dualAxisLineData,
        ...smartDomains, // Add smart domains
        keyInsight: dualAxisInsights.keyInsight,
      }];
      
      const answer = `I've created a line chart with ${vsEarly.var1} on the left axis and ${vsEarly.var2} on the right axis, plotted over ${lineChartX}.`;
      
      return withNotes({ answer, charts });
    }
    
    // Otherwise, create scatter plot and two separate line charts (original behavior)
    const scatterSpec: ChartSpec = {
      type: 'scatter',
      title: `Scatter Plot of ${vsEarly.var1} vs ${vsEarly.var2}`,
      x: vsEarly.var1,
      y: vsEarly.var2,
      xLabel: vsEarly.var1,
      yLabel: vsEarly.var2,
      aggregate: 'none',
    };
    
    // Create TWO separate line charts (one for each variable)
    const lineSpec1: ChartSpec = {
      type: 'line',
      title: `${vsEarly.var1} over ${lineChartX}`,
      x: lineChartX,
      y: vsEarly.var1,
      xLabel: lineChartX,
      yLabel: vsEarly.var1,
      aggregate: 'none',
    };
    
    const lineSpec2: ChartSpec = {
      type: 'line',
      title: `${vsEarly.var2} over ${lineChartX}`,
      x: lineChartX,
      y: vsEarly.var2,
      xLabel: lineChartX,
      yLabel: vsEarly.var2,
      aggregate: 'none',
    };
    
    // Process all charts
    console.log('🔄 Processing scatter chart data...');
    const scatterData = processChartData(workingData, scatterSpec);
    console.log(`✅ Scatter data: ${scatterData.length} points`);
    
    console.log('🔄 Processing line chart 1 data...');
    const lineData1 = processChartData(workingData, lineSpec1);
    console.log(`✅ Line chart 1 data: ${lineData1.length} points`);
    
    console.log('🔄 Processing line chart 2 data...');
    const lineData2 = processChartData(workingData, lineSpec2);
    console.log(`✅ Line chart 2 data: ${lineData2.length} points`);
    
    if (scatterData.length === 0 && lineData1.length === 0 && lineData2.length === 0) {
      return { 
        answer: `No valid data points found. Please check that columns "${vsEarly.var1}" and "${vsEarly.var2}" exist and contain numeric data. Available columns: ${allCols.join(', ')}` 
      };
    }
    
    const scatterInsights = await generateChartInsights(scatterSpec, scatterData, summary, chatInsights);
    const lineInsights1 = await generateChartInsights(lineSpec1, lineData1, summary, chatInsights);
    const lineInsights2 = await generateChartInsights(lineSpec2, lineData2, summary, chatInsights);
    
    const charts: ChartSpec[] = [];
    
    if (scatterData.length > 0) {
      charts.push({
        ...scatterSpec,
        data: scatterData,
        keyInsight: scatterInsights.keyInsight,
      });
    }
    
    if (lineData1.length > 0) {
      charts.push({
        ...lineSpec1,
        data: lineData1,
        keyInsight: lineInsights1.keyInsight,
      });
    }
    
    if (lineData2.length > 0) {
      charts.push({
        ...lineSpec2,
        data: lineData2,
        keyInsight: lineInsights2.keyInsight,
      });
    }
    
    const answer = `I've created a scatter plot comparing ${vsEarly.var1} and ${vsEarly.var2}, plus two separate line charts showing each variable over ${lineChartX}.`;
    
    return { answer, charts };
  }
  // Helper: detect explicit axis assignment like "foo (x-axis)" or "bar (y axis)"
  const parseExplicitAxes = (q: string): { x?: string; y?: string } => {
    const result: { x?: string; y?: string } = {};
    // capture phrases followed by (x-axis|x axis|xaxis) or (y-axis|y axis|yaxis)
    const axisRegex = /(.*?)\(([^\)]*)\)/g; // greedy left text + parentheses
    const lower = q.toLowerCase();
    let m: RegExpExecArray | null;
    while ((m = axisRegex.exec(q)) !== null) {
      const rawName = m[1].trim();
      const axisText = m[2].toLowerCase().replace(/\s+/g, '');
      if (!rawName) continue;
      if (axisText.includes('x-axis') || axisText.includes('xaxis') || axisText === 'x') {
        result.x = rawName;
      } else if (axisText.includes('y-axis') || axisText.includes('yaxis') || axisText === 'y') {
        result.y = rawName;
      }
    }
    // Also support formats like "x axis: foo" or "y axis is bar"
    const xMatch = lower.match(/x\s*-?\s*axis\s*[:=]\s*([^,;\n]+)/);
    if (xMatch && !result.x) result.x = xMatch[1].trim();
    const yMatch = lower.match(/y\s*-?\s*axis\s*[:=]\s*([^,;\n]+)/);
    if (yMatch && !result.y) result.y = yMatch[1].trim();
    return result;
  };

  // Detect filtering requests for correlations (positive/negative only)
  const questionLower = question.toLowerCase();
  const wantsOnlyPositive = /\b(only\s+positive|positive\s+only|just\s+positive|dont\s+include\s+negative|don't\s+include\s+negative|no\s+negative|exclude\s+negative|filter\s+positive|show\s+only\s+positive)\b/i.test(question);
  const wantsOnlyNegative = /\b(only\s+negative|negative\s+only|just\s+negative|dont\s+include\s+positive|don't\s+include\s+positive|no\s+positive|exclude\s+positive|filter\s+negative|show\s+only\s+negative)\b/i.test(question);
  const correlationFilter = wantsOnlyPositive ? 'positive' : wantsOnlyNegative ? 'negative' : 'all';
  
  // Detect sort order preference - only set if user explicitly requested it
  const wantsDescending = /\bdescending|highest\s+to\s+lowest|high\s+to\s+low|largest\s+to\s+smallest|biggest\s+to\s+smallest\b/i.test(question);
  const wantsAscending = /\bascending|lowest\s+to\s+highest|low\s+to\s+high|smallest\s+to\s+largest|smallest\s+to\s+biggest\b/i.test(question);
  const sortOrder = wantsDescending ? 'descending' : wantsAscending ? 'ascending' : undefined; // Only set if user explicitly requested

  // CRITICAL: Detect correlation requests even when chart type is specified
  // This handles queries like "bar plot for showing the correlation between X and Y"
  const mentionsCorrelation = /\bcorrelation\s+(between|of|with)\b/i.test(question);
  const allColumns = summary.columns.map(c => c.name);
  
  if (mentionsCorrelation) {
    console.log('🔍 Correlation detected in query, extracting variables...');
    
    // Extract target variable and comparison variables from the question
    // Pattern: "correlation between [TARGET] and [VARIABLES]"
    const correlationMatch = question.match(/\bcorrelation\s+(between|of|with)\s+(.+?)\s+(?:and|with)\s+(.+)/i);
    
    if (correlationMatch && correlationMatch.length >= 4) {
      const targetRaw = correlationMatch[2].trim();
      const variablesRaw = correlationMatch[3].trim();
      
      console.log(`   Extracted target: "${targetRaw}"`);
      console.log(`   Extracted variables: "${variablesRaw}"`);
      
      // Find target column
      const targetCol = findMatchingColumn(targetRaw, allColumns);
      
      if (!targetCol) {
        return {
          answer: `I couldn't find a column matching "${targetRaw}". Available columns: ${allColumns.join(', ')}`
        };
      }
      
      const targetIsNumeric = summary.numericColumns.includes(targetCol);
      if (!targetIsNumeric) {
        return {
          answer: `"${targetCol}" is not a numeric column. Correlation analysis requires numeric variables.`
        };
      }
      
      // Determine which columns to analyze
      let comparisonColumns: string[] = [];
      
      // Check if user asked for "adstocked variables" or similar
      const wantsAdstocked = /\badstocked\s+variables?\b/i.test(variablesRaw);
      const wantsSpecificType = /\b(adstocked|reach|grp|tom)\s+variables?\b/i.test(variablesRaw);
      
      if (wantsAdstocked || wantsSpecificType) {
        // Filter to only columns containing the keyword
        const keyword = wantsAdstocked ? 'adstock' : variablesRaw.match(/\b(adstocked|reach|grp|tom)\b/i)?.[1]?.toLowerCase() || 'adstock';
        comparisonColumns = summary.numericColumns.filter(col => 
          col !== targetCol && 
          col.toLowerCase().includes(keyword)
        );
        console.log(`   Filtered to ${keyword} columns: [${comparisonColumns.join(', ')}]`);
      } else {
        // Try to find specific variable mentioned
        const specificCol = findMatchingColumn(variablesRaw, allColumns);
        if (specificCol && summary.numericColumns.includes(specificCol)) {
          comparisonColumns = [specificCol];
        } else {
          // Default: analyze all numeric columns except target
          comparisonColumns = summary.numericColumns.filter(col => col !== targetCol);
        }
      }
      
      if (comparisonColumns.length === 0) {
        return {
          answer: `No matching numeric columns found for "${variablesRaw}". Available numeric columns: ${summary.numericColumns.join(', ')}`
        };
      }
      
      console.log(`   Analyzing correlation: ${targetCol} vs [${comparisonColumns.join(', ')}]`);
      
      // Perform correlation analysis
      const { charts, insights } = await analyzeCorrelations(
        workingData,
        targetCol,
        comparisonColumns,
        correlationFilter,
        sortOrder,
        chatInsights,
        undefined // No limit for legacy dataAnalyzer
      );
      
      // Update bar chart title to be more specific if adstocked variables were requested
      let enrichedCharts = charts;
      if (wantsAdstocked && Array.isArray(charts)) {
        enrichedCharts = charts.map((c: any) => {
          if (c.type === 'bar' && c.x === 'variable' && c.y === 'correlation') {
            return {
              ...c,
              title: `Correlation Between ${targetCol} and Adstocked Variables`,
            };
          }
          return c;
        });
      }
      
      // Enrich charts with insights if needed
      try {
        const needsEnrichment = Array.isArray(enrichedCharts) && enrichedCharts.some((c: any) => !('keyInsight' in c));
        if (needsEnrichment) {
          enrichedCharts = await Promise.all(
            enrichedCharts.map(async (c: any) => {
              const chartInsights = await generateChartInsights(c, c.data || [], summary, chatInsights);
              return { ...c, keyInsight: c.keyInsight ?? chartInsights.keyInsight } as ChartSpec;
            })
          );
        }
      } catch (e) {
        console.error('Failed to enrich correlation charts:', e);
      }
      
      const filterNote = correlationFilter === 'positive' 
        ? ' I\'ve filtered to show only positive correlations as requested.' 
        : correlationFilter === 'negative' 
        ? ' I\'ve filtered to show only negative correlations as requested.' 
        : '';
      
      // Only mention sort order if user explicitly requested it
      const sortOrderNote = sortOrder === 'descending' ? ', sorted in descending order (highest to lowest)' : sortOrder === 'ascending' ? ', sorted in ascending order (lowest to highest)' : '';
      const answer = `I've analyzed the correlation between ${targetCol} and ${wantsAdstocked ? 'the adstocked variables' : variablesRaw}.${filterNote} The bar chart shows the correlation strength for each variable${sortOrderNote}.`;
      
      return withNotes({ answer, charts: enrichedCharts, insights });
    }
  }

  // Classify the question
  const classification = await classifyQuestion(question, summary.numericColumns);

  // If it's a correlation question, use correlation analyzer
  if (classification.type === 'correlation' && classification.targetVariable) {
    console.log('=== QUESTION CLASSIFICATION DEBUG ===');
    console.log('Classification:', classification);
    console.log('Available numeric columns:', summary.numericColumns);
    console.log('All available columns:', allColumns);
    
    // Find matching column names from ALL columns (not just numeric)
    const targetCol = findMatchingColumn(classification.targetVariable, allColumns);
    console.log(`Target column match: "${classification.targetVariable}" -> "${targetCol}"`);
    
    if (!targetCol) {
      return { 
        answer: `I couldn't find a column matching "${classification.targetVariable}". Available columns: ${allColumns.join(', ')}` 
      };
    }

    // Determine if target is numeric or categorical
    const targetIsNumeric = summary.numericColumns.includes(targetCol);

    // Check if it's a specific two-variable correlation
    if (classification.specificVariable) {
      // Check for explicit axis hints in the question
      const { x: explicitXRaw, y: explicitYRaw } = parseExplicitAxes(question);
      const explicitX = explicitXRaw ? findMatchingColumn(explicitXRaw, allColumns) : null;
      const explicitY = explicitYRaw ? findMatchingColumn(explicitYRaw, allColumns) : null;
      const specificCol = findMatchingColumn(classification.specificVariable, allColumns);
      console.log(`Specific column match: "${classification.specificVariable}" -> "${specificCol}"`);
      
      if (!specificCol) {
        return { 
          answer: `I couldn't find a column matching "${classification.specificVariable}". Available columns: ${allColumns.join(', ')}` 
        };
      }

      const specificIsNumeric = summary.numericColumns.includes(specificCol);

      console.log(`Target "${targetCol}" is ${targetIsNumeric ? 'numeric' : 'categorical'}`);
      console.log(`Specific "${specificCol}" is ${specificIsNumeric ? 'numeric' : 'categorical'}`);
      
      // Log sample data values to verify we're using the right columns
      const sampleRows = workingData.slice(0, 5);
      console.log(`Sample "${targetCol}" values:`, sampleRows.map(row => row[targetCol]));
      console.log(`Sample "${specificCol}" values:`, sampleRows.map(row => row[specificCol]));
      console.log('=== END CLASSIFICATION DEBUG ===');

      // Handle different combinations of numeric/categorical
      if (targetIsNumeric && specificIsNumeric) {
        // Respect explicit axis mapping if provided
        const xVar = explicitX && (explicitX === specificCol || explicitX === targetCol)
          ? explicitX
          : specificCol; // default X = specific variable
        const yVar = explicitY && (explicitY === specificCol || explicitY === targetCol)
          ? explicitY
          : targetCol; // default Y = target variable

        // Both numeric: Use correlation analysis
        // Only set sort order if user explicitly requested it
        const wantsDescending = /\bdescending|highest\s+to\s+lowest|high\s+to\s+low\b/i.test(question);
        const wantsAscending = /\bascending|lowest\s+to\s+highest|low\s+to\s+high\b/i.test(question);
        const sortOrder = wantsDescending ? 'descending' : wantsAscending ? 'ascending' : undefined;
        
        const { charts, insights } = await analyzeCorrelations(
          workingData,
          yVar,
          [xVar],
          correlationFilter,
          sortOrder,
          chatInsights,
          undefined // No limit for legacy dataAnalyzer
        );
        const filterNote = correlationFilter === 'positive' ? ' (showing only positive correlations)' : correlationFilter === 'negative' ? ' (showing only negative correlations)' : '';
        const answer = `I've analyzed the correlation between ${specificCol} and ${targetCol}${filterNote}. The scatter plot is oriented with X = ${xVar} and Y = ${yVar} as requested.`;
        return withNotes({ answer, charts, insights });
      } else if (targetIsNumeric && !specificIsNumeric) {
        // Categorical vs Numeric: Create bar chart
        const chartSpec: ChartSpec = {
          type: 'bar',
          title: `${explicitY ? explicitY : targetCol} by ${explicitX ? explicitX : specificCol}`,
          x: explicitX || specificCol,
          y: explicitY || targetCol,
          aggregate: 'mean',
        };
        const charts = [{
          ...chartSpec,
          data: processChartData(workingData, chartSpec),
        }];
        const answer = `I've created a bar chart showing how ${chartSpec.y} varies across ${chartSpec.x} categories (X=${chartSpec.x}, Y=${chartSpec.y}).`;
        return withNotes({ answer, charts });
      } else if (!targetIsNumeric && specificIsNumeric) {
        // Numeric vs Categorical: Create bar chart (swap axes)
        const chartSpec: ChartSpec = {
          type: 'bar',
          title: `${explicitY ? explicitY : specificCol} by ${explicitX ? explicitX : targetCol}`,
          x: explicitX || targetCol,
          y: explicitY || specificCol,
          aggregate: 'mean',
        };
        const charts = [{
          ...chartSpec,
          data: processChartData(workingData, chartSpec),
        }];
        const answer = `I've created a bar chart showing how ${chartSpec.y} varies across ${chartSpec.x} categories (X=${chartSpec.x}, Y=${chartSpec.y}).`;
        return withNotes({ answer, charts });
      } else {
        // Both categorical: Cannot analyze relationship numerically
        return { 
          answer: `Both "${targetCol}" and "${specificCol}" are categorical columns. I cannot perform numerical correlation analysis on categorical data. Try asking for a different visualization, such as a pie chart or bar chart.` 
        };
      }
    } else {
      console.log(`Analyzing general correlation for: ${targetCol}`);
      console.log('=== END CLASSIFICATION DEBUG ===');
      
      if (!targetIsNumeric) {
        return {
          answer: `"${targetCol}" is a categorical column. Correlation analysis requires a numeric target variable. Try asking about a numeric column like: ${summary.numericColumns.slice(0, 3).join(', ')}`
        };
      }
      
      // General correlation analysis - analyze all numeric variables except the target itself
      const comparisonColumns = summary.numericColumns.filter(col => col !== targetCol);
      
      // Detect sort order preference for general correlation questions - only set if user explicitly requested it
      const wantsDescendingGeneral = /\bdescending|highest\s+to\s+lowest|high\s+to\s+low|largest\s+to\s+smallest|biggest\s+to\s+smallest\b/i.test(question);
      const wantsAscendingGeneral = /\bascending|lowest\s+to\s+highest|low\s+to\s+high|smallest\s+to\s+largest|smallest\s+to\s+biggest\b/i.test(question);
      const sortOrderGeneral = wantsDescendingGeneral ? 'descending' : wantsAscendingGeneral ? 'ascending' : undefined; // Only set if user explicitly requested
      
      // Send progress update for correlation computation
      if (onThinkingStep) {
        onThinkingStep({
          step: `Computing correlations for ${targetCol}...`,
          status: 'active',
          timestamp: Date.now(),
          details: `Analyzing ${workingData.length.toLocaleString()} rows`,
        });
      }

      const { charts, insights } = await analyzeCorrelations(
        workingData,
        targetCol,
        comparisonColumns,
        correlationFilter,
        sortOrderGeneral,
        chatInsights,
        undefined, // No limit for legacy dataAnalyzer
        (message, processed, total) => {
          // Send progress updates via thinking steps
          if (onThinkingStep) {
            onThinkingStep({
              step: message,
              status: processed && total && processed < total ? 'active' : 'completed',
              timestamp: Date.now(),
              details: processed && total ? `${processed.toLocaleString()}/${total.toLocaleString()} rows` : undefined,
            });
          }
        },
        sessionId // Pass sessionId for caching
      );

      if (onThinkingStep) {
        onThinkingStep({
          step: `Correlation analysis complete`,
          status: 'completed',
          timestamp: Date.now(),
        });
      }

      // Fallback: if for any reason charts came back without per-chart insights,
      // enrich them here so the UI always gets keyInsight.
      let enrichedCharts = charts;
      try {
        const needsEnrichment = Array.isArray(charts) && charts.some((c: any) => !('keyInsight' in c));
        if (needsEnrichment) {
          enrichedCharts = await Promise.all(
            charts.map(async (c: any) => {
              const chartInsights = await generateChartInsights(c, c.data || [], summary, chatInsights);
              return { ...c, keyInsight: c.keyInsight ?? chartInsights.keyInsight } as ChartSpec;
            })
          );
        }
      } catch (e) {
        console.error('Fallback enrichment failed for chat correlation charts:', e);
      }

      const filterNote = correlationFilter === 'positive' 
        ? ' I\'ve filtered to show only positive correlations as requested.' 
        : correlationFilter === 'negative' 
        ? ' I\'ve filtered to show only negative correlations as requested.' 
        : '';
      const answer = `I've analyzed what affects ${targetCol}.${filterNote} The correlation analysis shows the relationship strength between different variables and ${targetCol}. Scatter plots show the actual relationships, and the bar chart ranks variables by correlation strength.`;

      return withNotes({ answer, charts: enrichedCharts, insights });
    }
  }

  // For general questions, generate answer and optional charts
  // Pass workingData (already filtered) instead of raw data
  return await generateGeneralAnswer(workingData, question, chatHistory, summary, sessionId, parsedQuery, transformationNotes, chatInsights);
}

async function generateChartSpecs(summary: DataSummary, useFastModel: boolean = false): Promise<ChartSpec[]> {
  // Use AI generation for all file types
  console.log('🤖 Using AI to generate charts for all file types...');
  
  const prompt = `Analyze this dataset and generate EXACTLY 4-6 chart specifications. You MUST return multiple charts to provide comprehensive insights.

DATA SUMMARY:
- Rows: ${summary.rowCount}
- Columns: ${summary.columnCount}
- Numeric columns: ${summary.numericColumns.join(', ')}
- Date columns: ${summary.dateColumns.join(', ')}
- All columns: ${summary.columns.map((c) => `${c.name} (${c.type})`).join(', ')}

CRITICAL: You MUST use ONLY the exact column names listed above. Do NOT make up or modify column names.

Generate 4-6 diverse chart specifications that reveal different insights. Each chart should analyze different aspects of the data. Output ONLY a valid JSON array with objects containing:
- type: "line"|"bar"|"scatter"|"pie"|"area"
- title: descriptive title
- x: column name (string, not array) - MUST be from the available columns list
- y: column name (string, not array) - MUST be from the available columns list
- aggregate: "sum"|"mean"|"count"|"none" (use "none" for scatter plots, choose appropriate for others)

IMPORTANT: 
- x and y must be EXACT column names from the available columns list above
- Generate EXACTLY 4-6 charts, not just 1
- Each chart should use different column combinations
- Choose diverse chart types that work well with the data
- Use only the exact column names provided - do not modify them

Chart type preferences:
- Line/area charts for time series (if date columns exist) - use DATE columns on X-axis
- Bar charts for categorical comparisons (top 10) - use CATEGORICAL columns (like Product, Brand, Category) on X-axis, NOT date columns
- Scatter plots for relationships between numeric columns - use NUMERIC columns on both axes
- Pie charts for proportions (top 5) - use CATEGORICAL columns (like Product, Brand, Category, Region) on X-axis, NOT date columns like Month or Date

CRITICAL RULES FOR PIE CHARTS:
- X-axis MUST be a categorical column (Product, Brand, Category, Region, etc.)
- NEVER use date columns (Month, Date, Week, Year) as X-axis for pie charts
- Y-axis should be a numeric column (sum, mean, count)
- Example: "Product" (x-axis) vs "Revenue" (y-axis) = pie chart showing revenue by product

Output format: [{"type": "...", "title": "...", "x": "...", "y": "...", "aggregate": "..."}, ...]`;

  // OPTIMIZATION: Use faster model for large files (2-3x faster, much cheaper)
  const aiModel = useFastModel ? 'gpt-4o-mini' : MODEL;
  
  const response = await openai.chat.completions.create({
    model: aiModel as string,
    messages: [
      {
        role: 'system',
        content: 'You are a data visualization expert. Output only valid JSON array. Column names (x, y) must be strings, not arrays. Always return a complete, valid JSON array of chart specifications.',
      },
      {
        role: 'user',
        content: prompt,
      },
    ],
    temperature: 0.7,
    max_tokens: 2000,
  });

  const content = response.choices[0].message.content;
  
  if (!content || content.trim() === '') {
    console.error('Empty response from OpenAI for chart generation');
    return [];
  }

  console.log('🤖 AI Response for chart generation:');
  console.log('Raw content length:', content.length);
  console.log('First 500 chars:', content.substring(0, 500));

  let parsed;

  try {
    parsed = JSON.parse(content);
    // Handle if the AI wrapped it in an object
    let charts = parsed.charts || parsed.specifications || parsed.data || parsed;
    
    // Ensure we have an array
    if (!Array.isArray(charts)) {
      // Maybe it's a single object? Wrap it
      if (typeof charts === 'object' && charts.type) {
        charts = [charts];
      } else {
        return [];
      }
    }
    
    // Sanitize chart specs to ensure x and y are strings and valid column names
    const availableColumns = summary.columns.map(c => c.name);
    const numericColumns = summary.numericColumns;
    const dateColumns = summary.dateColumns;
    
    // Get categorical columns (non-numeric, non-date)
    const categoricalColumns = availableColumns.filter(
      col => !numericColumns.includes(col) && !dateColumns.includes(col)
    );
    
    const sanitized = charts.slice(0, 6).map((spec: any) => {
      // Extract x and y, handling various formats
      let x = spec.x;
      let y = spec.y;
      
      if (Array.isArray(x)) x = x[0];
      if (Array.isArray(y)) y = y[0];
      if (typeof x === 'object' && x !== null) x = x.name || x.value || String(x);
      if (typeof y === 'object' && y !== null) y = y.name || y.value || String(y);
      
      x = String(x || '');
      y = String(y || '');
      
      // Validate and fix column names with improved matching
      if (!availableColumns.includes(x)) {
        console.warn(`⚠️ Invalid X column "${x}" not found in data. Available: ${availableColumns.join(', ')}`);
        
        // Try multiple matching strategies
        let similarX = availableColumns.find(col => 
          col.toLowerCase() === x.toLowerCase()
        );
        
        if (!similarX) {
          similarX = availableColumns.find(col => 
            col.toLowerCase().includes(x.toLowerCase()) || 
            x.toLowerCase().includes(col.toLowerCase())
          );
        }
        
        if (!similarX) {
          // Try partial word matching
          const xWords = x.toLowerCase().split(/[\s_-]+/);
          similarX = availableColumns.find(col => {
            const colWords = col.toLowerCase().split(/[\s_-]+/);
            return xWords.some((word: string) => word.length > 2 && colWords.some((cWord: string) => cWord.includes(word) || word.includes(cWord)));
          });
        }
        
        if (!similarX) {
          // Try fuzzy matching for common abbreviations
          const fuzzyMatches = {
            'nGRP': 'GRP',
            'Adstocked': 'Adstock',
            'Reach': 'Reach',
            'TOM': 'TOM',
            'Max': 'Max'
          };
          
          for (const [key, value] of Object.entries(fuzzyMatches)) {
            if (x.includes(key)) {
              similarX = availableColumns.find(col => col.includes(value));
              if (similarX) break;
            }
          }
        }
        
        x = similarX || availableColumns[0];
        console.log(`   Fixed X column to: "${x}"`);
      }
      
      if (!availableColumns.includes(y)) {
        console.warn(`⚠️ Invalid Y column "${y}" not found in data. Available: ${availableColumns.join(', ')}`);
        
        // Try multiple matching strategies for Y column
        let similarY = availableColumns.find(col => 
          col.toLowerCase() === y.toLowerCase()
        );
        
        if (!similarY) {
          similarY = availableColumns.find(col => 
            col.toLowerCase().includes(y.toLowerCase()) || 
            y.toLowerCase().includes(col.toLowerCase())
          );
        }
        
        if (!similarY) {
          // Try partial word matching
          const yWords = y.toLowerCase().split(/[\s_-]+/);
          similarY = availableColumns.find(col => {
            const colWords = col.toLowerCase().split(/[\s_-]+/);
            return yWords.some((word: string) => word.length > 2 && colWords.some((cWord: string) => cWord.includes(word) || word.includes(cWord)));
          });
        }
        
        if (!similarY) {
          // Try fuzzy matching for common abbreviations
          const fuzzyMatches = {
            'nGRP': 'GRP',
            'Adstocked': 'Adstock',
            'Reach': 'Reach',
            'TOM': 'TOM',
            'Max': 'Max'
          };
          
          for (const [key, value] of Object.entries(fuzzyMatches)) {
            if (y.includes(key)) {
              similarY = availableColumns.find(col => col.includes(value));
              if (similarY) break;
            }
          }
        }
        
        y = similarY || (numericColumns[0] || availableColumns[1]);
        console.log(`   Fixed Y column to: "${y}"`);
      }
      
      // For pie charts, ensure X-axis is NOT a date column
      if (spec.type === 'pie' && dateColumns.includes(x)) {
        console.warn(`⚠️ Pie chart "${spec.title}" incorrectly uses date column "${x}" on X-axis. Finding categorical alternative...`);
        
        // Try to find a categorical column instead
        const alternativeX = categoricalColumns.find(col => 
          col.toLowerCase().includes('product') || 
          col.toLowerCase().includes('brand') || 
          col.toLowerCase().includes('category') ||
          col.toLowerCase().includes('region') ||
          col.toLowerCase().includes('name')
        ) || categoricalColumns[0];
        
        if (alternativeX) {
          console.log(`   Replacing "${x}" with "${alternativeX}" for pie chart`);
          x = alternativeX;
        } else {
          console.warn(`   No categorical column found, skipping this pie chart`);
          return null; // Will be filtered out
        }
      }
      
      // Sanitize aggregate field to only allow valid enum values
      let aggregate = spec.aggregate || 'none';
      const validAggregates = ['sum', 'mean', 'count', 'none'];
      if (!validAggregates.includes(aggregate)) {
        console.warn(`⚠️ Invalid aggregate value "${aggregate}", defaulting to "none"`);
        aggregate = 'none';
      }

      return {
        type: spec.type,
        title: spec.title || 'Untitled Chart',
        x: x,
        y: y,
        aggregate: aggregate,
      };
    }).filter((spec: any) => {
      if (!spec || !spec.type || !spec.x || !spec.y) return false;
      if (!['line', 'bar', 'scatter', 'pie', 'area'].includes(spec.type)) return false;
      
      // Filter out pie charts with date columns (unless explicitly requested in generateGeneralAnswer)
      // This function is for auto-generated charts, so we don't allow date columns for pie charts here
      if (spec.type === 'pie' && dateColumns.includes(spec.x)) {
        return false;
      }
      
      return true;
    });
    
    console.log('Generated charts:', sanitized.length);
    console.log(sanitized);
    return sanitized;
  } catch (error) {
    console.error('Error parsing chart specs:', error);
    console.error('Raw AI response (first 500 chars):', content?.substring(0, 500));
    return [];
  }
}

// generateChartInsights is now centralized in insightGenerator.ts

export async function generateInsights(
  data: Record<string, any>[],
  summary: DataSummary,
  useFastModel: boolean = false
): Promise<Insight[]> {
  // OPTIMIZATION: Sample data for large files to speed up statistics calculation
  // For files > 50K rows, use systematic sampling to get representative statistics
  const MAX_SAMPLE_SIZE = 50000; // Use max 50K rows for statistics (statistically sufficient)
  const useSampling = data.length > MAX_SAMPLE_SIZE;
  
  let sampleData: Record<string, any>[];
  let samplingRatio = 1.0;
  
  if (useSampling) {
    // Use systematic sampling for better representation across the dataset
    const step = Math.floor(data.length / MAX_SAMPLE_SIZE);
    sampleData = [];
    for (let i = 0; i < data.length && sampleData.length < MAX_SAMPLE_SIZE; i += step) {
      sampleData.push(data[i]);
    }
    samplingRatio = data.length / sampleData.length;
    console.log(`📊 Performance optimization: Sampling ${sampleData.length} rows from ${data.length} total (${(samplingRatio).toFixed(1)}x ratio) for statistics calculation`);
  } else {
    sampleData = data;
  }

  // Calculate comprehensive statistics with percentiles and variability
  const stats: Record<string, any> = {};
  const isPercent: Record<string, boolean> = {};

  const percentile = (arr: number[], p: number): number => {
    if (arr.length === 0) return NaN;
    const sorted = [...arr].sort((a, b) => a - b);
    const idx = (sorted.length - 1) * p;
    const lo = Math.floor(idx);
    const hi = Math.ceil(idx);
    if (lo === hi) return sorted[lo];
    return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
  };

  const stdDev = (arr: number[]): number => {
    if (arr.length === 0) return 0;
    const mean = arr.reduce((a, b) => a + b, 0) / arr.length;
    const variance = arr.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / arr.length;
    return Math.sqrt(variance);
  };

  // Helper to format values per column (adds % when needed)
  const formatValue = (col: string, v: number): string => {
    if (!isFinite(v)) return String(v);
    const abs = Math.abs(v);
    const fmt = (n: number) => {
      if (abs >= 100) return n.toFixed(0);
      if (abs >= 10) return n.toFixed(1);
      if (abs >= 1) return n.toFixed(2);
      return n.toFixed(3);
    };
    return isPercent[col] ? `${fmt(v)}%` : fmt(v);
  };

  for (const col of summary.numericColumns.slice(0, 5)) {
    // Detect percentage columns by scanning raw values for '%'
    const rawHasPercent = sampleData
      .slice(0, 200)
      .map(row => row[col])
      .filter(v => v !== null && v !== undefined)
      .some(v => typeof v === 'string' && v.includes('%'));
    isPercent[col] = rawHasPercent;

    // Use sampled data for statistics calculation (much faster for large files)
    const values = sampleData.map((row) => Number(String(row[col]).replace(/[%,,]/g, ''))).filter((v) => !isNaN(v));
    if (values.length > 0) {
      const avg = values.reduce((a, b) => a + b, 0) / values.length;
      const p25 = percentile(values, 0.25);
      const p50 = percentile(values, 0.5);
      const p75 = percentile(values, 0.75);
      const p90 = percentile(values, 0.9);
      const std = stdDev(values);
      const cv = avg !== 0 ? (std / Math.abs(avg)) * 100 : 0;
      
      // Calculate min/max without spread operator to avoid stack overflow on large arrays
      let min = values[0];
      let max = values[0];
      for (let i = 1; i < values.length; i++) {
        if (values[i] < min) min = values[i];
        if (values[i] > max) max = values[i];
      }
      
      // Scale total to full dataset size if we used sampling
      const sampleTotal = values.reduce((a, b) => a + b, 0);
      const scaledTotal = useSampling ? sampleTotal * samplingRatio : sampleTotal;
      
      stats[col] = {
        min: min,
        max: max,
        avg: avg, // Average is the same regardless of sample size
        total: scaledTotal, // Scale total to represent full dataset
        median: p50,
        p25,
        p75,
        p90,
        stdDev: std,
        cv: cv,
        variability: cv > 30 ? 'high' : cv > 15 ? 'moderate' : 'low',
        count: useSampling ? Math.round(values.length * samplingRatio) : values.length, // Scale count to full dataset
      };
    }
  }

  // Calculate top/bottom values for each column
  // For large files, use efficient single-pass algorithm instead of sorting all values
  // This is O(n) instead of O(n log n) and doesn't require loading all values into memory
  const topBottomStats: Record<string, {top: Array<{value: number, row: number}>, bottom: Array<{value: number, row: number}>}> = {};
  for (const col of summary.numericColumns.slice(0, 5)) {
    // For large files, use efficient single-pass algorithm to find top/bottom 3
    // This avoids sorting millions of values
    if (data.length > 50000) {
      // Efficient O(n) approach: single pass to find top/bottom 3
      let top3: Array<{value: number, row: number}> = [];
      let bottom3: Array<{value: number, row: number}> = [];
      
      for (let idx = 0; idx < data.length; idx++) {
        const row = data[idx];
        const numValue = Number(String(row[col]).replace(/[%,,]/g, ''));
        if (isNaN(numValue)) continue;
        
        const item = { value: numValue, row: idx };
        
        // Maintain top 3 (sorted descending)
        if (top3.length < 3) {
          top3.push(item);
          top3.sort((a, b) => b.value - a.value);
        } else if (numValue > top3[2].value) {
          top3[2] = item;
          top3.sort((a, b) => b.value - a.value);
        }
        
        // Maintain bottom 3 (sorted ascending)
        if (bottom3.length < 3) {
          bottom3.push(item);
          bottom3.sort((a, b) => a.value - b.value);
        } else if (numValue < bottom3[2].value) {
          bottom3[2] = item;
          bottom3.sort((a, b) => a.value - b.value);
        }
      }
      
      topBottomStats[col] = { top: top3, bottom: bottom3 };
    } else {
      // For smaller files, use the original approach (sorting is fast enough)
      const valuesWithIndex = data
        .map((row, idx) => ({ value: Number(String(row[col]).replace(/[%,,]/g, '')), row: idx }))
        .filter(item => !isNaN(item.value));
      
      if (valuesWithIndex.length > 0) {
        topBottomStats[col] = {
          top: valuesWithIndex.sort((a, b) => b.value - a.value).slice(0, 3),
          bottom: valuesWithIndex.sort((a, b) => a.value - b.value).slice(0, 3),
        };
      }
    }
  }

  const prompt = `Analyze this dataset and provide 5-7 specific, actionable business insights with QUANTIFIED suggestions.

DATA SUMMARY:
- ${summary.rowCount} rows, ${summary.columnCount} columns
- Numeric columns: ${summary.numericColumns.join(', ')}

COMPREHENSIVE STATISTICS:
${Object.entries(stats)
  .map(([col, s]: [string, any]) => {
    const topBottom = topBottomStats[col];
    const topStr = topBottom?.top.map(t => `${formatValue(col, t.value)}`).join(', ') || 'N/A';
    const bottomStr = topBottom?.bottom.map(t => `${formatValue(col, t.value)}`).join(', ') || 'N/A';
    return `${col}:
  - Range: ${formatValue(col, s.min)} to ${formatValue(col, s.max)}
  - Average: ${formatValue(col, s.avg)}
  - Median: ${formatValue(col, s.median)}
  - 25th percentile: ${formatValue(col, s.p25)}, 75th percentile: ${formatValue(col, s.p75)}, 90th percentile: ${formatValue(col, s.p90)}
  - Total: ${formatValue(col, s.total)}
  - Standard Deviation: ${formatValue(col, s.stdDev)}
  - Coefficient of Variation: ${s.cv.toFixed(1)}% (${s.variability} variability)
  - Top 3 values: ${topStr}
  - Bottom 3 values: ${bottomStr}
  - Data points: ${s.count}`;
  })
  .join('\n\n')}

Each insight MUST include:
1. A bold headline with the key finding (e.g., **High Marketing Efficiency:**)
2. Specific numbers, percentages, or metrics from the statistics above (use actual percentiles, averages, top/bottom values)
3. Explanation of WHY this matters to the business
4. Actionable suggestion starting with "**Actionable Suggestion:**" that includes:
   - Explicit numeric targets or thresholds (e.g., "target ${summary.numericColumns[0]} above ${formatValue(summary.numericColumns[0] || '', stats[summary.numericColumns[0] || '']?.p75 || 0)}", "maintain between ${formatValue(summary.numericColumns[0] || '', stats[summary.numericColumns[0] || '']?.p25 || 0)}-${formatValue(summary.numericColumns[0] || '', stats[summary.numericColumns[0] || '']?.p75 || 0)}")
   - Specific improvement goals (e.g., "increase by X%", "reduce by Y units", "achieve ${formatValue(summary.numericColumns[0] || '', stats[summary.numericColumns[0] || '']?.p90 || 0)}")
   - Quantified benchmarks (e.g., "reach top 10% performance of ${topBottomStats[summary.numericColumns[0]]?.top[0]?.value.toFixed(2) || 'target'}")
   - Measurable action items with specific numbers

Format each insight as a complete paragraph with the structure:
**[Insight Title]:** [Finding with specific metrics from statistics]. **Why it matters:** [Business impact]. **Actionable Suggestion:** [Quantified suggestion with specific targets, thresholds, and improvement goals].

CRITICAL REQUIREMENTS:
- Use ACTUAL numbers from the statistics above (percentiles, averages, top/bottom values)
- Suggestions must be measurable and quantifiable with specific targets
- Include specific improvement percentages or absolute values
- NEVER use percentile labels like "P75", "P90", "P25", "P50", "P75 level", "P90 level", "P75 value", "P90 value" in your output
- ONLY use the numeric values themselves (e.g., "increase to ${formatValue(summary.numericColumns[0] || '', stats[summary.numericColumns[0] || '']?.p75 || 0)}" NOT "increase to P75 level (${formatValue(summary.numericColumns[0] || '', stats[summary.numericColumns[0] || '']?.p75 || 0)})")
- No vague language - use specific numbers like "increase to ${formatValue(summary.numericColumns[0] || '', stats[summary.numericColumns[0] || '']?.p75 || 0)}" or "maintain between ${formatValue(summary.numericColumns[0] || '', stats[summary.numericColumns[0] || '']?.p25 || 0)}-${formatValue(summary.numericColumns[0] || '', stats[summary.numericColumns[0] || '']?.p75 || 0)}"

Example:
**Revenue Concentration Risk:** The top 3 products account for 78% of total revenue ($2.4M out of $3.1M), indicating high dependency. Average revenue per product is $X, with top performer at $Y. **Why it matters:** Over-reliance on few products creates vulnerability to market shifts or competitive pressure. **Actionable Suggestion:** Diversify revenue streams by investing in product development for the remaining portfolio. Target: Increase bottom 50% products' revenue by 25% to reach ${stats.revenue?.median.toFixed(2) || 'target'} within 12 months, aiming for 60/40 split between top and bottom performers.

Output as JSON array:
{
  "insights": [
    { "text": "**Insight Title:** Full insight text here with quantified suggestion..." },
    ...
  ]
}`;

  // OPTIMIZATION: Use faster model for large files (2-3x faster, much cheaper)
  const aiModel = useFastModel ? 'gpt-4o-mini' : MODEL;
  
  const response = await openai.chat.completions.create({
    model: aiModel as string,
    messages: [
      {
        role: 'system',
        content: 'You are a senior business analyst. Provide detailed, quantitative insights with specific metrics and actionable suggestions. NEVER use percentile labels like P75, P90, P25, P75 level, P90 level, P75 value, P90 value - only use numeric values. Output valid JSON.',
      },
      {
        role: 'user',
        content: prompt,
      },
    ],
    response_format: { type: 'json_object' },
    temperature: 0.6,
    max_tokens: 2500,
  });

  const content = response.choices[0].message.content || '{}';

  try {
    const parsed = JSON.parse(content);
    const insightArray = parsed.insights || [];
    
    return insightArray.slice(0, 7).map((item: any, index: number) => ({
      id: index + 1,
      text: item.text || item.insight || String(item),
    }));
  } catch (error) {
    console.error('Error parsing insights:', error);
    return [];
  }
}

async function classifyQuestion(
  question: string,
  numericColumns: string[]
): Promise<{ 
  type: 'correlation' | 'general'; 
  targetVariable: string | null;
  specificVariable?: string | null;
}> {
  const prompt = `Classify this question:

QUESTION: ${question}
NUMERIC COLUMNS: ${numericColumns.join(', ')}

IMPORTANT: Only classify as "correlation" if the question specifically asks about correlations, relationships, or what affects/influences something.

If the question:
- Requests a SPECIFIC chart type (pie chart, bar chart, line chart, etc.) → type: "general"
- Mentions specific chart visualization → type: "general"
- Asks about correlations/relationships WITHOUT specifying a chart type → type: "correlation"
- Asks "what affects" or "what influences" → type: "correlation"

For correlation questions:
- SPECIFIC: identifies two variables (e.g., "correlation between X and Y")
- GENERAL: asks what affects one variable (e.g., "what affects Y")

Output JSON:
{
  "type": "correlation" or "general",
  "isSpecific": true or false,
  "targetVariable": "column_name" or null,
  "specificVariable": "column_name" or null (only for specific correlations)
}

Examples:
- "pie chart between product type and revenue" → {"type": "general", "targetVariable": null, "specificVariable": null}
- "show me a bar chart of sales by region" → {"type": "general", "targetVariable": null, "specificVariable": null}
- "correlation between lead times and revenue" → {"type": "correlation", "isSpecific": true, "specificVariable": "lead times", "targetVariable": "revenue"}
- "what affects revenue" → {"type": "correlation", "isSpecific": false, "targetVariable": "revenue", "specificVariable": null}`;

  const response = await openai.chat.completions.create({
    model: MODEL as string,
    messages: [
      {
        role: 'system',
        content: 'You are a question classifier. Chart requests should be classified as "general", not "correlation". Output only valid JSON.',
      },
      {
        role: 'user',
        content: prompt,
      },
    ],
    response_format: { type: 'json_object' },
    temperature: 0.3,
    max_tokens: 200,
  });

  const content = response.choices[0].message.content || '{"type": "general", "targetVariable": null}';

  try {
    const result = JSON.parse(content);
    return {
      type: result.type === 'correlation' ? 'correlation' : 'general',
      targetVariable: result.targetVariable || null,
      specificVariable: result.specificVariable || null,
    };
  } catch {
    return { type: 'general', targetVariable: null };
  }
}

/**
 * Handle aggregation queries with category filters directly using regex
 * This performs the data operation immediately without asking AI
 */
async function handleAggregationWithCategoryDirect(
  question: string,
  data: Record<string, any>[],
  summary: DataSummary
): Promise<{ answer: string; charts?: ChartSpec[]; insights?: Insight[] } | null> {
  const questionLower = question.toLowerCase();
  
  // Detect aggregation with category patterns
  const isAggregationQuery = /\b(aggregated?\s+(?:column\s+name\s+)?value|aggregate|total|sum)\s+(?:for|of|in)\s+(?:the\s+)?(?:column\s+)?(?:category\s+)?[\w\s]+/i.test(question) ||
                             /\b(?:what\s+is\s+)?(?:the\s+)?(?:aggregated?\s+(?:column\s+name\s+)?value|total|sum)\s+(?:for|of|in)\s+(?:the\s+)?(?:column\s+)?(?:category\s+)?[\w\s]+/i.test(question) ||
                             /\b(?:aggregated?\s+value|aggregate|total|sum)\s+(?:for|of|in)\s+(?:the\s+)?column\s+category\s+[\w\s]+/i.test(question);
  
  if (!isAggregationQuery) {
    return null;
  }
  
  console.log('📊 Detected aggregation with category query - handling directly with regex');
  
  const allColumns = summary.columns.map(c => c.name);
  const numericColumns = summary.numericColumns || [];
  
  // Extract aggregation column name using regex
  // Patterns: "aggregated column name X", "aggregated value for column X", "total for X"
  let aggregateColumn: string | null = null;
  const aggregateColumnPatterns = [
    /aggregated?\s+column\s+name\s+(\w+)/i,
    /aggregated?\s+value\s+(?:for|of|in)\s+(?:the\s+)?column\s+(\w+)/i,
    /(?:total|sum)\s+(?:value\s+)?(?:for|of|in)\s+(?:the\s+)?(?:column\s+)?(\w+)/i,
  ];
  
  for (const pattern of aggregateColumnPatterns) {
    const match = question.match(pattern);
    if (match && match[1]) {
      const colName = match[1].trim();
      const matched = findMatchingColumn(colName, numericColumns);
      if (matched) {
        aggregateColumn = matched;
        console.log(`   ✅ Extracted aggregation column: "${aggregateColumn}"`);
        break;
      }
    }
  }
  
  // If not found, try to find "total" or first numeric column
  if (!aggregateColumn) {
    if (numericColumns.includes('total')) {
      aggregateColumn = 'total';
    } else if (numericColumns.length > 0) {
      aggregateColumn = numericColumns[0];
    }
    console.log(`   ✅ Using default aggregation column: "${aggregateColumn}"`);
  }
  
  // Extract category value using regex
  // Patterns: "for the column category X", "for category X", "for X"
  let categoryValue: string | null = null;
  const categoryPatterns = [
    /(?:for|of|in)\s+(?:the\s+)?column\s+category\s+(.+?)(?:\s*$|\s+category|\s+column)/i,
    /(?:for|of|in)\s+(?:the\s+)?category\s+(.+?)(?:\s*$|\s+category|\s+column|\s+value)/i,
    /(?:for|of|in)\s+(?:the\s+)?(?:column\s+)?(?:category\s+)?(.+?)$/i,
  ];
  
  for (const pattern of categoryPatterns) {
    const match = question.match(pattern);
    if (match && match[1]) {
      categoryValue = match[1].trim();
      // Remove trailing words that might be part of the sentence
      categoryValue = categoryValue.replace(/\s+(category|column|value|for|of|in|\?)$/i, '').trim();
      // Handle cases like "men's fashion" - preserve apostrophes and spaces
      if (categoryValue && categoryValue.length > 0) {
        console.log(`   ✅ Extracted category value: "${categoryValue}"`);
        break;
      }
    }
  }
  
  if (!categoryValue) {
    console.log('   ❌ Could not extract category value');
    return null; // Let AI handle it
  }
  
  if (!aggregateColumn) {
    console.log('   ❌ Could not find aggregation column');
    return null; // Let AI handle it
  }
  
  // Find category column
  let categoryColumn: string | null = null;
  const categoryColumnNames = ['category', 'Category', 'product_category', 'Product Category', 'product_category_name', 'cat'];
  for (const colName of categoryColumnNames) {
    const matched = findMatchingColumn(colName, allColumns);
    if (matched) {
      categoryColumn = matched;
      break;
    }
  }
  
  // If not found, try to find any non-numeric column that contains the category value
  if (!categoryColumn) {
    for (const col of allColumns) {
      if (!numericColumns.includes(col) && !summary.dateColumns.includes(col)) {
        const sampleValues = data.slice(0, 100).map(row => String(row[col] || '').toLowerCase());
        if (sampleValues.some(val => val.includes(categoryValue!.toLowerCase()) || categoryValue!.toLowerCase().includes(val))) {
          categoryColumn = col;
          break;
        }
      }
    }
  }
  
  if (!categoryColumn) {
    return {
      answer: `I couldn't find a category column in your dataset. Available columns: ${allColumns.slice(0, 10).join(', ')}${allColumns.length > 10 ? '...' : ''}`
    };
  }
  
  console.log(`   📊 Filtering by category: "${categoryColumn}" = "${categoryValue}"`);
  console.log(`   📊 Aggregating column: "${aggregateColumn}"`);
  
  // Filter data by category
  const filteredData = data.filter(row => {
    const rowCategory = String(row[categoryColumn!] || '').toLowerCase().trim();
    const targetCategory = categoryValue!.toLowerCase().trim();
    return rowCategory === targetCategory || rowCategory.includes(targetCategory) || targetCategory.includes(rowCategory);
  });
  
  if (filteredData.length === 0) {
    return {
      answer: `I couldn't find any data for the category "${categoryValue}" in the "${categoryColumn}" column. Please check that the category name is correct.`
    };
  }
  
  // Calculate aggregation
  const values = filteredData
    .map(row => {
      const val = row[aggregateColumn!];
      if (val === null || val === undefined || val === '') return null;
      if (typeof val === 'number') return isNaN(val) ? null : val;
      const cleaned = String(val).replace(/[%,]/g, '').trim();
      const parsed = Number(cleaned);
      return isNaN(parsed) ? null : parsed;
    })
    .filter(v => v !== null) as number[];
  
  if (values.length === 0) {
    return {
      answer: `I found ${filteredData.length} records for category "${categoryValue}", but the ${aggregateColumn} column has no numeric values.`
    };
  }
  
  const sum = values.reduce((a, b) => a + b, 0);
  const avg = sum / values.length;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const count = values.length;
  
  const answer = `The aggregated value for the category "${categoryValue}" is:\n\n` +
    `- Total (Sum): ${sum.toFixed(2)}\n` +
    `- Average: ${avg.toFixed(2)}\n` +
    `- Minimum: ${min.toFixed(2)}\n` +
    `- Maximum: ${max.toFixed(2)}\n` +
    `- Count: ${count} records`;
  
  console.log(`   ✅ Calculated aggregation: Sum=${sum.toFixed(2)}, Avg=${avg.toFixed(2)}, Count=${count}`);
  
  return {
    answer,
  };
}

/**
 * Check if user explicitly requested charts/visualizations
 */
function isExplicitChartRequest(question: string): boolean {
  const lower = question.toLowerCase();
  const chartKeywords = [
    /\b(show|display|create|generate|make|draw|plot|graph)\s+(me\s+)?(a\s+)?(chart|graph|plot|visualization|visual|diagram|figure)/i,
    /\b(chart|graph|plot|visualization|visual)\s+(of|for|showing|with)/i,
    /\b(show|display|create|generate|make|draw)\s+(me\s+)?(a\s+)?(bar|line|scatter|pie|area)\s+(chart|graph|plot)/i,
    /\b(visualize|visualization|visual)\s+/i,
    /\b(can you|please)\s+(show|display|create|generate|make|draw)\s+(me\s+)?(a\s+)?(chart|graph|plot)/i,
  ];
  
  return chartKeywords.some(pattern => pattern.test(lower));
}

export async function generateGeneralAnswer(
  data: Record<string, any>[],
  question: string,
  chatHistory: Message[],
  summary: DataSummary,
  sessionId?: string,
  preParsedQuery?: ParsedQuery | null,
  preTransformationNotes?: string[],
  chatInsights?: Insight[],
  requiredColumns?: string[]
): Promise<{ answer: string; charts?: ChartSpec[]; insights?: Insight[] }> {
  // QUERY-ONLY ARCHITECTURE: For information-seeking queries, execute query and return results (no explanations)
  if (isInformationSeekingQuery(question)) {
    console.log('🔍 Detected information-seeking query, using query-only reasoning layer');
    try {
      // Step 1: Identify relevant columns AND filter data based on query conditions
      console.log('📋 Step 1: Identifying relevant columns and filtering data...');
      const columnIdentification = await identifyRelevantColumnsAndFilterData(question, data, summary, chatHistory);
      console.log('✅ Columns identified:', columnIdentification.identifiedColumns);
      console.log('📝 Column mapping:', columnIdentification.columnMapping);
      console.log('🔍 Filters applied:', columnIdentification.filterDescription);
      console.log(`📊 Data filtered: ${data.length} → ${columnIdentification.filteredData.length} rows`);
      
      // Step 2: Generate execution plan using identified columns and filtered data
      console.log('📋 Step 2: Generating execution plan...');
      const executionPlan = await generateExecutionPlan(question, summary, chatHistory, columnIdentification, data);
      console.log('✅ Execution plan generated:', executionPlan.description);
      
      // Step 3: Execute plan using Python engine with filtered data
      console.log('📋 Step 3: Executing plan with filtered data...');
      const executionResult = await executePlan(executionPlan, columnIdentification.filteredData);
      
      if (!executionResult.success || !executionResult.data) {
        throw new Error(executionResult.error || 'Execution failed');
      }
      
      console.log(`✅ Query executed: ${executionResult.data.length} rows returned`);
      
      // Step 4: Format results as a clean response (no explanations, just the data)
      const results = executionResult.data;
      let formattedAnswer = '';
      
      if (results.length === 0) {
        formattedAnswer = 'No results found.';
      } else {
        // Helper function to format values
        const formatValue = (key: string, value: any): string => {
          if (value === null || value === undefined) {
            return 'N/A';
          }
          
          // Format numbers with Indian currency if it's a revenue/amount column
          if (typeof value === 'number') {
            const keyLower = key.toLowerCase();
            if (keyLower.includes('revenue') || keyLower.includes('total') || keyLower.includes('amount') || keyLower.includes('value') || keyLower.includes('sales')) {
              if (value >= 10000000) {
                return `₹${(value / 10000000).toFixed(2)} crore`;
              } else if (value >= 100000) {
                return `₹${(value / 100000).toFixed(2)} lakh`;
              } else {
                return `₹${value.toLocaleString('en-IN')}`;
              }
            }
            // Format other numbers with thousand separators
            return value.toLocaleString('en-IN');
          }
          
          return String(value);
        };
        
        if (results.length === 1) {
          // Single result - format as key-value pairs
          const result = results[0];
          const entries = Object.entries(result)
            .map(([key, value]) => `${key}: ${formatValue(key, value)}`)
            .join('\n');
          formattedAnswer = entries;
        } else {
          // Multiple results - format as table
          const headers = Object.keys(results[0]);
          const maxResults = 100; // Limit to top 100 results
          const displayResults = results.slice(0, maxResults);
          
          // Calculate column widths for better alignment
          const columnWidths = headers.map(header => {
            const headerWidth = header.length;
            const maxValueWidth = Math.max(
              ...displayResults.map(row => formatValue(header, row[header]).length)
            );
            return Math.max(headerWidth, maxValueWidth, 10); // Minimum width of 10
          });
          
          // Format header row
          const headerRow = headers
            .map((header, idx) => header.padEnd(columnWidths[idx]))
            .join(' | ');
          
          // Format separator
          const separator = columnWidths.map(width => '-'.repeat(width)).join(' | ');
          
          // Format data rows
          const dataRows = displayResults.map(row => 
            headers
              .map((header, idx) => formatValue(header, row[header]).padEnd(columnWidths[idx]))
              .join(' | ')
          );
          
          formattedAnswer = `${headerRow}\n${separator}\n${dataRows.join('\n')}`;
          
          // Add summary if results were limited
          if (results.length > maxResults) {
            formattedAnswer += `\n\n(Showing top ${maxResults} of ${results.length} results)`;
          } else {
            formattedAnswer += `\n\n(Total: ${results.length} results)`;
          }
        }
      }
      
      return {
        answer: formattedAnswer,
        charts: undefined,
        insights: undefined,
      };
    } catch (error) {
      console.error('❌ Error in information-seeking query execution:', error);
      // Fall through to current architecture
      console.log('⚠️ Falling back to current architecture');
    }
  }
  
  // CURRENT ARCHITECTURE: Route analytical (non-visualization) queries through execution plan engine with execution
  if (isAnalyticalQuery(question)) {
    console.log('🔍 Detected analytical query, using new execution plan architecture');
    try {
      // Step 1: Identify relevant columns AND filter data based on query conditions
      console.log('📋 Step 1: Identifying relevant columns and filtering data...');
      const columnIdentification = await identifyRelevantColumnsAndFilterData(question, data, summary, chatHistory);
      console.log('✅ Columns identified:', columnIdentification.identifiedColumns);
      console.log('📝 Column mapping:', columnIdentification.columnMapping);
      console.log('🔍 Filters applied:', columnIdentification.filterDescription);
      console.log(`📊 Data filtered: ${data.length} → ${columnIdentification.filteredData.length} rows`);
      
      // Step 2: Generate execution plan using identified columns and filtered data
      console.log('📋 Step 2: Generating execution plan...');
      const executionPlan = await generateExecutionPlan(question, summary, chatHistory, columnIdentification, data);
      console.log('✅ Execution plan generated:', executionPlan.description);
      
      // Step 3: Execute plan using Python engine with filtered data
      console.log('📋 Step 3: Executing plan with filtered data...');
      const executionResult = await executePlan(executionPlan, columnIdentification.filteredData);
      
      if (!executionResult.success || !executionResult.data) {
        throw new Error(executionResult.error || 'Execution failed');
      }
      
      console.log(`✅ Query executed: ${executionResult.data.length} rows returned`);
      
      // Step 3: Generate explanation using Azure OpenAI
      const explanation = await generateExplanation(
        question,
        executionPlan,
        executionResult.data,
        summary
      );
      
      return {
        answer: explanation,
        charts: undefined, // No charts for analytical queries
        insights: undefined,
      };
    } catch (error) {
      console.error('❌ Error in analytical query engine:', error);
      // Fall through to legacy implementation
      console.log('⚠️ Falling back to legacy implementation');
    }
  }
  
  // CRITICAL: Handle aggregation queries with category filters directly using regex
  // These are data operations that should be performed immediately, not passed to AI
  const aggregationWithCategoryResult = await handleAggregationWithCategoryDirect(question, data, summary);
  if (aggregationWithCategoryResult) {
    return aggregationWithCategoryResult;
  }
  
  // Check if user explicitly requested charts
  const wantsCharts = isExplicitChartRequest(question);
  // Detect explicit axis hints for any chart request (including secondary Y-axis)
  const parseExplicitAxes = (q: string): { x?: string; y?: string; y2?: string } => {
    const result: { x?: string; y?: string; y2?: string } = {};
    const axisRegex = /(.*?)\(([^\)]*)\)/g;
    let m: RegExpExecArray | null;
    while ((m = axisRegex.exec(q)) !== null) {
      const rawName = m[1].trim();
      const axisText = m[2].toLowerCase().replace(/\s+/g, '');
      if (!rawName) continue;
      if (axisText.includes('x-axis') || axisText.includes('xaxis') || axisText === 'x') {
        result.x = rawName;
      } else if (axisText.includes('y-axis') || axisText.includes('yaxis') || axisText === 'y') {
        result.y = rawName;
      }
    }
    const lower = q.toLowerCase();
    const xMatch = lower.match(/x\s*-?\s*axis\s*[:=]\s*([^,;\n]+)/);
    if (xMatch && !result.x) result.x = xMatch[1].trim();
    const yMatch = lower.match(/y\s*-?\s*axis\s*[:=]\s*([^,;\n]+)/);
    if (yMatch && !result.y) result.y = yMatch[1].trim();
    
    // Detect "add X on secondary Y axis" or "X on secondary Y axis" pattern
    // Handle variations: "add PA nGRP on secondary Y axis", "PA nGRP on secondary Y axis please"
    const secondaryYMatch = lower.match(/(?:add\s+)?(.+?)\s+on\s+(?:the\s+)?secondary\s+y\s*axis(?:\s+please)?/i);
    if (secondaryYMatch) {
      let y2Var = secondaryYMatch[1].trim();
      // Remove trailing "please" or other common words
      y2Var = y2Var.replace(/\s+(please|now|then)$/i, '').trim();
      result.y2 = y2Var;
      console.log('✅ Detected secondary Y-axis request:', result.y2);
    }
    
    // Also check for "secondary Y axis: X" pattern
    const secondaryYColonMatch = lower.match(/secondary\s+y\s*axis\s*[:=]\s*([^,;\n]+)/);
    if (secondaryYColonMatch && !result.y2) {
      result.y2 = secondaryYColonMatch[1].trim();
      console.log('✅ Detected secondary Y-axis (colon format):', result.y2);
    }
    
    return result;
  };

  const { x: explicitXRaw, y: explicitYRaw, y2: explicitY2Raw } = parseExplicitAxes(question);
  const availableColumns = summary.columns.map(c => c.name);
  const explicitX = explicitXRaw ? findMatchingColumn(explicitXRaw, availableColumns) : null;
  const explicitY = explicitYRaw ? findMatchingColumn(explicitYRaw, availableColumns) : null;
  const explicitY2 = explicitY2Raw ? findMatchingColumn(explicitY2Raw, availableColumns) : null;
  
  console.log('📊 Parsed explicit axes:', { x: explicitX, y: explicitY, y2: explicitY2 });

  // Parse query for filters, aggregations, and other transformations
  // Use pre-parsed query if provided (from answerQuestion), otherwise parse here
  let parsedQuery: ParsedQuery | null = preParsedQuery ?? null;
  let transformationNotes: string[] = preTransformationNotes ?? [];
  let workingData = data;
  
  if (preParsedQuery && preTransformationNotes) {
    // Data already filtered at top level, use it directly
    workingData = data; // data is already the filtered workingData from answerQuestion
    console.log(`✅ Using pre-filtered data with notes: ${preTransformationNotes.join('; ')}`);
  } else if (!parsedQuery) {
    // Only parse if not already provided
    try {
      parsedQuery = await parseUserQuery(question, summary, chatHistory);
      console.log('🧠 Parsed query:', parsedQuery);
    } catch (error) {
      console.error('⚠️ Query parsing failed, continuing without structured filters:', error);
    }
  } else {
    console.log('🧠 Using pre-parsed query:', parsedQuery);
  }

  if (parsedQuery && !preParsedQuery) {
    // Only apply transformations if we parsed here (not pre-filtered)
    const { data: transformedData, descriptions } = applyQueryTransformations(data, summary, parsedQuery);
    transformationNotes = descriptions;
    if (transformedData.length > 0 || descriptions.length > 0) {
      workingData = transformedData;
    }
  }
  
  // Execute analytical query executor for analytical questions
  // This runs queries on CSV data and provides results to Azure AI
  // Reuse already parsed query to avoid duplicate parsing
  let analyticalQueryContext = '';
  try {
    const analyticalResult = await executeAnalyticalQuery(question, data, summary, chatHistory, parsedQuery);
    if (analyticalResult.isAnalytical && analyticalResult.queryResults) {
      console.log('✅ Analytical query executed, injecting results into AI prompt');
      analyticalQueryContext = generateQueryContextForAI(analyticalResult);
      // Use the query results data if available and we haven't already transformed
      if (analyticalResult.queryResults.data.length > 0 && !preParsedQuery) {
        workingData = analyticalResult.queryResults.data;
        transformationNotes.push(analyticalResult.queryResults.summary);
      }
    }
  } catch (error) {
    console.error('⚠️ Analytical query executor failed:', error);
    // Continue without analytical query context
  }
  
  const withNotes = <T extends { answer: string }>(result: T): T => {
    // Return result as-is without appending filters applied text
    return result;
  };
  
  // Detect simple bar plot requests: "bar plot for status and total" or "bar plot for column status"
  const detectSimpleBarPlot = (q: string): { xColumn: string | null; yColumn: string | null } | null => {
    const ql = q.toLowerCase();
    
    // Check if user explicitly mentions "bar plot" or "bar chart"
    const hasBarKeyword = /\b(bar\s+(plot|chart|graph)|barplot|barchart)\b/i.test(q);
    if (!hasBarKeyword) {
      return null;
    }
    
    console.log('🔍 Detecting simple bar plot request...');
    
    // Pattern 1: "bar plot for [X] and [Y]" or "bar plot for column [X]"
    const pattern1 = q.match(/\b(?:bar\s+(?:plot|chart|graph))\s+(?:for|of|with)\s+(?:column\s+)?([^\s]+(?:\s+[^\s]+)?)\s+(?:and|with)\s+([^\s]+(?:\s+[^\s]+)?)/i);
    if (pattern1) {
      const xRaw = pattern1[1].trim();
      const yRaw = pattern1[2].trim();
      const xCol = findMatchingColumn(xRaw, availableColumns);
      const yCol = findMatchingColumn(yRaw, availableColumns);
      
      if (xCol && yCol) {
        console.log(`✅ Detected bar plot: X="${xCol}", Y="${yCol}"`);
        return { xColumn: xCol, yColumn: yCol };
      }
    }
    
    // Pattern 2: "bar plot for [X]" - try to infer Y from context or use a numeric column
    const pattern2 = q.match(/\b(?:bar\s+(?:plot|chart|graph))\s+(?:for|of|with)\s+(?:column\s+)?([^\s]+(?:\s+[^\s]+)?)/i);
    if (pattern2) {
      const xRaw = pattern2[1].trim();
      const xCol = findMatchingColumn(xRaw, availableColumns);
      
      if (xCol) {
        // Try to find a numeric column mentioned in the question or use "total" if it exists
        let yCol: string | null = null;
        
        // Check if "total" is mentioned or exists
        if (summary.numericColumns.includes('total')) {
          yCol = 'total';
        } else if (summary.numericColumns.length > 0) {
          // Use first numeric column as default
          yCol = summary.numericColumns[0];
        }
        
        if (xCol && yCol) {
          console.log(`✅ Detected bar plot: X="${xCol}", Y="${yCol}" (inferred)`);
          return { xColumn: xCol, yColumn: yCol };
        }
      }
    }
    
    // Pattern 3: Extract two column names from the question
    // Look for mentions of "status" and "total" or similar patterns
    const columnMentions = availableColumns.filter(col => {
      const colLower = col.toLowerCase();
      return ql.includes(colLower);
    });
    
    if (columnMentions.length >= 2) {
      // Find one categorical and one numeric
      const categorical = columnMentions.find(col => !summary.numericColumns.includes(col));
      const numeric = columnMentions.find(col => summary.numericColumns.includes(col));
      
      if (categorical && numeric) {
        console.log(`✅ Detected bar plot from column mentions: X="${categorical}", Y="${numeric}"`);
        return { xColumn: categorical, yColumn: numeric };
      }
    }
    
    return null;
  };
  
  // Detect simple pie chart requests: "pie chart for status and total" or "pie chart for column status"
  const detectSimplePieChart = (q: string): { xColumn: string | null; yColumn: string | null } | null => {
    const ql = q.toLowerCase();
    
    // Check if user explicitly mentions "pie chart" or "pie graph"
    const hasPieKeyword = /\b(pie\s+(chart|graph|plot)|piechart)\b/i.test(q);
    if (!hasPieKeyword) {
      return null;
    }
    
    console.log('🔍 Detecting simple pie chart request...');
    
    // Pattern 1: "pie chart for [X] and [Y]" or "pie chart for column [X]"
    const pattern1 = q.match(/\b(?:pie\s+(?:chart|graph|plot))\s+(?:for|of|with)\s+(?:column\s+)?([^\s]+(?:\s+[^\s]+)?)\s+(?:and|with)\s+([^\s]+(?:\s+[^\s]+)?)/i);
    if (pattern1) {
      const xRaw = pattern1[1].trim();
      const yRaw = pattern1[2].trim();
      const xCol = findMatchingColumn(xRaw, availableColumns);
      const yCol = findMatchingColumn(yRaw, availableColumns);
      
      if (xCol && yCol) {
        console.log(`✅ Detected pie chart: X="${xCol}", Y="${yCol}"`);
        return { xColumn: xCol, yColumn: yCol };
      }
    }
    
    // Pattern 2: "pie chart for [X]" - try to infer Y from context or use a numeric column
    const pattern2 = q.match(/\b(?:pie\s+(?:chart|graph|plot))\s+(?:for|of|with)\s+(?:column\s+)?([^\s]+(?:\s+[^\s]+)?)/i);
    if (pattern2) {
      const xRaw = pattern2[1].trim();
      const xCol = findMatchingColumn(xRaw, availableColumns);
      
      if (xCol) {
        // Try to find a numeric column mentioned in the question or use "total" if it exists
        let yCol: string | null = null;
        
        // Check if "total" is mentioned or exists
        if (summary.numericColumns.includes('total')) {
          yCol = 'total';
        } else if (summary.numericColumns.length > 0) {
          // Use first numeric column as default
          yCol = summary.numericColumns[0];
        }
        
        if (xCol && yCol) {
          console.log(`✅ Detected pie chart: X="${xCol}", Y="${yCol}" (inferred)`);
          return { xColumn: xCol, yColumn: yCol };
        }
      }
    }
    
    // Pattern 3: Extract two column names from the question
    // Look for mentions of column names in the question
    const columnMentions = availableColumns.filter(col => {
      const colLower = col.toLowerCase();
      return ql.includes(colLower);
    });
    
    if (columnMentions.length >= 2) {
      // Find one categorical and one numeric
      const categorical = columnMentions.find(col => !summary.numericColumns.includes(col));
      const numeric = columnMentions.find(col => summary.numericColumns.includes(col));
      
      if (categorical && numeric) {
        console.log(`✅ Detected pie chart from column mentions: X="${categorical}", Y="${numeric}"`);
        return { xColumn: categorical, yColumn: numeric };
      }
    }
    
    return null;
  };
  
  // Check for simple bar plot request early
  const barPlotRequest = detectSimpleBarPlot(question);
  if (barPlotRequest && barPlotRequest.xColumn && barPlotRequest.yColumn) {
    console.log(`📊 Creating bar plot: ${barPlotRequest.xColumn} (X) vs ${barPlotRequest.yColumn} (Y)`);
    
    try {
      const barSpec: ChartSpec = {
        type: 'bar',
        title: `Bar Chart: ${barPlotRequest.yColumn} by ${barPlotRequest.xColumn}`,
        x: barPlotRequest.xColumn,
        y: barPlotRequest.yColumn,
        xLabel: barPlotRequest.xColumn,
        yLabel: barPlotRequest.yColumn,
        aggregate: 'sum', // Default to sum for bar charts
      };
      
      console.log('🔄 Processing bar chart data...');
      const barData = processChartData(workingData, barSpec);
      console.log(`✅ Bar chart data: ${barData.length} bars`);
      
      if (barData.length === 0) {
        return {
          answer: `I couldn't generate a bar chart. The data might be empty after filtering, or there might be an issue with the columns "${barPlotRequest.xColumn}" and "${barPlotRequest.yColumn}".`
        };
      }
      
      const insights = await generateChartInsights(barSpec, barData, summary, chatInsights);
      
      return withNotes({
        answer: `I've created a bar chart showing ${barPlotRequest.yColumn} grouped by ${barPlotRequest.xColumn}.`,
        charts: [{ ...barSpec, data: barData, keyInsight: insights.keyInsight }],
        insights: []
      });
    } catch (error) {
      console.error('❌ Error creating bar plot:', error);
      // Fall through to general processing
    }
  }
  
  // Check for simple pie chart request early
  const pieChartRequest = detectSimplePieChart(question);
  if (pieChartRequest && pieChartRequest.xColumn && pieChartRequest.yColumn) {
    console.log(`🥧 Creating pie chart: ${pieChartRequest.xColumn} (X) vs ${pieChartRequest.yColumn} (Y)`);
    
    try {
      // Ensure X column is not a date column for pie charts (unless explicitly requested)
      const isDateCol = summary.dateColumns.includes(pieChartRequest.xColumn);
      if (isDateCol) {
        console.warn(`⚠️ Pie chart requested with date column "${pieChartRequest.xColumn}". Finding categorical alternative...`);
        // Try to find a categorical column
        const categoricalCol = availableColumns.find(col => 
          !summary.dateColumns.includes(col) && 
          !summary.numericColumns.includes(col) &&
          col !== pieChartRequest.xColumn
        );
        
        if (categoricalCol) {
          console.log(`   Using categorical column "${categoricalCol}" instead`);
          pieChartRequest.xColumn = categoricalCol;
        } else {
          return {
            answer: `I can't create a pie chart using the date column "${pieChartRequest.xColumn}". Pie charts work best with categorical columns like status, category, or product type. Please specify a categorical column for the pie chart.`
          };
        }
      }
      
      const pieSpec: ChartSpec = {
        type: 'pie',
        title: `Pie Chart: ${pieChartRequest.yColumn} by ${pieChartRequest.xColumn}`,
        x: pieChartRequest.xColumn,
        y: pieChartRequest.yColumn,
        xLabel: pieChartRequest.xColumn,
        yLabel: pieChartRequest.yColumn,
        aggregate: 'sum', // Default to sum for pie charts
      };
      
      console.log('🔄 Processing pie chart data...');
      const pieData = processChartData(workingData, pieSpec);
      console.log(`✅ Pie chart data: ${pieData.length} segments`);
      
      if (pieData.length === 0) {
        return {
          answer: `I couldn't generate a pie chart. The data might be empty after filtering, or there might be an issue with the columns "${pieChartRequest.xColumn}" and "${pieChartRequest.yColumn}".`
        };
      }
      
      const insights = await generateChartInsights(pieSpec, pieData, summary, chatInsights);
      
      return withNotes({
        answer: `I've created a pie chart showing the distribution of ${pieChartRequest.yColumn} by ${pieChartRequest.xColumn}. The chart shows the top categories and their proportions.`,
        charts: [{ ...pieSpec, data: pieData, keyInsight: insights.keyInsight }],
        insights: []
      });
    } catch (error) {
      console.error('❌ Error creating pie chart:', error);
      // Fall through to general processing
    }
  }
  
  // If secondary Y-axis is requested, try to find the previous chart from chat history
  if (explicitY2) {
    console.log('🔍 Secondary Y-axis detected, looking for previous chart in chat history...');
    
    // Look for the most recent chart in chat history
    let previousChart: ChartSpec | null = null;
    for (let i = chatHistory.length - 1; i >= 0; i--) {
      const msg = chatHistory[i];
      if (msg.role === 'assistant' && msg.charts && msg.charts.length > 0) {
        // Find a line chart (most likely to have dual-axis)
        previousChart = msg.charts.find(c => c.type === 'line') || msg.charts[0];
        if (previousChart) {
          console.log('✅ Found previous chart:', previousChart.title);
          break;
        }
      }
    }
    
    // If we found a previous chart, add the secondary Y-axis to it
    if (previousChart && previousChart.type === 'line') {
      console.log('🔄 Adding secondary Y-axis to existing chart...');
      
      // Create updated chart spec with y2
      const updatedChart: ChartSpec = {
        ...previousChart,
        y2: explicitY2,
        y2Label: explicitY2,
        title: previousChart.title?.replace(/over.*$/i, '') || `${previousChart.y} and ${explicitY2} Trends`,
      };
      
      // Process the data
      const chartData = processChartData(workingData, updatedChart);
      console.log(`✅ Dual-axis line data: ${chartData.length} points`);
      
      if (chartData.length === 0) {
        return { answer: `No valid data points found. Please check that column "${explicitY2}" exists and contains numeric data.` };
      }
      
      const insights = await generateChartInsights(updatedChart, chartData, summary, chatInsights);
      
      return withNotes({
        answer: `I've added ${explicitY2} on the secondary Y-axis. The chart now shows ${previousChart.y} on the left axis and ${explicitY2} on the right axis.`,
        charts: [{
          ...updatedChart,
          data: chartData,
          keyInsight: insights.keyInsight,
        }],
      });
    }
    
    // If no previous chart found, but we have explicitY2, try to create a new dual-axis chart
    // We need to infer the primary Y-axis and X-axis
    if (!previousChart && explicitY2) {
      console.log('⚠️ No previous chart found, trying to create new dual-axis chart...');
      
      // Try to find the primary Y-axis from the question or use the first numeric column
      const primaryY = explicitY || summary.numericColumns[0];
      const xAxis = summary.dateColumns[0] || 
                    findMatchingColumn('Month', availableColumns) || 
                    findMatchingColumn('Date', availableColumns) ||
                    availableColumns[0];
      
      if (primaryY && explicitY2 && xAxis) {
        const dualAxisSpec: ChartSpec = {
          type: 'line',
          title: `${primaryY} and ${explicitY2} Trends Over Time`,
          x: xAxis,
          y: primaryY,
          y2: explicitY2,
          xLabel: xAxis,
          yLabel: primaryY,
          y2Label: explicitY2,
          aggregate: 'none',
        };
        
        const chartData = processChartData(workingData, dualAxisSpec);
        if (chartData.length > 0) {
          const insights = await generateChartInsights(dualAxisSpec, chartData, summary, chatInsights);
          return withNotes({
            answer: `I've created a line chart with ${primaryY} on the left axis and ${explicitY2} on the right axis.`,
            charts: [{
              ...dualAxisSpec,
              data: chartData,
              keyInsight: insights.keyInsight,
            }],
          });
        }
      }
    }
  }

  // Detect "add" queries that add multiple variables to an existing chart
  const detectAddQuery = (q: string): { variablesToAdd: string[]; previousChart: ChartSpec | null } | null => {
    const ql = q.toLowerCase();
    
    // Check if query mentions "add" and variable names
    const mentionsAdd = /\b(add|include|show|plot)\b/i.test(q);
    if (!mentionsAdd) {
      console.log('❌ detectAddQuery - does not mention add/include/show');
      return null;
    }
    
    console.log('🔍 detectAddQuery - checking for variables to add...');
    
    // Look for the most recent chart in chat history
    let previousChart: ChartSpec | null = null;
    for (let i = chatHistory.length - 1; i >= 0; i--) {
      const msg = chatHistory[i];
      if (msg.role === 'assistant' && msg.charts && msg.charts.length > 0) {
        previousChart = msg.charts.find(c => c.type === 'line' || c.type === 'scatter') || msg.charts[0];
        if (previousChart) {
          console.log('✅ Found previous chart:', previousChart.title);
          break;
        }
      }
    }
    
    if (!previousChart) {
      console.log('❌ detectAddQuery - no previous chart found');
      return null;
    }
    
    // Extract variable names from the query
    // Look for patterns like "add Jui, Dabur and Meril" or "add Jui, Dabur, Meril"
    const variablesToAdd: string[] = [];
    
    // Try to match variable names from available columns
    // Common patterns: "add X, Y and Z" or "add X, Y, Z"
    const addMatch = q.match(/\b(?:add|include|show|plot)\s+(.+)/i);
    if (addMatch) {
      const variablesText = addMatch[1];
      // Split by comma and "and"
      const parts = variablesText.split(/[,\s]+and\s+|[,\s]+/i).map(p => p.trim()).filter(p => p.length > 0);
      
      console.log('📝 Extracted variable parts:', parts);
      
      // Try to match each part to a column name
      for (const part of parts) {
        // Try exact match first
        let matched = findMatchingColumn(part, availableColumns);
        
        // If no match, try with common suffixes (nGRP Adstocked, etc.)
        if (!matched) {
          // Try adding common patterns
          const patterns = [
            `${part} nGRP Adstocked`,
            `${part} nGRP`,
            `Dabur ${part}`,
            `Meril ${part}`,
            `Jui ${part}`,
          ];
          
          for (const pattern of patterns) {
            matched = findMatchingColumn(pattern, availableColumns);
            if (matched) break;
          }
        }
        
        if (matched && summary.numericColumns.includes(matched)) {
          variablesToAdd.push(matched);
          console.log(`✅ Matched "${part}" to column: ${matched}`);
        } else {
          console.log(`⚠️ Could not match "${part}" to a numeric column`);
        }
      }
    }
    
    if (variablesToAdd.length === 0) {
      console.log('❌ detectAddQuery - no variables extracted');
      return null;
    }
    
    console.log('✅ detectAddQuery - extracted variables to add:', variablesToAdd);
    return { variablesToAdd, previousChart };
  };

  // Check for "trends in X over time" queries FIRST (before other detections)
  const detectTrendInQuery = (q: string): { variable: string | null; xAxis: string | null } | null => {
    const ql = q.toLowerCase();
    
    // Pattern: "trends in X over time" or "trends in X" or "X over time"
    const trendInMatch = q.match(/\b(?:trends?\s+in|trends?\s+for|analyze\s+trends?\s+in)\s+([a-zA-Z0-9_\s]+?)(?:\s+over\s+time|$)/i);
    const overTimeMatch = q.match(/([a-zA-Z0-9_\s]+?)\s+over\s+time/i);
    
    if (!trendInMatch && !overTimeMatch) {
      return null;
    }
    
    const variableRaw = trendInMatch ? trendInMatch[1].trim() : (overTimeMatch ? overTimeMatch[1].trim() : null);
    if (!variableRaw) {
      return null;
    }
    
    // Match variable to actual column
    const variable = findMatchingColumn(variableRaw, availableColumns);
    if (!variable || !summary.numericColumns.includes(variable)) {
      console.log(`⚠️ Could not match trend variable "${variableRaw}" to a numeric column`);
      return null;
    }
    
    // Find time/date column for X-axis
    const xAxis = summary.dateColumns[0] || 
                  findMatchingColumn('Month', availableColumns) || 
                  findMatchingColumn('Date', availableColumns) ||
                  findMatchingColumn('Time', availableColumns) ||
                  availableColumns[0];
    
    console.log(`✅ Detected trend query: variable="${variable}", xAxis="${xAxis}"`);
    return { variable, xAxis };
  };

  const trendInQuery = detectTrendInQuery(question);
  if (trendInQuery && trendInQuery.variable && trendInQuery.xAxis) {
    console.log('✅ Processing trend query:', trendInQuery);
    
    // Create line chart for trend
    const trendSpec: ChartSpec = {
      type: 'line',
      title: `Trend of ${trendInQuery.variable} Over Time`,
      x: trendInQuery.xAxis,
      y: trendInQuery.variable,
      xLabel: trendInQuery.xAxis,
      yLabel: trendInQuery.variable,
      aggregate: 'none',
    };
    
    console.log('🔄 Processing trend chart data...');
    const trendData = processChartData(workingData, trendSpec);
    console.log(`✅ Trend chart data: ${trendData.length} points`);
    
    if (trendData.length === 0) {
      return { 
        answer: `No valid data points found for trend chart. Please check that columns "${trendInQuery.xAxis}" and "${trendInQuery.variable}" contain valid data.` 
      };
    }
    
    // Calculate smart axis domains
    const smartDomains = calculateSmartDomainsForChart(
      trendData,
      trendSpec.x,
      trendSpec.y,
      undefined,
      {
        yOptions: { useIQR: true, paddingPercent: 5, includeOutliers: true },
      }
    );
    
    // Generate chart insights and full insights
    const chartInsights = await generateChartInsights(trendSpec, trendData, summary, chatInsights);
    
    // Generate full insights array
    let fullInsights: Insight[] = [];
    try {
      fullInsights = await generateInsights(trendData, summary);
      console.log(`✅ Generated ${fullInsights.length} insights for trend chart`);
    } catch (insightError) {
      console.error('⚠️ Failed to generate insights for trend chart:', insightError);
    }
    
    const answer = `I've created a trend line showing ${trendInQuery.variable} over time (${trendInQuery.xAxis}).`;
    
    return withNotes({
      answer,
      charts: [{
        ...trendSpec,
        data: trendData,
        ...smartDomains,
        keyInsight: chartInsights.keyInsight,
      }],
      insights: fullInsights,
    });
  }

  // Check for "add" queries first (before "both" detection)
  const addQuery = detectAddQuery(question);
  if (addQuery && addQuery.variablesToAdd.length > 0 && addQuery.previousChart) {
    console.log('✅ detectAddQuery matched! Variables to add:', addQuery.variablesToAdd);
    
    const previousChart = addQuery.previousChart;
    const variablesToAdd = addQuery.variablesToAdd;
    const primaryY = previousChart.y;
    const xAxis = previousChart.x;
    
    // For now, we'll create a chart with the first variable on y2
    // TODO: Extend schema to support multiple y2 variables
    // For now, we'll put all variables in the data and use the first one for y2
    const firstVariable = variablesToAdd[0];
    const allVariables = [primaryY, ...variablesToAdd];
    
    // Create dual-axis line chart with all variables on right axis
    const spec: ChartSpec = {
      type: 'line',
      title: `${primaryY} vs ${variablesToAdd.join(', ')}`,
      x: xAxis,
      y: primaryY,
      y2: firstVariable, // Use first variable for y2 (right axis) - for backward compatibility
      y2Series: variablesToAdd, // Store all variables for multi-series rendering
      xLabel: xAxis,
      yLabel: primaryY,
      y2Label: variablesToAdd.length === 1 ? firstVariable : `${variablesToAdd.join(', ')}`,
      aggregate: 'none',
    } as any;
    
    // Process data with all variables included
    console.log('🔄 Processing dual-axis line chart data with multiple variables...');
    
    // First, process with y2 to get the data structure
    const processed = processChartData(workingData, spec);
    console.log(`✅ Dual-axis line data: ${processed.length} points`);
    
    if (processed.length === 0) {
      const allCols = summary.columns.map(c => c.name);
      return { 
        answer: `No valid data points found. Please check that columns exist and contain numeric data. Available columns: ${allCols.join(', ')}` 
      };
    }
    
    // Add additional variables to the data
    // We'll include them in the data so the frontend can render them if extended
    const enrichedData = processed.map(row => {
      const enrichedRow = { ...row };
      // Add all additional variables to the row data
      for (const variable of variablesToAdd) {
        const matchedVar = findMatchingColumn(variable, availableColumns);
        if (matchedVar && data.length > 0) {
          // Find the corresponding value for this x value
          const xValue = row[xAxis];
          const dataRow = data.find(d => String(d[xAxis]) === String(xValue));
          if (dataRow && dataRow[matchedVar] !== undefined) {
            enrichedRow[matchedVar] = toNumber(dataRow[matchedVar]);
          }
        }
      }
      return enrichedRow;
    });
    
    // Store additional variables in a custom field (we'll need to extend schema later)
    // For now, we'll include them in the data and use y2Label to indicate multiple
    const insights = await generateChartInsights(spec, enrichedData, summary, chatInsights);
    
    const answer = variablesToAdd.length === 1
      ? `I've added ${firstVariable} on the secondary Y-axis. The chart now shows ${primaryY} on the left axis and ${firstVariable} on the right axis.`
      : `I've created a chart with ${primaryY} on the left axis and ${variablesToAdd.join(', ')} on the right axis.`;
    
    return withNotes({
      answer,
      charts: [{ 
        ...spec, 
        data: enrichedData, 
        keyInsight: insights.keyInsight, 
        // Store additional variables in a way the frontend can access
        // We'll use a custom property that won't break the schema
      } as any],
      insights: []
    });
  }

  // Detect "both" queries that refer to previous chart/conversation variables
  const detectBothQuery = (q: string): { var1: string | null; var2: string | null; x: string | null } | null => {
    const ql = q.toLowerCase();
    
    // Check if query mentions "both" and "trends" or "show"
    const mentionsBoth = /\b(both|them|they)\b/i.test(q);
    const mentionsTrends = /\b(trends?|show|display|plot|graph|chart)\b/i.test(q);
    
    if (!mentionsBoth || !mentionsTrends) {
      console.log('❌ detectBothQuery - does not mention both/trends');
      return null;
    }
    
    console.log('🔍 detectBothQuery - checking for previous chart variables...');
    
    // Look for the most recent chart and messages in chat history to extract variables
    let previousChart: ChartSpec | null = null;
    let previousUserMessage: string | null = null;
    let previousAssistantMessage: string | null = null;
    
    for (let i = chatHistory.length - 1; i >= 0; i--) {
      const msg = chatHistory[i];
      
      // Look for assistant messages with charts
      if (msg.role === 'assistant' && msg.charts && msg.charts.length > 0) {
        previousChart = msg.charts.find(c => c.type === 'line' || c.type === 'scatter') || msg.charts[0];
        if (previousChart) {
          console.log('✅ Found previous chart:', previousChart.title);
        }
      }
      
      // Also capture assistant message content (might mention both variables)
      if (msg.role === 'assistant' && msg.content && !previousAssistantMessage) {
        previousAssistantMessage = msg.content;
      }
      
      // Also look for user messages that might mention variables
      if (msg.role === 'user' && msg.content && !previousUserMessage) {
        previousUserMessage = msg.content;
      }
      
      // If we found a chart, we can break (but still want to capture messages)
      if (previousChart && previousAssistantMessage && previousUserMessage) {
        break;
      }
    }
    
    // Try to extract two variables from previous chart
    if (previousChart) {
      const var1 = previousChart.y;
      const var2 = (previousChart as any).y2 || null;
      const xAxis = previousChart.x;
      
      // If chart has y2, use y and y2
      if (var2) {
        console.log('✅ detectBothQuery - extracted from previous chart:', { var1, var2, x: xAxis });
        return { var1, var2, x: xAxis };
      }
      
      // If no y2, try to find second variable from previous messages
      // Check both assistant and user messages for mentioned variables
      const messagesToCheck = [
        previousAssistantMessage,
        previousUserMessage
      ].filter(Boolean) as string[];
      
      for (const message of messagesToCheck) {
        console.log('🔍 Looking for second variable in message:', message);
        
        // Try to extract variable names from message
        // Look for patterns like "PA TOM" and "PA nGRP Adstocked"
        const variablePatterns = availableColumns.filter(col => 
          message.toLowerCase().includes(col.toLowerCase())
        );
        
        if (variablePatterns.length >= 2) {
          // Find the two that match numeric columns
          const numericVars = variablePatterns.filter(v => summary.numericColumns.includes(v));
          if (numericVars.length >= 2) {
            // Use var1 from chart if it's in the list, otherwise use first two
            const var1Match = numericVars.find(v => v === var1) || numericVars[0];
            const var2Match = numericVars.find(v => v !== var1Match) || numericVars[1];
            console.log('✅ detectBothQuery - extracted from message:', { var1: var1Match, var2: var2Match, x: xAxis });
            return { var1: var1Match, var2: var2Match, x: xAxis };
          }
        }
      }
    }
    
    // If we have a previous user message, try to extract both variables from it
    if (previousUserMessage && !previousChart) {
      console.log('🔍 No previous chart, extracting from user message:', previousUserMessage);
      
      // Look for two variable names in the previous message
      const mentionedVars = availableColumns.filter(col => 
        previousUserMessage!.toLowerCase().includes(col.toLowerCase())
      );
      
      const numericVars = mentionedVars.filter(v => summary.numericColumns.includes(v));
      if (numericVars.length >= 2) {
        const xAxis = summary.dateColumns[0] || 
                     findMatchingColumn('Month', availableColumns) || 
                     findMatchingColumn('Date', availableColumns) ||
                     availableColumns[0];
        console.log('✅ detectBothQuery - extracted from user message:', { var1: numericVars[0], var2: numericVars[1], x: xAxis });
        return { var1: numericVars[0], var2: numericVars[1], x: xAxis };
      }
    }
    
    console.log('❌ detectBothQuery - could not extract two variables');
    return null;
  };

  // Check for "both" queries first (before vs/and detection)
  const bothQuery = detectBothQuery(question);
  if (bothQuery && bothQuery.var1 && bothQuery.var2) {
    console.log('✅ detectBothQuery matched! Result:', bothQuery);
    
    // Verify columns exist and are numeric
    const firstRow = data[0];
    if (!firstRow) {
      console.error('❌ No data rows available');
      return { answer: 'No data available to create charts. Please upload a data file first.' };
    }
    
    const var1 = bothQuery.var1;
    const var2 = bothQuery.var2;
    const xAxis = bothQuery.x || summary.dateColumns[0] || 
                 findMatchingColumn('Month', availableColumns) || 
                 findMatchingColumn('Date', availableColumns) ||
                 availableColumns[0];
    
    // Verify both are numeric
    if (!summary.numericColumns.includes(var1) || !summary.numericColumns.includes(var2)) {
      console.error(`❌ Variables not both numeric: ${var1}, ${var2}`);
      return { answer: `Both variables must be numeric for a dual-axis line chart. "${var1}" and "${var2}" are not both numeric.` };
    }
    
    // Create dual-axis line chart
    const spec: ChartSpec = {
      type: 'line',
      title: `Trends for ${var1} and ${var2} Over Time`,
      x: xAxis,
      y: var1,
      y2: var2,
      xLabel: xAxis,
      yLabel: var1,
      y2Label: var2,
      aggregate: 'none',
    } as any;
    
    console.log('🔄 Processing dual-axis line chart data...');
    const processed = processChartData(workingData, spec);
    console.log(`✅ Dual-axis line data: ${processed.length} points`);
    
    if (processed.length === 0) {
      const allCols = summary.columns.map(c => c.name);
      return { 
        answer: `No valid data points found. Please check that columns "${var1}" and "${var2}" exist and contain numeric data. Available columns: ${allCols.join(', ')}` 
      };
    }
    
    const insights = await generateChartInsights(spec, processed, summary, chatInsights);
    const answer = `I've created a line chart with ${var1} on the left axis and ${var2} on the right axis, plotted over ${xAxis}.`;
    
    return withNotes({
      answer,
      charts: [{ ...spec, data: processed, keyInsight: insights.keyInsight }],
      insights: []
    });
  }

  // Detect "vs" queries for two numeric variables - generate both scatter and line charts
  const detectVsQuery = (q: string): { var1: string | null; var2: string | null } | null => {
    console.log('🔍 Detecting vs query in:', q);
    
    // Match text before and after "vs" - capture everything up to the end or common stopping words
    const vsMatch = q.match(/(.+?)\s+vs\s+(.+?)(?:\s+on\s+|$)/i) ||
                   q.match(/(.+?)\s+vs\s+(.+)/i);
    
    if (!vsMatch) {
      console.log('❌ No vs match found');
      return null;
    }
    
    let var1Raw = vsMatch[1].trim();
    let var2Raw = vsMatch[2].trim();
    
    console.log('📝 Raw variables:', { var1Raw, var2Raw });
    
    // Remove common chart-related words from the beginning of var1
    var1Raw = var1Raw.replace(/^(?:can\s+you\s+)?(?:plot|graph|chart|show|display)\s+/i, '').trim();
    
    // Remove trailing words like "on two separate axes" from var2
    var2Raw = var2Raw.replace(/\s+(?:on|with|using|separate|axes|axis|chart|graph|plot).*$/i, '').trim();
    
    console.log('🧹 Cleaned variables:', { var1Raw, var2Raw });
    console.log('📊 Available columns:', availableColumns);
    console.log('🔢 Numeric columns:', summary.numericColumns);
    
    // Try to find matching columns - be flexible with matching
    const var1 = findMatchingColumn(var1Raw, availableColumns);
    const var2 = findMatchingColumn(var2Raw, availableColumns);
    
    console.log('🎯 Matched columns:', { var1, var2 });
    
    if (!var1 || !var2) {
      console.log('❌ Could not match columns');
      return null;
    }
    
    // Check if both are numeric
    const bothNumeric = summary.numericColumns.includes(var1) && summary.numericColumns.includes(var2);
    if (!bothNumeric) {
      console.log('❌ Not both numeric. var1 numeric:', summary.numericColumns.includes(var1), 'var2 numeric:', summary.numericColumns.includes(var2));
      return null;
    }
    
    console.log('✅ Valid vs query detected:', { var1, var2 });
    return { var1, var2 };
  };

  // Also check for "and" queries in generateGeneralAnswer (fallback if detectTwoSeriesLine didn't catch it)
  const detectAndQuery = (q: string): { var1: string | null; var2: string | null } | null => {
    console.log('🔍 Detecting "and" query in generateGeneralAnswer:', q);
    const ql = q.toLowerCase();
    
    // Must have "and" 
    if (!ql.includes(' and ')) {
      console.log('❌ No "and" found');
      return null;
    }
    
    // Check for "two separate axes" or "plot" or "line chart" to distinguish from other uses of "and"
    const wantsChart = /\b(two\s+separates?\s+axes?|separates?\s+axes?|dual\s+axis|plot|graph|chart|line)\b/i.test(q);
    if (!wantsChart) {
      console.log('❌ Does not want chart');
      return null;
    }
    
    const andMatch = q.match(/(.+?)\s+and\s+(.+)/i);
    if (!andMatch) {
      console.log('❌ No and match pattern found');
      return null;
    }
    
    let var1Raw = andMatch[1].trim();
    let var2Raw = andMatch[2].trim();
    
    // Clean up
    var1Raw = var1Raw.replace(/^(?:can\s+you\s+)?(?:plot|graph|chart|show|display)\s+/i, '').trim();
    var2Raw = var2Raw.replace(/\s+(?:on|with|using|separate|axes|axis|chart|graph|plot|over.*).*$/i, '').trim();
    
    console.log('📝 Cleaned "and" variables:', { var1Raw, var2Raw });
    
    const var1 = findMatchingColumn(var1Raw, availableColumns);
    const var2 = findMatchingColumn(var2Raw, availableColumns);
    
    console.log('🎯 Matched "and" columns:', { var1, var2 });
    
    if (!var1 || !var2) {
      console.log('❌ Could not match "and" columns');
      return null;
    }
    
    const bothNumeric = summary.numericColumns.includes(var1) && summary.numericColumns.includes(var2);
    if (!bothNumeric) {
      console.log('❌ "And" columns not both numeric');
      return null;
    }
    
    console.log('✅ Valid "and" query detected:', { var1, var2 });
    return { var1, var2 };
  };

  const andQuery = detectAndQuery(question);
  if (andQuery && andQuery.var1 && andQuery.var2) {
    console.log('🚀 Processing "and" query with dual-axis line chart:', andQuery);
    
    // Determine X-axis for line chart
    const lineChartX = summary.dateColumns[0] || 
                      findMatchingColumn('Month', availableColumns) || 
                      findMatchingColumn('Date', availableColumns) ||
                      findMatchingColumn('Week', availableColumns) ||
                      availableColumns[0];
    
    const wantsDualAxis = /\b(two\s+separates?\s+axes?|separates?\s+axes?|dual\s+axis)\b/i.test(question);
    
    // Create dual-axis line chart
    const lineSpec: ChartSpec = {
      type: 'line',
      title: `${andQuery.var1} and ${andQuery.var2} over ${lineChartX}`,
      x: lineChartX,
      y: andQuery.var1,
      y2: andQuery.var2,
      xLabel: lineChartX,
      yLabel: andQuery.var1,
      y2Label: andQuery.var2,
      aggregate: 'none',
    };
    
    console.log('🔄 Processing dual-axis line chart data...');
    const lineData = processChartData(workingData, lineSpec);
    console.log(`✅ Dual-axis line data: ${lineData.length} points`);
    
    if (lineData.length === 0) {
      return { answer: `No valid data points found for line chart. Please check that columns "${andQuery.var1}" and "${andQuery.var2}" contain numeric data.` };
    }
    
    const lineInsights = await generateChartInsights(lineSpec, lineData, summary, chatInsights);
    
    const charts: ChartSpec[] = [{
      ...lineSpec,
      data: lineData,
      keyInsight: lineInsights.keyInsight,
    }];
    
    const answer = wantsDualAxis
      ? `I've created a line chart with ${andQuery.var1} on the left axis and ${andQuery.var2} on the right axis, plotted over ${lineChartX}.`
      : `I've created a line chart showing ${andQuery.var1} and ${andQuery.var2} over ${lineChartX}.`;
    
    return withNotes({
      answer,
      charts,
    });
  }

  const vsQuery = detectVsQuery(question);
  
  // If "vs" query detected with two numeric variables, generate both scatter and line charts
  if (vsQuery && vsQuery.var1 && vsQuery.var2) {
    console.log('🚀 Processing vs query with variables:', vsQuery);
    
    // Verify columns exist in data
    const firstRow = data[0];
    if (!firstRow) {
      console.error('❌ No data rows available');
      return { answer: 'No data available to create charts. Please upload a data file first.' };
    }
    
    if (!firstRow.hasOwnProperty(vsQuery.var1)) {
      console.error(`❌ Column "${vsQuery.var1}" not found in data`);
      return { answer: `Column "${vsQuery.var1}" not found in the data. Available columns: ${availableColumns.join(', ')}` };
    }
    
    if (!firstRow.hasOwnProperty(vsQuery.var2)) {
      console.error(`❌ Column "${vsQuery.var2}" not found in data`);
      return { answer: `Column "${vsQuery.var2}" not found in the data. Available columns: ${availableColumns.join(', ')}` };
    }
    
    // Determine X-axis for line chart: prefer date column, otherwise use index or first variable
    const lineChartX = summary.dateColumns[0] || 
                      findMatchingColumn('Month', availableColumns) || 
                      findMatchingColumn('Date', availableColumns) ||
                      findMatchingColumn('Week', availableColumns) ||
                      availableColumns[0]; // fallback to first column
    
    console.log('📈 Line chart X-axis:', lineChartX);
    
    // Use explicit axes if provided, otherwise use detected variables
    const scatterX = explicitX || vsQuery.var1;
    const scatterY = explicitY || vsQuery.var2;
    
    console.log('📊 Scatter chart axes:', { scatterX, scatterY });
    
    // Create scatter chart spec
    const scatterSpec: ChartSpec = {
      type: 'scatter',
      title: `Scatter Plot of ${scatterX} vs ${scatterY}`,
      x: scatterX,
      y: scatterY,
      xLabel: scatterX,
      yLabel: scatterY,
      aggregate: 'none',
    };
    
    // Create line chart spec (dual Y-axis)
    const lineSpec: ChartSpec = {
      type: 'line',
      title: `${vsQuery.var1} and ${vsQuery.var2} over ${lineChartX}`,
      x: lineChartX,
      y: vsQuery.var1,
      y2: vsQuery.var2,
      xLabel: lineChartX,
      yLabel: vsQuery.var1,
      y2Label: vsQuery.var2,
      aggregate: 'none',
    };
    
    // Process both charts
    console.log('🔄 Processing scatter chart data...');
    const scatterData = processChartData(workingData, scatterSpec);
    console.log(`✅ Scatter data: ${scatterData.length} points`);
    
    console.log('🔄 Processing line chart data...');
    const lineData = processChartData(workingData, lineSpec);
    console.log(`✅ Line data: ${lineData.length} points`);
    
    if (scatterData.length === 0) {
      console.error('❌ Scatter chart has no data');
      return { answer: `No valid data points found for scatter plot. Please check that columns "${scatterX}" and "${scatterY}" contain numeric data.` };
    }
    
    if (lineData.length === 0) {
      console.error('❌ Line chart has no data');
      return { answer: `No valid data points found for line chart. Please check that columns "${vsQuery.var1}" and "${vsQuery.var2}" contain numeric data.` };
    }
    
    // Calculate smart domains for both charts
    const scatterDomains = calculateSmartDomainsForChart(
      scatterData,
      scatterSpec.x,
      scatterSpec.y,
      undefined,
      {
        yOptions: { useIQR: true, paddingPercent: 5, includeOutliers: true },
      }
    );
    
    const lineDomains = calculateSmartDomainsForChart(
      lineData,
      lineSpec.x,
      lineSpec.y,
      lineSpec.y2,
      {
        yOptions: { useIQR: true, paddingPercent: 5, includeOutliers: true },
        y2Options: lineSpec.y2 ? { useIQR: true, paddingPercent: 5, includeOutliers: true } : undefined,
      }
    );
    
    const scatterInsights = await generateChartInsights(scatterSpec, scatterData, summary, chatInsights);
    const lineInsights = await generateChartInsights(lineSpec, lineData, summary, chatInsights);
    
    const charts: ChartSpec[] = [
      {
        ...scatterSpec,
        data: scatterData,
        ...scatterDomains, // Add smart domains
        keyInsight: scatterInsights.keyInsight,
      },
      {
        ...lineSpec,
        data: lineData,
        ...lineDomains, // Add smart domains
        keyInsight: lineInsights.keyInsight,
      },
    ];
    
    console.log('✅ Successfully created both charts');
    const answer = `I've created both a scatter plot and a line chart comparing ${vsQuery.var1} and ${vsQuery.var2}. The scatter plot shows the relationship between the two variables, while the line chart shows their trends over ${lineChartX}.`;
    
    return withNotes({
      answer,
      charts,
    });
  }

  // Use more messages for better context (last 15 messages)
  // Truncate long messages to reduce token usage while preserving context
  const recentHistory = chatHistory
    .slice(-15)
    .map((msg) => {
      const content = msg.content || '';
      const truncated = content.length > 500 ? content.substring(0, 500) + '...' : content;
      return `${msg.role}: ${truncated}`;
    })
    .join('\n');
  
  // Limit total history context to 2000 characters to avoid token limits
  const historyContext = recentHistory.length > 2000 
    ? recentHistory.substring(0, 2000) + '...' 
    : recentHistory;
  
  // If we have a parsed query with aggregations, extract the aggregation column names for the AI
  let aggregationColumnHints = '';
  if (parsedQuery && parsedQuery.aggregations && parsedQuery.aggregations.length > 0) {
    const aggColumns = parsedQuery.aggregations.map(agg => {
      const columnName = agg.alias || `${agg.column}_${agg.operation}`;
      return `- ${columnName} (from ${agg.operation}(${agg.column}))`;
    });
    aggregationColumnHints = `\n\nIMPORTANT - Aggregated columns created from your query:\n${aggColumns.join('\n')}\nWhen creating charts, use these column names for the Y-axis, NOT the original column names.`;
  }

  // STEP 1: Detect conversational queries FIRST (before expensive RAG calls)
  // This handles greetings, casual chat, and non-data questions
  const questionLower = question.trim().toLowerCase();
  
  // Expanded conversational patterns - handle phrases, not just single words
  const conversationalPatterns = [
    // Greetings
    /^(hi|hello|hey|hiya|howdy|greetings|sup|what's up|whats up|wassup)$/i,
    /^(hi|hello|hey)\s+(there|you|everyone|all)$/i,
    /^how\s+(are\s+you|you\s+doing|is\s+it\s+going|things\s+going)/i,
    /^what's?\s+(up|new|good|happening)/i,
    /^how\s+(do\s+you\s+do|goes\s+it)/i,
    
    // Thanks
    /^(thanks?|thank\s+you|thx|ty|appreciate\s+it|much\s+appreciated)/i,
    /^(thanks?|thank\s+you)\s+(so\s+much|a\s+lot|very\s+much|tons)/i,
    
    // Casual responses
    /^(ok|okay|sure|yep|yeah|yup|alright|all\s+right|got\s+it|understood|perfect|great|awesome|cool|nice|good|sounds\s+good|sounds\s+great)$/i,
    /^(yes|no|nope|nah)\s*$/i,
    
    // Farewells
    /^(bye|goodbye|see\s+ya|see\s+you|later|talk\s+to\s+you\s+later|catch\s+you\s+later|gotta\s+go)/i,
    /^(have\s+a\s+good|have\s+a\s+nice)\s+(day|one|weekend)/i,
    
    // Politeness
    /^(please|pls|plz)$/i,
    /^(sorry|my\s+bad|oops|whoops)/i,
    
    // Questions about the bot
    /^(who\s+are\s+you|what\s+are\s+you|what\s+can\s+you\s+do|what\s+do\s+you\s+do)/i,
    /^(help|what\s+can\s+you\s+help|how\s+can\s+you\s+help)/i,
  ];
  
  const isPureConversation = conversationalPatterns.some(pattern => pattern.test(questionLower));
  
  // Handle pure conversational queries IMMEDIATELY (before RAG)
  if (isPureConversation) {
    // Use AI for more natural, context-aware responses to conversational queries
    // Enhanced to be more ChatGPT-like with comprehensive responses
    try {
      // Check if this is a simple greeting vs a general question
      const isSimpleChat = /^(hi|hello|hey|thanks|thank you|bye|goodbye|ok|okay|sure|yes|no)$/i.test(question.trim());
      
      // Build response guidelines based on question type
      let responseGuidelines = '';
      if (isSimpleChat) {
        responseGuidelines = '- For simple greetings/casual responses: Keep it warm, friendly, and brief (1-2 sentences). Be enthusiastic and engaging.';
      } else {
        responseGuidelines = `- For general questions or discussions: Provide comprehensive, detailed, and helpful responses. Explain concepts clearly, provide examples when relevant, and be thorough but not overwhelming.
- Structure longer responses with clear paragraphs if needed
- Use natural, conversational language - like talking to a knowledgeable friend
- If the question relates to data analysis, you can mention your data analysis capabilities
- If it's a general knowledge question, answer it fully and accurately
- Be helpful, accurate, and engaging in all responses`;
      }
      
      const conversationalPrompt = `You are an intelligent, helpful, and friendly AI assistant with expertise in data analysis. You're having a natural conversation with the user.

USER'S MESSAGE: "${question}"

${historyContext ? `CONVERSATION HISTORY:\n${historyContext}\n\nUse this conversation history to provide context-aware, natural responses. Reference previous topics when relevant.` : ''}

YOUR CAPABILITIES:
- You're primarily a data analyst assistant, but you can also answer general questions, explain concepts, provide insights, and engage in natural conversation
- You can discuss data analysis, statistics, machine learning, business intelligence, and related topics
- You can answer general knowledge questions, explain how things work, provide advice, and have meaningful conversations
- You maintain context from previous messages and reference them naturally when relevant

RESPONSE GUIDELINES:
${responseGuidelines}

- Maintain a warm, friendly, and professional tone
- Use emojis sparingly (1-2 max, only when appropriate)
- Be conversational and natural, not robotic
- If you don't know something, say so honestly and offer to help with what you can do

Respond naturally and helpfully to the user's message.`;

      const response = await openai.chat.completions.create({
        model: MODEL as string,
        messages: [
          {
            role: 'system',
            content: 'You are an intelligent, helpful, and friendly AI assistant with expertise in data analysis. You can answer general questions, explain concepts, provide insights, and engage in natural, flowing conversations. You maintain context from previous messages and provide comprehensive, helpful responses when needed.',
          },
          {
            role: 'user',
            content: conversationalPrompt,
          },
        ],
        temperature: 0.8, // Balanced temperature for natural but coherent responses
        max_tokens: isSimpleChat ? 150 : 800, // Longer responses for general questions
      });

      const answer = response.choices[0].message.content?.trim() || "Hi! I'm here to help you. What would you like to know?";
      return { answer };
    } catch (error) {
      console.error('Conversational response error, using fallback:', error);
      // Fallback responses
      const fallbackResponses: Record<string, string> = {
        'hi': "Hi there! 👋 I'm here to help you explore your data and answer your questions. What would you like to know?",
        'hello': "Hello! 👋 Ready to dive into your data or discuss anything else? Ask me anything!",
        'hey': "Hey! 👋 What can I help you discover today?",
        'how are you': "I'm doing great, thanks for asking! Ready to help you analyze your data or answer any questions. What would you like to explore?",
        'what\'s up': "Not much! Just here waiting to help you with your data analysis or any questions you might have. What can I show you?",
        'thanks': "You're welcome! Happy to help. Anything else you'd like to explore or discuss?",
        'thank you': "You're very welcome! Feel free to ask if you need anything else. I'm here to help!",
      };
      
      const response = fallbackResponses[questionLower] || "I'm here to help! I can assist with data analysis, answer general questions, explain concepts, and have meaningful conversations. What would you like to know?";
      return { answer: response };
    }
  }

  // RAG retrieval removed
  let retrievedContext: string = '';
  
  // Extract key topics and entities from conversation history for better context
  const conversationTopics = chatHistory
    .slice(-10)
    .map(msg => msg.content)
    .join(' ')
    .toLowerCase();
  
  // Extract mentioned columns/variables from history
  const mentionedColumns = summary.columns
    .map(c => c.name)
    .filter(col => conversationTopics.includes(col.toLowerCase()));
  
  // Create statistical summary for AI prompt (instead of sending raw data)
  let dataContext = '';
  if (requiredColumns && requiredColumns.length > 0 && data.length > 1000) {
    // For large datasets, use statistical summaries
    try {
      const { createStatisticalSummary, formatStatisticalSummary } = await import('./statisticalSummary.js');
      const statSummary = createStatisticalSummary(data, summary, requiredColumns);
      dataContext = formatStatisticalSummary(statSummary);
      console.log(`📊 Using statistical summary for AI prompt (${statSummary.columns.length} columns)`);
    } catch (error) {
      console.warn('⚠️ Failed to create statistical summary, using basic context:', error);
      dataContext = `- ${summary.rowCount} rows, ${summary.columnCount} columns\n- Numeric columns: ${summary.numericColumns.join(', ')}`;
    }
  } else {
    // For small datasets, use basic context
    dataContext = `- ${summary.rowCount} rows, ${summary.columnCount} columns\n- Numeric columns: ${summary.numericColumns.join(', ')}`;
  }
  
  const prompt = `You are an intelligent, helpful, and friendly AI assistant with expertise in data analysis. You're having a natural, flowing conversation with the user. You can answer questions about their data, explain concepts, provide insights, discuss general topics, and engage in meaningful conversations. Be warm, helpful, and engaging - like talking to a knowledgeable friend.

🚨 CRITICAL RULE - ABSOLUTE PRIORITY 🚨
NEVER ASK FOR CLARIFICATION WHEN THE USER ASKS A SPECIFIC ANALYTICAL QUESTION AND DATA IS AVAILABLE.
EXECUTE THE QUERY IMMEDIATELY AND RETURN ACTUAL RESULTS WITH VALUES.

CRITICAL - NEVER ASK FOR CLARIFICATION WHEN DATA IS AVAILABLE:
- If the user asks a specific analytical question (which, what, how many, show me) and data has been filtered/aggregated, you MUST answer with actual results
- DO NOT ask "Could you clarify what kind of chart you'd like to see?" - execute the analysis and return results
- DO NOT ask "Let me know, and I'd be happy to assist!" - just assist immediately with the data provided
- DO NOT ask "Just to clarify, when you say 'categories,' are you referring to..." - use the available columns to infer what they mean
- DO NOT ask "Just to clarify, are you looking to analyze or filter..." - if they mention a column name, use it directly
- When user mentions "category column" or "category", look for columns like "category", "Category", "product_category" in the available columns and use them
- When data is provided, use it directly to answer the question - no clarification needed
- If the question mentions a column name (even partially), match it to available columns and proceed - don't ask for confirmation

CRITICAL DATA OPERATION HANDLING - EXECUTE IMMEDIATELY:
- If the user asks for "aggregated value for category X", "total for category X", "aggregated column name value for the column category X", "what is the total value for men's fashion category", or similar aggregation queries with category filters:
  * These are DATA OPERATIONS - execute them directly, DO NOT ask for clarification
  * Extract the category value (e.g., "men's fashion") from patterns like:
    - "for the column category X"
    - "for category X"  
    - "for X category"
    - "for X"
  * Extract the aggregation column name if specified (e.g., "total", "qty_ordered"), otherwise default to "total" or first numeric column
  * Find the category column automatically (look for "category", "Category", "product_category", etc.)
  * Filter the data by the category value
  * Calculate sum, average, min, max, and count for the aggregation column
  * Return the results directly in a clear format like:
    "The aggregated value for the category '[category]' is:
    - Total (Sum): X.XX
    - Average: X.XX
    - Minimum: X.XX
    - Maximum: X.XX
    - Count: X records"
  * DO NOT ask "Could you clarify..." or "What kind of statistical analysis..." - just perform the operation

CURRENT QUESTION: ${question}

${historyContext ? `CONVERSATION HISTORY:\n${historyContext}\n\nIMPORTANT - Use this history to:
- Understand context and references (when user says "that", "it", "the chart", "the previous one", "the last thing", etc.)
- Remember what columns/variables were discussed: ${mentionedColumns.length > 0 ? mentionedColumns.join(', ') : 'none yet'}
- Maintain conversation flow and continuity - respond naturally to follow-ups
- Reference previous answers naturally ("As I mentioned before...", "Building on what we discussed...")
- Show you remember what was discussed before
- If they're asking a follow-up, acknowledge it naturally ("Sure!", "Absolutely!", "Let me show you that...")
- Match their tone - if they're casual, be casual; if they're formal, be professional` : ''}

DATA CONTEXT:
${dataContext}
${parsedQuery && parsedQuery.aggregations && parsedQuery.aggregations.length > 0 ? `\nAggregated columns: ${parsedQuery.aggregations.map(agg => agg.alias || `${agg.column}_${agg.operation}`).join(', ')}` : ''}
${aggregationColumnHints}
${retrievedContext}
${analyticalQueryContext}

AVAILABLE COLUMNS IN DATASET:
${summary.columns.map(c => `- ${c.name} [${c.type}]`).join('\n')}

CRITICAL COLUMN INFERENCE RULES - USE THESE DIRECTLY, NO QUESTIONS:
- When user says "category" or "categories" or "category column", AUTOMATICALLY use: ${summary.columns.filter(c => /category/i.test(c.name)).map(c => c.name).join(', ') || 'Look for any column containing "category" (case-insensitive)'}
- When user says "revenue" or "revenue growth", AUTOMATICALLY use: ${summary.numericColumns.filter(c => /total|revenue|value|sales/i.test(c)).slice(0, 5).join(', ') || 'Look for total, revenue, value, or sales columns'}
- When user says "completed or received orders" or mentions order status, AUTOMATICALLY use status column: ${summary.columns.filter(c => /status/i.test(c.name)).map(c => c.name).join(', ') || 'Look for status or order_status columns'}
- When user mentions a year like "2020", AUTOMATICALLY filter by date columns: ${summary.dateColumns.join(', ') || 'Look for date, Month, order_date columns'}
- DO NOT ask "Just to clarify, when you say 'categories'..." - you have the column list above, use it
- DO NOT ask "Just to clarify, are you looking to analyze..." - infer from the question and execute
- DO NOT ask "Thanks for your question! Just to clarify..." - execute immediately
- DO NOT ask "Could you let me know what you'd like to explore..." - the question is clear, execute it
- DO NOT ask "Happy to help!" and then ask for clarification - just help immediately
- Match column names case-insensitively and use fuzzy matching (partial matches are OK)
- If multiple columns match, use the most common one (e.g., "category" over "product_category" if both exist)
- When user clarifies "i am saying category column", they mean use the category column - don't ask again, just use it

${transformationNotes.length > 0 ? `\nIMPORTANT - DATA HAS BEEN FILTERED/AGGREGATED:\n${transformationNotes.join('\n')}\n\nThe data you see below has already been filtered and aggregated according to the user's query. Use these RESULTS directly to answer the question with ACTUAL VALUES.\n` : ''}

${workingData.length > 0 && workingData.length <= 100 ? `\nFILTERED/AGGREGATED DATA RESULTS (use these to answer the question):\n${JSON.stringify(workingData.slice(0, 50), null, 2)}\n\nCRITICAL: These are the ACTUAL RESULTS after filtering and aggregation. Use these values directly in your answer. DO NOT describe "how you would approach it" - use these results NOW.\n\nFOR TIME SERIES QUESTIONS (month-over-month growth, trends, etc.):\n- If the data is grouped by month/category, calculate growth rates between consecutive months\n- Identify which categories/items showed consistent positive growth\n- Return the actual category names and their growth patterns\n- DO NOT ask for clarification - execute the analysis and show results\n\nFOR "ABOVE AVERAGE" OR "BELOW AVERAGE" QUERIES:\n- If user asks "which months/categories had X above the average" or "above the yearly monthly average":\n  1. If data is already grouped by month/category with aggregated values, use it directly\n  2. Calculate the average: sum all aggregated values and divide by count of groups\n  3. Filter rows where aggregated_value > average (or < average for "below")\n  4. Return the actual months/categories that meet the criteria with their values\n- Example: "Which months had revenue above the yearly monthly average?":\n  * If data shows monthly totals: [Jan: 1000, Feb: 1200, Mar: 800, ...]\n  * Calculate: average = (1000 + 1200 + 800 + ...) / 12\n  * Filter: months where monthly_total > average\n  * Return: "The months with revenue above the yearly monthly average are: [list months with values]"\n- Example: "Which months had total revenue above the yearly monthly average, but only from orders with discounts below 10%":\n  * The data has already been filtered (discount < 10%) and aggregated by month\n  * Calculate the average of all monthly totals\n  * Filter months where monthly_total > average\n  * Return the actual months with their revenue values\n- Use the data provided directly - don't ask to rephrase\n- If the data is already filtered and aggregated, use those results to calculate averages and comparisons` : workingData.length > 100 ? `\nFILTERED/AGGREGATED DATA RESULTS (first 20 rows, use these to answer):\n${JSON.stringify(workingData.slice(0, 20), null, 2)}\n\nTotal rows in filtered/aggregated result: ${workingData.length}\n\nCRITICAL: These are the ACTUAL RESULTS after filtering and aggregation. Use these values directly in your answer. DO NOT describe "how you would approach it" - use these results NOW.\n\nFOR TIME SERIES QUESTIONS (month-over-month growth, trends, etc.):\n- If the data is grouped by month/category, calculate growth rates between consecutive months\n- Identify which categories/items showed consistent positive growth\n- Return the actual category names and their growth patterns\n- DO NOT ask for clarification - execute the analysis and show results\n\nFOR "ABOVE AVERAGE" OR "BELOW AVERAGE" QUERIES:\n- If user asks "which months/categories had X above the average" or "above the yearly monthly average":\n  1. If data is already grouped by month/category with aggregated values, use it directly\n  2. Calculate the average: sum all aggregated values and divide by count of groups\n  3. Filter rows where aggregated_value > average (or < average for "below")\n  4. Return the actual months/categories that meet the criteria with their values\n- Example: "Which months had revenue above the yearly monthly average?":\n  * If data shows monthly totals: [Jan: 1000, Feb: 1200, Mar: 800, ...]\n  * Calculate: average = (1000 + 1200 + 800 + ...) / 12\n  * Filter: months where monthly_total > average\n  * Return: "The months with revenue above the yearly monthly average are: [list months with values]"\n- Example: "Which months had total revenue above the yearly monthly average, but only from orders with discounts below 10%":\n  * The data has already been filtered (discount < 10%) and aggregated by month\n  * Calculate the average of all monthly totals\n  * Filter months where monthly_total > average\n  * Return the actual months with their revenue values\n- Use the data provided directly - don't ask to rephrase\n- If the data is already filtered and aggregated, use those results to calculate averages and comparisons` : ''}

CONVERSATION STYLE - CRITICAL:
- Be NATURALLY conversational - like you're talking to a friend, not a robot
- Use contractions: "I've", "you're", "that's", "it's" - makes it feel human
- Vary your responses - don't use the same phrases repeatedly
- Show personality: be enthusiastic, helpful, and genuinely interested
- Reference previous parts naturally: "As we saw earlier...", "Remember when we looked at...", "Building on that..."
- If they ask a follow-up, acknowledge it: "Sure!", "Absolutely!", "Great question!", "Let me show you..."
- Use natural transitions: "So...", "Now...", "Here's the thing...", "Actually..."
- Ask clarifying questions if needed: "Are you looking for...?", "Do you mean...?"
- Match their energy - if they're excited, be excited; if they're casual, be casual
- Don't be overly formal - use everyday language

RESPONSE GUIDELINES:
- CRITICAL: If the question requires data analysis (filtering, aggregation, calculations, comparisons), you MUST execute the query and return ACTUAL RESULTS with VALUES, not just describe the approach
- When a question asks "which", "what", "how many", "show me" with specific criteria, you MUST:
  1. Use the parsed query filters and aggregations that have been applied to the data
  2. Return the actual results with specific values, numbers, and data
  3. DO NOT just describe "how you would approach it" - EXECUTE and SHOW RESULTS
  4. Present results clearly: list the categories, values, counts, etc. that match the criteria
- If the question is about the user's data: Analyze it, provide insights with actual values, and generate charts if requested
- If the question is general knowledge or conceptual: Answer it comprehensively and helpfully, even if it's not directly about their data
- If the question requests a chart or visualization: Generate appropriate chart specifications
- If the question is conversational or exploratory: Engage naturally and provide thoughtful responses
- Always be helpful, accurate, and engaging regardless of the question type

CRITICAL EXECUTION RULES - EXECUTE IMMEDIATELY, NO CLARIFICATION:
- When user asks "Which categories generated more than X in revenue in Y year, considering only SKUs that sold at least Z units":
  * DO NOT say "Here's how I'd approach it" or "I'll crunch those numbers"
  * DO NOT ask "Could you clarify what kind of chart you'd like to see?"
  * DO execute the query using the filtered/aggregated data provided
  * DO return the actual category names and their revenue values
  * DO show the results immediately: "The categories that meet your criteria are: [list with values]"

- When user asks about TIME SERIES ANALYSIS (month-over-month growth, trends, consistent growth, etc.):
  * DO NOT ask "Just to clarify, when you say 'categories,' are you referring to..." - use the available columns to find the category column
  * DO NOT ask "Just to clarify, are you looking to analyze or filter..." - execute the analysis immediately
  * DO NOT ask "Could you clarify what kind of chart you'd like to see?" or "Let me know, and I'd be happy to assist!"
  * DO NOT ask for ANY clarification - the question is clear, execute it immediately
  * DO infer column names from context:
    - "category" or "categories" → look for "category", "Category", "product_category" columns
    - "revenue" or "revenue growth" → look for "total", "revenue", "value", "sales" columns
    - "completed or received orders" → look for "status" column and filter for "completed", "received"
  * DO execute the time series analysis using the filtered/aggregated data provided
  * DO calculate month-over-month growth rates if the data is grouped by month and category
  * DO identify which categories/items showed consistent growth (positive growth in consecutive months)
  * DO return the actual results: list the categories/items that meet the criteria with their growth rates
  * Example: "The categories that showed consistent month-over-month revenue growth in 2020 are:\n- Category A: Showed positive growth in 8 out of 12 months (Jan: +5%, Feb: +3%, Mar: +7%...)\n- Category B: Showed positive growth in 10 out of 12 months (Jan: +2%, Feb: +4%, Mar: +6%...)"
  * If the data is grouped by month and category, analyze the growth pattern for each category across months
  * Calculate: (current_month - previous_month) / previous_month * 100 for each category-month pair
  * Identify categories where growth was positive in most consecutive months
  * Use the available columns list to match column names - don't ask which column to use

- When user asks COMPLEX QUERIES with multiple conditions (e.g., "Which months had total revenue above the yearly monthly average, but only from orders with discounts below 10%"):
  * DO NOT give up or ask to rephrase - execute the query step by step using the data provided
  * DO NOT say "I encountered an issue" or "Could you rephrase" - process the query with available data
  * The data has likely already been filtered (discount < 10%) and aggregated by month
  * Break down the query:
    1. Check if data is already filtered and aggregated - if so, use it directly
    2. If data shows monthly totals (e.g., [{Month: "Jan 2020", total_sum: 1000}, {Month: "Feb 2020", total_sum: 1200}, ...]):
       - Calculate average: sum all total_sum values / number of months
       - Filter: months where total_sum > average
       - Return: list of months with their revenue values
    3. If data needs further processing, use the provided data to calculate
  * Use the filtered/aggregated data provided to perform these calculations
  * If data is grouped by month, calculate: average = sum(all monthly totals) / number of months
  * Then filter: months where monthly_total > average
  * Return actual results: list the months/categories that meet all criteria with their values
  * Example: "The months that had total revenue above the yearly monthly average (considering only orders with discounts below 10%) are:\n- January 2020: ₹X (yearly monthly average: ₹Y)\n- March 2020: ₹X (yearly monthly average: ₹Y)..."
  * If you have the data, use it - don't ask to rephrase
  * NEVER return "I encountered an issue processing your request" - always try to process with available data
- When user asks questions with filters and aggregations, the data has already been filtered/aggregated for you - use those results directly
- Present results in a clear, structured format with actual numbers and values
- If results are empty, say so clearly: "No categories meet the specified criteria"
- NEVER ask for clarification when the question is clear and data is available - execute immediately and return results
- NEVER say "I encountered an issue" or "Could you rephrase" - if data is available, use it to answer the question

🚨 CRITICAL: IF QUERY RESULTS ARE PROVIDED ABOVE (marked with "QUERY RESULTS FOR ANALYTICAL QUESTION"):
- DO NOT generate a chart - answer the question directly using the query results
- DO NOT say "No valid data points found" - the query has been executed and results are above
- DO NOT try to create a trend chart or any visualization - use the provided query results
- ANSWER DIRECTLY with the actual values from the query results
- If the user asks "which region/category generated more than X", list the regions/categories from the query results
- Format your answer clearly: "The regions that meet the criteria are: [list with values from query results]"

If the question requests a chart or visualization AND NO QUERY RESULTS ARE PROVIDED, generate appropriate chart specifications. Otherwise, provide a helpful answer with ACTUAL RESULTS and VALUES.

CHART GUIDELINES:
- You can use ANY column (categorical or numeric) for x or y
- Pie charts: Use categorical column for x, numeric column for y, aggregate "sum" or "count"
  - IMPORTANT: If user explicitly asks for pie chart "across months", "by month", or "for [variable] across [date column]", use the date column for x-axis and set aggregate to "sum"
- Bar charts: Can use categorical or numeric for x, numeric for y
- Line/Area: Typically numeric or date for x, numeric for y
- Scatter: Numeric for both x and y
- x and y must be single column names (strings), NOT arrays

CRITICAL FOR CORRELATION CHARTS:
- If generating correlation charts, NEVER modify correlation values
- Use EXACT correlation values as calculated (positive/negative)
- Do NOT convert negative correlations to positive or vice versa
- Correlation values must preserve their original sign

CONVERSATION MEMORY:
${mentionedColumns.length > 0 ? `- Previously discussed columns: ${mentionedColumns.join(', ')}` : ''}
- Remember user's interests and preferences from the conversation
- If user asks about something mentioned before, show you remember

Output JSON:
{
  "answer": "your detailed, conversational answer that references previous topics when relevant",
  "charts": [{"type": "...", "title": "...", "x": "...", "y": "...", "aggregate": "..."}] or null,
  "generateInsights": true or false
}`;

  const response = await openai.chat.completions.create({
    model: MODEL as string,
      messages: [
        {
          role: 'system',
          content: `You are an intelligent, helpful, and friendly AI assistant with expertise in data analysis. You can answer questions about data, explain concepts, provide insights, discuss general topics, and engage in natural, flowing conversations. You're having a natural conversation with the user.

🚨 ABSOLUTE RULE - NEVER ASK FOR CLARIFICATION:
When a user asks a specific analytical question (which, what, how many, show me) and data/columns are available, you MUST execute the query immediately and return actual results. DO NOT ask "Just to clarify..." or "Could you let me know..." - infer from context and execute.

CRITICAL DATA OPERATION RULES:
- If user asks for "aggregated value for category X", "total for category X", "aggregated column name value for the column category X", or similar:
  * These are DATA OPERATIONS - execute them directly using regex to extract:
    - Category value from "for the column category X", "for category X", or "for X"
    - Aggregation column from "aggregated column name X" or default to "total"
  * Filter data by category and calculate sum, average, min, max, count
  * Return results immediately - DO NOT ask for clarification
  * DO NOT say "Could you clarify..." or "What kind of statistical analysis..."
        
CRITICAL CONVERSATION RULES:
- Be NATURALLY conversational - like talking to a friend, not a robot
- Use contractions and everyday language: "I've", "you're", "that's", "it's", "here's"
- Vary your responses - don't repeat the same phrases
- Show personality: be enthusiastic, helpful, genuinely interested
- Reference previous conversation naturally: "As we saw...", "Remember when...", "Building on that..."
- Acknowledge follow-ups warmly: "Sure!", "Absolutely!", "Great question!", "Let me show you..."
- Use natural transitions: "So...", "Now...", "Here's the thing...", "Actually..."
- Match their tone - casual or formal, match it
- Ask clarifying questions when needed: "Are you looking for...?", "Do you mean...?"
- Don't be overly formal - use everyday, natural language

TECHNICAL RULES:
- Column names (x, y) must be strings, not arrays
- Never modify correlation values - preserve their original positive/negative signs
- If the user is just chatting, respond naturally without forcing charts`,
      },
      {
        role: 'user',
        content: prompt,
      },
    ],
    response_format: { type: 'json_object' },
    temperature: 0.85, // Higher temperature for more natural, varied, human-like responses
    max_tokens: 1200, // Increased for more detailed conversational responses
  });

  const content = response.choices[0].message.content || '{"answer": "I cannot answer that question."}';

  try {
    const result = JSON.parse(content);

    let processedCharts: ChartSpec[] | undefined;

    // Only include charts if user explicitly requested them
    if (wantsCharts && result.charts && Array.isArray(result.charts)) {
      // Check if user explicitly wants pie chart across months/dates
      const questionLower = question.toLowerCase();
      const wantsPieAcrossMonths = /\bpie\s+chart.*(?:across|by|for).*(?:month|date|time)\b/i.test(question) ||
                                   /\bpie\s+chart.*(?:month|date|time).*(?:across|by|for)\b/i.test(question);
      
      // Sanitize chart specs
      const sanitized = result.charts.map((spec: any) => {
        let x = spec.x;
        let y = spec.y;
        
        if (Array.isArray(x)) x = x[0];
        if (Array.isArray(y)) y = y[0];
        if (typeof x === 'object' && x !== null) x = x.name || x.value || String(x);
        if (typeof y === 'object' && y !== null) y = y.name || y.value || String(y);
        
        // For pie charts when user explicitly asks "across months", use date column and ensure aggregation
        if (spec.type === 'pie' && wantsPieAcrossMonths && summary.dateColumns.length > 0) {
          const dateCol = summary.dateColumns[0] || findMatchingColumn('Month', availableColumns) || findMatchingColumn('Date', availableColumns);
          if (dateCol) {
            console.log(`   Pie chart requested across months - using date column "${dateCol}" for X-axis`);
            x = dateCol;
            // Ensure aggregate is set to 'sum' if not already specified
            if (!spec.aggregate || spec.aggregate === 'none') {
              spec.aggregate = 'sum';
            }
          }
        }
        
        // Apply explicit axis overrides from the question if provided
        const finalX = explicitX || String(x || '');
        const finalY = explicitY || String(y || '');

        // Sanitize aggregate field to only allow valid enum values
        let aggregate = spec.aggregate || 'none';
        // For pie charts, default to 'sum' if not specified (especially for date-based grouping)
        if (spec.type === 'pie' && aggregate === 'none') {
          aggregate = 'sum';
        }
        // For bar charts, default to 'sum' if not specified (needed for categorical X with numeric Y)
        if (spec.type === 'bar' && aggregate === 'none') {
          aggregate = 'sum';
        }
        const validAggregates = ['sum', 'mean', 'count', 'none'];
        if (!validAggregates.includes(aggregate)) {
          console.warn(`⚠️ Invalid aggregate value "${aggregate}", defaulting to "sum" for pie/bar charts or "none" for others`);
          aggregate = (spec.type === 'pie' || spec.type === 'bar') ? 'sum' : 'none';
        }

        return {
          type: spec.type,
          title: spec.title || 'Chart',
          x: finalX,
          y: finalY,
          aggregate: aggregate,
        };
      }).filter((spec: any) => 
        spec.type && spec.x && spec.y &&
        ['line', 'bar', 'scatter', 'pie', 'area'].includes(spec.type)
      );

      const chartsResult = await Promise.all(sanitized.map(async (spec: ChartSpec) => {
        console.log(`🔍 Processing chart: "${spec.title}"`);
        console.log(`   Original spec: x="${spec.x}", y="${spec.y}", aggregate="${spec.aggregate}"`);
        console.log(`   Working data rows: ${workingData.length}`);
        
        // If we have aggregations in the parsed query, try to match the y-axis to aggregated columns
        if (parsedQuery && parsedQuery.aggregations && parsedQuery.aggregations.length > 0 && workingData.length > 0) {
          const availableColumns = Object.keys(workingData[0]);
          console.log(`   Available columns after aggregation: [${availableColumns.join(', ')}]`);
          
          // Check if the x-axis column exists (should be in groupBy)
          const xColumnExists = availableColumns.some(col => 
            col === spec.x || col.toLowerCase() === spec.x.toLowerCase()
          );
          
          if (!xColumnExists && parsedQuery.groupBy && parsedQuery.groupBy.length > 0) {
            // Try to match x-axis to groupBy columns
            const matchedX = parsedQuery.groupBy.find(gbCol => 
              gbCol.toLowerCase() === spec.x.toLowerCase() ||
              spec.x.toLowerCase().includes(gbCol.toLowerCase()) ||
              gbCol.toLowerCase().includes(spec.x.toLowerCase())
            );
            
            if (matchedX && availableColumns.includes(matchedX)) {
              console.log(`   ✅ Matched X-axis from "${spec.x}" to "${matchedX}"`);
              spec.x = matchedX;
              spec.xLabel = matchedX;
            }
          }
          
          // Check if the y-axis column exists in the data (exact match or case-insensitive)
          const yColumnExists = availableColumns.some(col => 
            col === spec.y || col.toLowerCase() === spec.y.toLowerCase()
          );
          
          console.log(`   Y-axis column "${spec.y}" exists: ${yColumnExists}`);
          
          // If y-axis column doesn't exist, try to find the aggregated column
          if (!yColumnExists) {
            console.log(`   Looking for aggregated column matching "${spec.y}"...`);
            let foundMatch = false;
            
            // Normalize the spec.y for better matching (remove spaces, handle variations)
            const normalizedSpecY = spec.y.toLowerCase().replace(/\s+/g, '').replace(/[_-]/g, '');
            
            // First, try to match against aggregated columns directly
            for (const agg of parsedQuery.aggregations) {
              const aggColumnName = agg.alias || `${agg.column}_${agg.operation}`;
              console.log(`   Checking aggregation: ${agg.column} -> ${aggColumnName}`);
              
              // Check if this aggregated column exists in available columns
              const exactMatch = availableColumns.find(col => 
                col === aggColumnName || col.toLowerCase() === aggColumnName.toLowerCase()
              );
              
              if (exactMatch) {
                // Normalize for comparison
                const normalizedAggColumn = agg.column.toLowerCase().replace(/\s+/g, '').replace(/[_-]/g, '');
                const normalizedAggColumnName = exactMatch.toLowerCase().replace(/\s+/g, '').replace(/[_-]/g, '');
                
                // Check if the original column name matches spec.y (with fuzzy matching)
                const columnMatches = 
                    normalizedAggColumn === normalizedSpecY ||
                    normalizedSpecY.includes(normalizedAggColumn) ||
                    normalizedAggColumn.includes(normalizedSpecY) ||
                    // Also check if spec.y contains key terms from the aggregated column
                    (normalizedSpecY.includes('adstock') && normalizedAggColumn.includes('adstock')) ||
                    (normalizedSpecY.includes('grp') && normalizedAggColumn.includes('grp')) ||
                    (normalizedSpecY.includes('total') && normalizedAggColumnName.includes('sum')) ||
                    // Match if both contain same key business terms
                    (normalizedSpecY.includes('adstock') && normalizedSpecY.includes('grp') && 
                     normalizedAggColumn.includes('adstock') && normalizedAggColumn.includes('grp'));
                
                if (columnMatches) {
                  console.log(`   ✅ Match found! Updating chart y-axis from "${spec.y}" to "${exactMatch}"`);
                  spec.y = exactMatch;
                  spec.yLabel = exactMatch;
                  foundMatch = true;
                  break;
                }
              }
            }
            
            // If still no match, try fuzzy matching against all available columns
            if (!foundMatch) {
              const groupByColumns = new Set(parsedQuery.groupBy || []);
              
              // Extract key terms from spec.y
              const keyTerms: string[] = [];
              const importantTerms = ['adstock', 'grp', 'reach', 'tom', 'total', 'sum', 'pangrp', 'ngrp'];
              for (const term of importantTerms) {
                if (normalizedSpecY.includes(term)) {
                  keyTerms.push(term);
                }
              }
              
              // Try to find a column that contains matching key terms
              const fuzzyMatch = availableColumns.find(col => {
                if (groupByColumns.has(col)) return false;
                const normalizedCol = col.toLowerCase().replace(/\s+/g, '').replace(/[_-]/g, '');
                
                // If we have key terms, check if column contains at least one matching term
                if (keyTerms.length > 0) {
                  return keyTerms.some(term => normalizedCol.includes(term));
                }
                
                // Fallback: check if column contains any common business terms
                return normalizedCol.includes('adstock') || normalizedCol.includes('grp') || normalizedCol.includes('sum');
              });
              
              if (fuzzyMatch) {
                console.log(`   ✅ Fuzzy match found! Updating chart y-axis from "${spec.y}" to "${fuzzyMatch}"`);
                spec.y = fuzzyMatch;
                spec.yLabel = fuzzyMatch;
                foundMatch = true;
              }
            }
            
            // Final fallback: use the first aggregated column (not in groupBy)
            if (!foundMatch && availableColumns.length > 0) {
              const groupByColumns = new Set(parsedQuery.groupBy || []);
              
              // First try: find any column that matches an aggregation pattern
              let aggregatedCol = availableColumns.find(col => {
                if (groupByColumns.has(col)) return false;
                // Check if it matches aggregation naming pattern (column_operation or has _sum, _mean, etc.)
                return col.includes('_sum') || col.includes('_mean') || col.includes('_avg') || 
                       col.includes('_count') || parsedQuery.aggregations!.some(agg => {
                  const aggName = agg.alias || `${agg.column}_${agg.operation}`;
                  return col === aggName || col.toLowerCase() === aggName.toLowerCase();
                });
              });
              
              // If still no match, just use the first non-groupBy column
              if (!aggregatedCol) {
                aggregatedCol = availableColumns.find(col => !groupByColumns.has(col));
              }
              
              if (aggregatedCol) {
                console.log(`   ⚠️ Using fallback aggregated column: "${aggregatedCol}"`);
                spec.y = aggregatedCol;
                spec.yLabel = aggregatedCol;
                foundMatch = true;
              } else {
                console.warn(`   ❌ Could not find any aggregated column to use for y-axis`);
                console.warn(`   Available columns: [${availableColumns.join(', ')}]`);
                console.warn(`   GroupBy columns: [${Array.from(groupByColumns).join(', ')}]`);
              }
            }
          }
        } else if (workingData.length === 0) {
          console.warn(`   ⚠️ No data available - chart will be empty`);
          console.warn(`   This might mean the filter removed all rows or aggregation failed`);
        }
        
        console.log(`   Final chart spec: x="${spec.x}", y="${spec.y}", aggregate="${spec.aggregate}"`);
        const processedData = processChartData(workingData, spec);
        console.log(`   Processed data rows: ${processedData.length}`);
        
        // If no data was processed, provide a helpful error message
        if (processedData.length === 0) {
          const allCols = summary.columns.map(c => c.name);
          const xExists = allCols.some(c => c.toLowerCase() === spec.x.toLowerCase());
          const yExists = allCols.some(c => c.toLowerCase() === spec.y.toLowerCase());
          
          let errorMsg = `No valid data points found. `;
          if (!xExists) {
            errorMsg += `Column "${spec.x}" not found. `;
          }
          if (!yExists) {
            errorMsg += `Column "${spec.y}" not found. `;
          }
          if (xExists && yExists) {
            errorMsg += `Please check that "${spec.y}" contains numeric data that can be aggregated. `;
          }
          errorMsg += `Available columns: ${allCols.join(', ')}`;
          
          console.warn(`⚠️ Chart processing failed: ${errorMsg}`);
          return null; // Return null to indicate failure
        }
        
        const chartInsights = await generateChartInsights(spec, processedData, summary, chatInsights);
        
        return {
          ...spec,
          xLabel: spec.x,
          yLabel: spec.y,
          data: processedData,
          keyInsight: chartInsights.keyInsight,
        };
      }));
      
      processedCharts = chartsResult.filter((chart): chart is ChartSpec => chart !== null);
      
      // If all charts failed, return error message
      if (processedCharts && processedCharts.length === 0 && sanitized.length > 0) {
        const allCols = summary.columns.map(c => c.name);
        const firstSpec = sanitized[0];
        const xExists = allCols.some(c => c.toLowerCase() === firstSpec.x.toLowerCase());
        const yExists = allCols.some(c => c.toLowerCase() === firstSpec.y.toLowerCase());
        
        let errorMsg = `No valid data points found. `;
        if (!xExists) {
          errorMsg += `Column "${firstSpec.x}" not found. `;
        }
        if (!yExists) {
          errorMsg += `Column "${firstSpec.y}" not found. `;
        }
        if (xExists && yExists) {
          errorMsg += `Please check that "${firstSpec.y}" contains numeric data that can be aggregated. `;
        }
        errorMsg += `Available columns: ${allCols.join(', ')}`;
        
        return {
          answer: errorMsg,
          charts: undefined,
        };
      }
      
      console.log('Chat charts generated:', processedCharts?.length || 0);

      // If user asked explicitly for one line chart with two variables, merge first two line charts sharing same X
      const wantsSingleCombined = /\b(one|single)\s+line\s*chart\b|\bin\s+one\s+chart\b|\btogether\b/i.test(question);
      if (wantsSingleCombined && processedCharts && processedCharts.length >= 2) {
        const c1 = processedCharts.find(c => c.type === 'line' && Array.isArray(c.data));
        const c2 = processedCharts.find(c => c !== c1 && c.type === 'line' && Array.isArray(c.data));
        if (c1 && c2 && c1.x === c2.x) {
          // Build map from X to values for both series
          const xKey = c1.x;
          const y1Key = c1.y;
          const y2Key = c2.y;
          const map = new Map<string | number, any>();
          (c1.data as any[]).forEach(row => {
            const k = row[xKey];
            map.set(k, { [xKey]: k, [y1Key]: row[y1Key] });
          });
          (c2.data as any[]).forEach(row => {
            const k = row[xKey];
            const existing = map.get(k) || { [xKey]: k };
            existing[y2Key] = row[y2Key];
            map.set(k, existing);
          });
          const mergedData = Array.from(map.values()).sort((a, b) => String(a[xKey]).localeCompare(String(b[xKey])));
          const merged: ChartSpec = {
            type: 'line',
            title: c1.title || `${y1Key} and ${y2Key} over ${xKey}`,
            x: xKey,
            y: y1Key,
            y2: y2Key,
            xLabel: c1.xLabel || xKey,
            yLabel: c1.yLabel || y1Key,
            y2Label: c2.yLabel || y2Key,
            aggregate: 'none',
            data: mergedData,
            keyInsight: c1.keyInsight,
          } as any;
          // Replace charts with single merged one
          processedCharts = [merged];
        }
      }
    }

    // Always provide chat-level insights: prefer model's, else derive from charts
    let overallInsights = Array.isArray(result.insights) ? result.insights : undefined;
    if ((!overallInsights || overallInsights.length === 0) && Array.isArray(processedCharts) && processedCharts.length > 0) {
      // Generate insights from keyInsights
      overallInsights = [];
      processedCharts.forEach((c, idx) => {
        if (c.keyInsight) {
          overallInsights!.push({ id: overallInsights!.length + 1, text: c.keyInsight });
        }
      });
      // If still no insights, create at least one fallback
      if (overallInsights.length === 0) {
        overallInsights = [{ id: 1, text: `Generated ${processedCharts.length} chart(s) based on your question. Review the charts for detailed insights.` }];
      }
    }

    return withNotes({
      answer: result.answer,
      charts: processedCharts,
      insights: overallInsights,
    });
  } catch {
    return { answer: 'I apologize, but I had trouble processing your question. Please try rephrasing it.' };
  }
}
