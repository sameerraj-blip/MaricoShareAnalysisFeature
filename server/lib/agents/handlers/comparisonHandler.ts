import { BaseHandler, HandlerContext, HandlerResponse } from './baseHandler.js';
import { AnalysisIntent } from '../intentClassifier.js';
import { findMatchingColumn } from '../utils/columnMatcher.js';
import { analyzeCorrelations } from '../../correlationAnalyzer.js';
import { ChartSpec, Insight } from '../../../../shared/schema.js';

/**
 * Comparison Handler
 * Handles queries comparing variables, finding "best" options, ranking, etc.
 * Works for ANY domain: competitors, products, categories, etc.
 */
export class ComparisonHandler extends BaseHandler {
  canHandle(intent: AnalysisIntent): boolean {
    return intent.type === 'comparison';
  }

  async handle(intent: AnalysisIntent, context: HandlerContext): Promise<HandlerResponse> {
    console.log('📊 ComparisonHandler processing intent:', intent.type);
    
    const validation = this.validateData(intent, context);
    if (!validation.valid && validation.errors.some(e => e.includes('No data'))) {
      return this.createErrorResponse(
        validation.errors.join(', '),
        intent,
        validation.suggestions
      );
    }

    const question = intent.originalQuestion || intent.customRequest || '';
    const questionLower = question.toLowerCase();
    
    // Detect "best competitor/product/category" queries
    const bestMatch = questionLower.match(/best\s+(competitor|product|brand|company|category|option|choice|variable)\s+(?:to|for|of|with)\s+(\w+)/i);
    const bestSimpleMatch = questionLower.match(/best\s+(\w+)\s+(?:to|for|of|with)\s+(\w+)/i);
    
    if (bestMatch || bestSimpleMatch) {
      const match = bestMatch || bestSimpleMatch;
      const targetEntity = match![2]; // The entity we're comparing against (e.g., "PA")
      const relationshipType = bestMatch ? bestMatch[1] : 'option'; // competitor, product, etc.
      
      console.log(`🎯 Detected "best ${relationshipType}" query for target: ${targetEntity}`);
      
      return this.findBestOption(targetEntity, question, intent, context, relationshipType);
    }
    
    // Detect "compare X and Y" or "X vs Y" queries
    if (questionLower.includes(' vs ') || questionLower.includes(' versus ') || 
        (questionLower.includes(' compare ') && questionLower.includes(' and '))) {
      return this.compareVariables(question, intent, context);
    }
    
    // Fallback: Use general handler for other comparison queries
    console.log('⚠️ Comparison query not recognized, using general handler');
    const { generateGeneralAnswer } = await import('../../dataAnalyzer.js');
    const result = await generateGeneralAnswer(
      context.data,
      question,
      context.chatHistory,
      context.summary,
      context.sessionId
    );
    
    return {
      answer: result.answer,
      charts: result.charts,
      insights: result.insights,
    };
  }

  /**
   * Find the "best" option (competitor, product, etc.) based on correlation with target
   */
  private async findBestOption(
    targetEntity: string,
    question: string,
    intent: AnalysisIntent,
    context: HandlerContext,
    relationshipType: string
  ): Promise<HandlerResponse> {
    const allColumns = context.summary.columns.map(c => c.name);
    
    // Find target column (e.g., "PA" → "PA TOM")
    let targetCol = findMatchingColumn(targetEntity, allColumns);
    
    // Try pattern discovery if not found
    if (!targetCol) {
      const candidates = this.discoverColumnPatterns(allColumns, targetEntity);
      if (candidates.length > 0) {
        // Prefer columns that contain the exact entity name
        const exactMatch = candidates.find(c => 
          c.toLowerCase().includes(targetEntity.toLowerCase())
        );
        targetCol = exactMatch || candidates[0];
      }
    }
    
    if (!targetCol) {
      return {
        answer: `I couldn't find a column matching "${targetEntity}". Available columns: ${allColumns.slice(0, 5).join(', ')}${allColumns.length > 5 ? '...' : ''}`,
        requiresClarification: true,
      };
    }
    
    // Check if target is numeric
    if (!context.summary.numericColumns.includes(targetCol)) {
      return {
        answer: `The column "${targetCol}" is not numeric. I need numeric data to compare ${relationshipType}s.`,
        requiresClarification: true,
      };
    }
    
    // Extract potential competitors/options from question, chat history, or use all numeric columns
    let optionsToCompare: string[] = [];
    
    // Strategy 1: Extract from question explicitly mentioned (e.g., "JUI, Dabur Vatika, Meril Baby")
    const optionsMatch = question.match(/(?:focus|narrow|these|mentioned|discussed|we've|narrowed).*?([A-Z][A-Za-z\s,]+(?:and|,)[A-Z][A-Za-z\s,]+)/i);
    if (optionsMatch) {
      const optionsText = optionsMatch[1];
      const options = optionsText.split(/,|\sand\s/i).map(o => o.trim()).filter(o => o.length > 0);
      optionsToCompare = options.map(opt => {
        const matched = findMatchingColumn(opt, allColumns);
        return matched || null;
      }).filter((opt): opt is string => opt !== null);
      console.log(`📝 Extracted options from question:`, optionsToCompare);
    }
    
    // Strategy 2: Extract from recent chat history (AI might have mentioned them)
    // Look for patterns like "X, Y, and Z" or "X and Y" in recent messages
    if (optionsToCompare.length === 0 && context.chatHistory.length > 0) {
      const recentMessages = context.chatHistory.slice(-5).map(m => m.content).join(' ');
      // Match capitalized words/phrases separated by commas or "and"
      const listPattern = /\b([A-Z][A-Za-z\s]+(?:\s+[A-Z][A-Za-z\s]+)?)(?:\s*,\s*|\s+and\s+)([A-Z][A-Za-z\s]+(?:\s+[A-Z][A-Za-z\s]+)?)(?:\s*,\s*|\s+and\s+)?([A-Z][A-Za-z\s]+(?:\s+[A-Z][A-Za-z\s]+)?)?/g;
      const matches = Array.from(recentMessages.matchAll(listPattern));
      
      if (matches.length > 0) {
        // Get the most recent match (likely the relevant one)
        const lastMatch = matches[matches.length - 1];
        const potentialOptions = [lastMatch[1], lastMatch[2], lastMatch[3]]
          .filter(Boolean)
          .map(o => o.trim());
        
        optionsToCompare = potentialOptions
          .map(opt => findMatchingColumn(opt, allColumns))
          .filter((opt): opt is string => opt !== null && opt !== targetCol);
        
        if (optionsToCompare.length > 0) {
          console.log(`📝 Extracted options from chat history:`, optionsToCompare);
        }
      }
    }
    
    // Strategy 3: Use intent.variables if specified
    if (optionsToCompare.length === 0 && intent.variables && intent.variables.length > 0) {
      optionsToCompare = intent.variables
        .map(v => findMatchingColumn(v, allColumns))
        .filter((v): v is string => v !== null && v !== targetCol);
      console.log(`📝 Extracted options from intent variables:`, optionsToCompare);
    }
    
    // Strategy 4: If no options extracted, intelligently filter numeric columns
    // Exclude target and columns that are clearly not competitors (like other TOM columns)
    if (optionsToCompare.length === 0) {
      const targetLower = targetCol.toLowerCase();
      optionsToCompare = context.summary.numericColumns.filter(
        col => {
          const colLower = col.toLowerCase();
          // Exclude target column
          if (col === targetCol) return false;
          // Exclude columns that are clearly the same entity (e.g., "PA TOM" vs "PA nGRP")
          if (targetLower.split(/\s+/)[0] === colLower.split(/\s+/)[0] && 
              (colLower.includes('tom') || targetLower.includes('tom'))) {
            return false;
          }
          return true;
        }
      );
      console.log(`📝 Using all available numeric columns (${optionsToCompare.length} options)`);
    }
    
    if (optionsToCompare.length === 0) {
      return {
        answer: `I couldn't find any ${relationshipType}s to compare with "${targetCol}".`,
        requiresClarification: true,
      };
    }
    
    console.log(`📊 Comparing ${optionsToCompare.length} ${relationshipType}s against ${targetCol}`);
    console.log(`📋 Options:`, optionsToCompare);
    
    // Perform correlation analysis
    try {
      // Calculate correlations first to get ranking data
      const correlations = await this.calculateCorrelationsForRanking(
        context.data,
        targetCol,
        optionsToCompare
      );
      
      console.log(`📊 Calculated ${correlations.length} correlations for ranking`);
      
      // Detect sort order preference
      const question = intent.originalQuestion || intent.customRequest || '';
      const wantsDescending = /\bdescending|highest\s+to\s+lowest|high\s+to\s+low\b/i.test(question);
      const wantsAscending = /\bascending|lowest\s+to\s+highest|low\s+to\s+high\b/i.test(question);
      const sortOrder = wantsDescending ? 'descending' : wantsAscending ? 'ascending' : undefined; // Only set if user explicitly requested
      
      // Then get charts and insights from correlation analyzer
      const { charts, insights } = await analyzeCorrelations(
        context.data,
        targetCol,
        optionsToCompare,
        'all', // Get all correlations (positive and negative)
        sortOrder,
        context.chatInsights,
        undefined // No limit for comparison handler
      );
      
      console.log(`📊 Correlation analyzer returned ${charts?.length || 0} charts and ${insights?.length || 0} insights`);
      
      // Filter to positive correlations only (for "best" we want positive relationships)
      const positiveCorrelations = correlations
        .filter(c => c.correlation > 0)
        .sort((a, b) => b.correlation - a.correlation); // Highest first
      
      if (positiveCorrelations.length === 0) {
        return {
          answer: `I analyzed the ${relationshipType}s, but none of them show a positive correlation with "${targetCol}". This means they don't move together in the same direction.`,
          charts,
          insights,
        };
      }
      
      const best = positiveCorrelations[0];
      const bestName = best.variable;
      const bestCorrelation = best.correlation;
      
      // Build answer
      let answer = `Based on my analysis, ${bestName} is the best ${relationshipType} to "${targetCol}" `;
      answer += `with a positive correlation of ${bestCorrelation.toFixed(3)}. `;
      
      if (positiveCorrelations.length > 1) {
        answer += `Here's how all ${relationshipType}s rank:\n\n`;
        positiveCorrelations.slice(0, 5).forEach((corr, idx) => {
          answer += `${idx + 1}. ${corr.variable}: ${corr.correlation.toFixed(3)}\n`;
        });
      }
      
      answer += `\nThis means when ${bestName} increases, ${targetCol} tends to increase as well, indicating a strong positive relationship.`;
      
      // Filter charts to show only the best option
      const filteredCharts = charts?.filter(chart => {
        if (chart.type === 'scatter' && chart.x === bestName) {
          return true;
        }
        if (chart.type === 'bar' && chart.data) {
          const data = chart.data as any[];
          return data.some((item: any) => 
            (item.variable || item[chart.x]) === bestName
          );
        }
        return false;
      }) || [];
      
      // Create a ranking chart with proper data structure
      // No limit - show all correlations in ranking chart
      const rankingData = positiveCorrelations.map(corr => ({
        variable: corr.variable,
        correlation: corr.correlation,
      }));
      
      const rankingChart: ChartSpec = {
        type: 'bar',
        title: `Top ${relationshipType}s by Correlation with ${targetCol}`,
        x: 'variable',
        y: 'correlation',
        xLabel: relationshipType,
        yLabel: 'Correlation',
        aggregate: 'none',
        data: rankingData,
      };
      
      // Generate insights if not provided
      let finalInsights = insights || [];
      if (finalInsights.length === 0) {
        finalInsights = [
          {
            id: 1,
            text: `**Best ${relationshipType}**: ${bestName} has the strongest positive correlation (${bestCorrelation.toFixed(3)}) with ${targetCol}, indicating the best alignment.`,
          },
          {
            id: 2,
            text: `**Suggestion**: Focus on ${bestName} as it shows the strongest positive relationship with ${targetCol}. When ${bestName} increases, ${targetCol} tends to increase as well.`,
          },
        ];
      }
      
      console.log(`✅ Comparison complete: Best ${relationshipType} is ${bestName} (correlation: ${bestCorrelation.toFixed(3)})`);
      console.log(`📊 Returning ${[rankingChart, ...filteredCharts].length} charts and ${finalInsights.length} insights`);
      
      return {
        answer,
        charts: [rankingChart, ...filteredCharts],
        insights: finalInsights,
      };
    } catch (error) {
      console.error('Comparison analysis error:', error);
      return this.createErrorResponse(
        error instanceof Error ? error : new Error(String(error)),
        intent
      );
    }
  }

  /**
   * Compare two or more variables directly
   */
  private async compareVariables(
    question: string,
    intent: AnalysisIntent,
    context: HandlerContext
  ): Promise<HandlerResponse> {
    // Extract variables from question
    const variables = intent.variables || [];
    if (variables.length < 2) {
      return {
        answer: "I need at least two variables to compare. Please specify which variables you'd like to compare.",
        requiresClarification: true,
      };
    }
    
    // Use general handler for now - can be enhanced later
    const { generateGeneralAnswer } = await import('../../dataAnalyzer.js');
    const result = await generateGeneralAnswer(
      context.data,
      question,
      context.chatHistory,
      context.summary,
      context.sessionId
    );
    
    return {
      answer: result.answer,
      charts: result.charts,
      insights: result.insights,
    };
  }

  /**
   * Calculate correlations for ranking purposes
   */
  private async calculateCorrelationsForRanking(
    data: Record<string, any>[],
    targetVariable: string,
    comparisonColumns: string[]
  ): Promise<Array<{ variable: string; correlation: number }>> {
    const correlations: Array<{ variable: string; correlation: number }> = [];
    
    // Simple correlation calculation (can be optimized)
    const targetValues = data
      .map(row => this.parseNumericValue(row[targetVariable]))
      .filter(v => v !== null && !isNaN(v)) as number[];
    
    for (const col of comparisonColumns) {
      const colValues = data
        .map(row => this.parseNumericValue(row[col]))
        .filter(v => v !== null && !isNaN(v)) as number[];
      
      if (colValues.length === 0 || targetValues.length === 0) continue;
      
      // Calculate Pearson correlation
      const correlation = this.pearsonCorrelation(targetValues, colValues);
      if (!isNaN(correlation)) {
        correlations.push({ variable: col, correlation });
      }
    }
    
    return correlations;
  }

  private parseNumericValue(value: any): number | null {
    if (value === null || value === undefined || value === '') return null;
    if (typeof value === 'number') return isNaN(value) ? null : value;
    const cleaned = String(value).replace(/[%,]/g, '').trim();
    const parsed = Number(cleaned);
    return isNaN(parsed) ? null : parsed;
  }

  private pearsonCorrelation(x: number[], y: number[]): number {
    const n = Math.min(x.length, y.length);
    if (n === 0) return NaN;

    const sumX = x.slice(0, n).reduce((a, b) => a + b, 0);
    const sumY = y.slice(0, n).reduce((a, b) => a + b, 0);
    const sumXY = x.slice(0, n).reduce((sum, xi, i) => sum + xi * y[i], 0);
    const sumX2 = x.slice(0, n).reduce((sum, xi) => sum + xi * xi, 0);
    const sumY2 = y.slice(0, n).reduce((sum, yi) => sum + yi * yi, 0);

    const numerator = n * sumXY - sumX * sumY;
    const denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));

    return denominator === 0 ? NaN : numerator / denominator;
  }

  /**
   * Discover column patterns (same as correlation handler)
   */
  private discoverColumnPatterns(columns: string[], searchTerm: string): string[] {
    const candidates: string[] = [];
    const searchLower = searchTerm.toLowerCase().trim();
    const searchWords = searchLower.split(/\s+/).filter(w => w.length > 0);
    
    for (const col of columns) {
      const colLower = col.toLowerCase().trim();
      if (colLower.includes(searchLower)) {
        candidates.push(col);
      }
    }
    
    if (searchWords.length > 1) {
      for (const col of columns) {
        const colLower = col.toLowerCase().trim();
        if (candidates.includes(col)) continue;
        
        let allWordsFound = true;
        for (const word of searchWords) {
          if (!colLower.includes(word)) {
            allWordsFound = false;
            break;
          }
        }
        if (allWordsFound) {
          candidates.push(col);
        }
      }
    }
    
    if (searchWords.length > 0) {
      const firstWord = searchWords[0];
      for (const col of columns) {
        const colLower = col.toLowerCase().trim();
        if (candidates.includes(col)) continue;
        
        if (colLower.startsWith(firstWord) && colLower.length > firstWord.length) {
          candidates.push(col);
        }
      }
    }
    
    return Array.from(new Set(candidates)).slice(0, 10);
  }
}

