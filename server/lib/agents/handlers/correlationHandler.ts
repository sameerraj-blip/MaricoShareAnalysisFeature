import { BaseHandler, HandlerContext, HandlerResponse } from './baseHandler.js';
import { AnalysisIntent } from '../intentClassifier.js';
import { analyzeCorrelations } from '../../correlationAnalyzer.js';
import { findMatchingColumn } from '../utils/columnMatcher.js';

/**
 * Correlation Handler
 * Handles correlation analysis queries like "what affects X?"
 */
export class CorrelationHandler extends BaseHandler {
  canHandle(intent: AnalysisIntent): boolean {
    return intent.type === 'correlation';
  }

  async handle(intent: AnalysisIntent, context: HandlerContext): Promise<HandlerResponse> {
    // Validate data (basic checks only - column matching happens below)
    const validation = this.validateData(intent, context);
    // Only fail on critical errors (no data), not on column matching issues
    // Column matching is handled intelligently below
    if (!validation.valid && validation.errors.some(e => e.includes('No data'))) {
      return this.createErrorResponse(
        validation.errors.join(', '),
        intent,
        validation.suggestions
      );
    }

    // Extract target variable
    let targetVariable = intent.targetVariable;
    
    // If no target variable, try to extract it from the question
    if (!targetVariable) {
      const question = intent.originalQuestion || intent.customRequest || '';
      console.log(`🔍 No targetVariable in intent, trying to extract from question: "${question}"`);
      
      // Pattern 1: "what impacts X" or "what affects X" or "what influences X"
      // Handle both "what impacts X?" and "what impacts X" (with or without question mark)
      const impactMatch = question.match(/what\s+(?:impacts?|affects?|influences?)\s+(.+?)(?:\s*\?|$)/i);
      if (impactMatch && impactMatch[1]) {
        targetVariable = impactMatch[1].trim();
        // Remove trailing question mark if present
        targetVariable = targetVariable.replace(/\?+$/, '').trim();
        console.log(`📝 Extracted target from "what impacts" pattern: ${targetVariable}`);
      }
      
      // Pattern 2: "X is my brand", "X is the brand", "X is the main brand"
      if (!targetVariable) {
        const brandMatch = question.match(/(\w+(?:\s+\w+)*)\s+is\s+(?:my|the|our)\s+brand/i);
        if (brandMatch && brandMatch[1]) {
          targetVariable = brandMatch[1].trim();
          console.log(`📝 Extracted target brand from question: ${targetVariable}`);
        }
      }
      
      // Pattern 3: "correlation of X with all the other variables" or "correlation between X and Y"
      if (!targetVariable) {
        // First try: "correlation of X with all (the other) variables"
        const correlationWithAllMatch = question.match(/correlation\s+of\s+(.+?)\s+with\s+all(?:\s+the\s+other)?\s+variables?/i);
        if (correlationWithAllMatch && correlationWithAllMatch[1]) {
          targetVariable = correlationWithAllMatch[1].trim();
          console.log(`📝 Extracted target from "correlation of X with all" pattern: ${targetVariable}`);
        }
        
        // Second try: "correlation between X and Y" or "correlation of X with Y"
        if (!targetVariable) {
          const correlationMatch = question.match(/correlation\s+(?:between|of)\s+(.+?)\s+(?:and|with)\s+(?!all\s+the?\s+other)/i);
          if (correlationMatch && correlationMatch[1]) {
            targetVariable = correlationMatch[1].trim();
            console.log(`📝 Extracted target from correlation pattern: ${targetVariable}`);
          }
        }
      }
      
      // Pattern 4: Look for any column name mentioned in the question
      if (!targetVariable) {
        const allColumns = context.summary.columns.map(c => c.name);
        const questionLower = question.toLowerCase();
        // Try to find a column name that appears in the question
        for (const col of allColumns) {
          const colLower = col.toLowerCase();
          // Check if column name (or significant part of it) appears in question
          const colWords = colLower.split(/\s+/).filter(w => w.length > 2);
          if (colWords.length > 0 && colWords.some(word => questionLower.includes(word))) {
            // Verify it's a reasonable match (not too generic)
            if (colWords.length >= 2 || col.length >= 4) {
              targetVariable = col;
              console.log(`📝 Extracted target from column name match: ${targetVariable}`);
              break;
            }
          }
        }
      }
    }
    
    if (!targetVariable) {
      // Last resort: try to find any column name mentioned in the question
      const question = intent.originalQuestion || intent.customRequest || '';
      const allColumns = context.summary.columns.map(c => c.name);
      const questionLower = question.toLowerCase();
      
      // Try to find any column that appears in the question
      for (const col of allColumns) {
        const colWords = col.toLowerCase().split(/\s+/).filter(w => w.length > 2);
        // Check if any significant word from column appears in question
        if (colWords.some(word => questionLower.includes(word))) {
          // Check if it's a reasonable match (not too generic, at least 2 words or 4+ chars)
          if (colWords.length >= 2 || col.length >= 4) {
            targetVariable = col;
            console.log(`📝 Last resort: extracted target from column name in question: ${targetVariable}`);
            break;
          }
        }
      }
      
      if (!targetVariable) {
        return {
          answer: `I need to know which variable you'd like to analyze. For example: 'What affects revenue?' or 'What impacts ${allColumns[0] || 'PA TOM'}?'`,
          requiresClarification: true,
          suggestions: allColumns.slice(0, 5),
        };
      }
    }

    // Find matching column - use intelligent pattern discovery, not hardcoded suffixes
    const allColumns = context.summary.columns.map(c => c.name);
    console.log(`🔍 Looking for target variable: "${targetVariable}"`);
    console.log(`📋 Available columns (first 10):`, allColumns.slice(0, 10));
    
    let targetCol = findMatchingColumn(targetVariable, allColumns);
    console.log(`🎯 Direct match result:`, targetCol || 'NOT FOUND');
    
    // If not found, discover column patterns from the data itself
    if (!targetCol) {
      console.log(`🔍 No direct match, trying pattern discovery...`);
      // Discover potential matching columns by analyzing naming patterns
      const candidateColumns = this.discoverColumnPatterns(allColumns, targetVariable);
      console.log(`📊 Discovered candidate columns:`, candidateColumns);
      
      // Pattern discovery returns actual column names, so use them directly
      // Prioritize exact phrase matches first
      const searchLower = targetVariable.toLowerCase().trim();
      for (const candidate of candidateColumns) {
        const candidateLower = candidate.toLowerCase().trim();
        // Prefer columns that contain the exact search phrase
        if (candidateLower.includes(searchLower)) {
          targetCol = candidate;
          console.log(`✅ Matched target via pattern discovery (exact phrase): ${candidate}`);
          break;
        }
      }
      
      // If no exact phrase match, use the first candidate
      if (!targetCol && candidateColumns.length > 0) {
        targetCol = candidateColumns[0];
        console.log(`✅ Matched target via pattern discovery (best candidate): ${targetCol}`);
      }
    }
    
    console.log(`✅ Final target column:`, targetCol || 'NOT FOUND');

    if (!targetCol) {
      const suggestions = this.findSimilarColumns(targetVariable || '', context.summary);
      return {
        answer: `I couldn't find a column matching "${targetVariable}". ${suggestions.length > 0 ? `Did you mean: ${suggestions.join(', ')}?` : `Available columns: ${allColumns.slice(0, 5).join(', ')}${allColumns.length > 5 ? '...' : ''}`}`,
        requiresClarification: true,
        suggestions,
      };
    }

    // Check if target is numeric
    if (!context.summary.numericColumns.includes(targetCol)) {
      return {
        answer: `The column "${targetCol}" is not numeric. Correlation analysis requires numeric data. Available numeric columns: ${context.summary.numericColumns.slice(0, 5).join(', ')}${context.summary.numericColumns.length > 5 ? '...' : ''}`,
        requiresClarification: true,
        suggestions: context.summary.numericColumns.slice(0, 5),
      };
    }

    // Determine filter
    const filter = intent.filters?.correlationSign === 'positive' 
      ? 'positive' 
      : intent.filters?.correlationSign === 'negative' 
      ? 'negative' 
      : 'all';

    // Get comparison columns (exclude target variable)
    const comparisonColumns = context.summary.numericColumns.filter(
      col => col !== targetCol
    );

    if (comparisonColumns.length === 0) {
      return {
        answer: `I need at least one other numeric column to compare with "${targetCol}". Your dataset only has one numeric column.`,
      };
    }

    // Apply variable filters if specified
    let filteredComparisonColumns = comparisonColumns;
    
    // Handle excludeVariables - these are variables to completely exclude
    if (intent.filters?.excludeVariables && intent.filters.excludeVariables.length > 0) {
      filteredComparisonColumns = filteredComparisonColumns.filter(
        col => !intent.filters!.excludeVariables!.some(exclude => {
          const matched = findMatchingColumn(exclude, [col]);
          return matched === col || matched === col.trim();
        })
      );
    }
    
    // Prepare list of variables that need special filtering (general-purpose, not domain-specific)
    // This will be used post-analysis to apply constraints
    const variablesToFilterNegative = intent.filters?.excludeVariables || [];

    if (intent.filters?.includeOnly && intent.filters.includeOnly.length > 0) {
      filteredComparisonColumns = filteredComparisonColumns.filter(
        col => intent.filters!.includeOnly!.some(include => {
          const matched = findMatchingColumn(include, [col]);
          return matched === col || matched === col.trim();
        })
      );
    }
    
    // If intent.variables is specified, use those instead of all comparison columns
    if (intent.variables && intent.variables.length > 0) {
      const matchedVariables = intent.variables
        .map(v => findMatchingColumn(v, allColumns))
        .filter((v): v is string => v !== null && v !== targetCol);
      
      if (matchedVariables.length > 0) {
        filteredComparisonColumns = filteredComparisonColumns.filter(
          col => matchedVariables.includes(col) || matchedVariables.includes(col.trim())
        );
      }
    }

    if (filteredComparisonColumns.length === 0) {
      return {
        answer: `After applying filters, there are no columns left to compare with "${targetCol}".`,
        requiresClarification: true,
      };
    }

    console.log(`📊 Analyzing correlations for "${targetCol}" with ${filteredComparisonColumns.length} comparison columns`);
    console.log(`📋 Comparison columns:`, filteredComparisonColumns);

    // Validate that columns actually exist in the data
    const firstRow = context.data[0];
    if (firstRow) {
      const actualColumns = Object.keys(firstRow);
      const missingColumns = filteredComparisonColumns.filter(
        col => !actualColumns.includes(col) && !actualColumns.includes(col.trim())
      );
      
      if (missingColumns.length > 0) {
        console.warn(`⚠️ Some columns not found in data:`, missingColumns);
        // Try to find matches
        const matchedColumns = filteredComparisonColumns
          .map(col => {
            if (actualColumns.includes(col) || actualColumns.includes(col.trim())) {
              return actualColumns.find(ac => ac === col || ac.trim() === col.trim()) || col;
            }
            // Try fuzzy match
            const match = findMatchingColumn(col, actualColumns);
            return match || null;
          })
          .filter((col): col is string => col !== null);
        
        if (matchedColumns.length === 0) {
          return {
            answer: `I couldn't find the columns to compare. The extracted column names don't match the actual data columns. Please try rephrasing your question.`,
            requiresClarification: true,
            suggestions: actualColumns.slice(0, 5),
          };
        }
        
        filteredComparisonColumns = matchedColumns;
        console.log(`✅ Matched columns:`, filteredComparisonColumns);
      }
    }

    try {
      // Detect sort order preference
      const originalQuestion = intent.originalQuestion || intent.customRequest || '';
      const wantsDescending = /\bdescending|highest\s+to\s+lowest|high\s+to\s+low|largest\s+to\s+smallest|biggest\s+to\s+smallest\b/i.test(originalQuestion);
      const wantsAscending = /\bascending|lowest\s+to\s+highest|low\s+to\s+high|smallest\s+to\s+largest|smallest\s+to\s+biggest\b/i.test(originalQuestion);
      const sortOrder = wantsDescending ? 'descending' : wantsAscending ? 'ascending' : undefined; // Only set if user explicitly requested
      
      // Detect "top N" request (e.g., "top 10 variables", "top 5 factors")
      const topNMatch = originalQuestion.match(/\btop\s+(\d+)\b/i);
      const maxResults = topNMatch ? parseInt(topNMatch[1], 10) : undefined;
      
      // Detect yes/no style correlation questions, e.g.:
      // "Should we analyze PA TOM's correlation with PA nGRP Adstocked?"
      const isYesNoCorrelationQuestion =
        /\bshould\s+we\s+analy[sz]e\b/i.test(originalQuestion) ||
        /\bshould\s+i\s+analy[sz]e\b/i.test(originalQuestion) ||
        /\bshould\s+we\s+look\s+at\s+the\s+correlation\b/i.test(originalQuestion);

      // Call correlation analyzer
      let { charts, insights } = await analyzeCorrelations(
        context.data,
        targetCol,
        filteredComparisonColumns,
          filter,
          sortOrder,
          context.chatInsights,
          maxResults
      );

      // Post-process: Apply general constraint system (works for ANY relationship type, not just "sister brands")
      // Check if user wants to exclude negative correlations for specific variables
      const mentionsNegativeImpact = /don'?t\s+want.*negative|no\s+negative\s+impact|exclude.*negative.*impact/i.test(originalQuestion);
      
      // Get variables that should have negative correlations excluded (general-purpose)
      const variablesToFilterNegative = intent.filters?.excludeVariables || [];
      
      if (mentionsNegativeImpact && variablesToFilterNegative.length > 0) {
        console.log(`🔍 Filtering out negative correlations for specified variables: ${variablesToFilterNegative.join(', ')}`);
        
        // Filter charts: Remove scatter plots for variables with negative correlations
        const allColumns = context.summary.columns.map(c => c.name);
        charts = charts.filter(chart => {
          if (chart.type === 'scatter' && chart.x) {
            // Check if this chart's X-axis is in the filter list
            const shouldFilter = variablesToFilterNegative.some(variable => {
              const matched = findMatchingColumn(variable, [chart.x]);
              return matched === chart.x || matched === chart.x.trim();
            });
            
            if (shouldFilter) {
              // We'll filter these out and let the bar chart show filtered results
              return false;
            }
          }
          return true;
        });
        
        // Filter bar chart data: Remove negative correlations for specified variables
        charts = charts.map(chart => {
          if (chart.type === 'bar' && chart.data) {
            const filteredData = (chart.data as any[]).filter((item: any) => {
              const variable = item.variable || item[chart.x];
              if (!variable) return true;
              
              const shouldFilter = variablesToFilterNegative.some(filterVar => {
                const matched = findMatchingColumn(filterVar, [variable]);
                return matched === variable || matched === variable.trim();
              });
              
              if (shouldFilter) {
                const correlation = item.correlation || item[chart.y];
                // Only keep if correlation is positive
                return correlation > 0;
              }
              
              return true;
            });
            
            return { ...chart, data: filteredData };
          }
          return chart;
        });
      }

      // Build answer
      let answer: string;

      if (isYesNoCorrelationQuestion && filteredComparisonColumns.length === 1) {
        const comparedVar = filteredComparisonColumns[0];

        // Try to extract the correlation coefficient r from the scatter chart title, if available
        let rValueText: string | null = null;
        let strengthText: string | null = null;

        const scatterChart = charts.find(
          (c) => c.type === 'scatter' && typeof c.title === 'string' && c.title.includes('r=')
        );

        if (scatterChart && scatterChart.title) {
          const match = scatterChart.title.match(/r\s*=\s*([-+]?\d*\.?\d+)/i);
          if (match) {
            const rVal = parseFloat(match[1]);
            if (!Number.isNaN(rVal)) {
              const absR = Math.abs(rVal);
              rValueText = rVal.toFixed(2);
              if (absR < 0.2) {
                strengthText = 'very weak';
              } else if (absR < 0.4) {
                strengthText = 'weak';
              } else if (absR < 0.6) {
                strengthText = 'moderate';
              } else {
                strengthText = 'strong';
              }
            }
          }
        }

        if (rValueText && strengthText) {
          answer = `Yes – based on your data, the correlation between ${comparedVar} and ${targetCol} is r ≈ ${rValueText}, which is a ${strengthText} relationship. `;
        } else {
          answer = `Yes – we can analyze the correlation between ${comparedVar} and ${targetCol} using your dataset. `;
        }

        if (charts.length > 0) {
          answer += `I've included ${charts.length} visualization${charts.length > 1 ? 's' : ''} to show this relationship. `;
        }
        if (insights.length > 0) {
          answer += `Here are the key insights:`;
        }
      } else {
        // Default explanatory style for general correlation questions
        answer = `I've analyzed what affects ${targetCol}. `;

        if (maxResults) {
          answer += `I've limited the analysis to the top ${maxResults} variables as requested. `;
        }
        
        if (filter === 'positive') {
          answer += `I've filtered to show only positive correlations as requested. `;
        } else if (filter === 'negative') {
          answer += `I've filtered to show only negative correlations as requested. `;
        }
        
        if (mentionsNegativeImpact && variablesToFilterNegative.length > 0) {
          answer += `As requested, I've excluded negative correlations for the specified variables (${variablesToFilterNegative.join(', ')}). `;
        }

        if (charts.length > 0) {
          answer += `I've created ${charts.length} visualization${charts.length > 1 ? 's' : ''} showing the key relationships. `;
        }
        
        if (insights.length > 0) {
          answer += `Here are the key insights:`;
        }
      }

      return {
        answer,
        charts,
        insights,
      };
    } catch (error) {
      console.error('Correlation analysis error:', error);
      return this.createErrorResponse(
        error instanceof Error ? error : new Error(String(error)),
        intent,
        this.findSimilarColumns(targetVariable || '', context.summary)
      );
    }
  }

  /**
   * Discover column patterns from data structure (general-purpose, no hardcoding)
   * Returns columns that might match the search term (for use with findMatchingColumn)
   */
  private discoverColumnPatterns(columns: string[], searchTerm: string): string[] {
    const candidates: string[] = [];
    const searchLower = searchTerm.toLowerCase().trim();
    const searchWords = searchLower.split(/\s+/).filter(w => w.length > 0);
    
    // Strategy 1: Find columns that contain the exact search phrase as a substring
    // This handles cases like "PAB nGRP" matching "PAB nGRP Adstocked" or "PAB nGRP Value"
    for (const col of columns) {
      const colLower = col.toLowerCase().trim();
      if (colLower.includes(searchLower)) {
        candidates.push(col);
      }
    }
    
    // Strategy 2: Find columns that start with the search phrase (prefix match)
    // This handles cases like "PAB nGRP" matching "PAB nGRP Adstocked"
    for (const col of columns) {
      const colLower = col.toLowerCase().trim();
      if (candidates.includes(col)) continue; // Already added
      
      if (colLower.startsWith(searchLower + ' ') || colLower === searchLower) {
        candidates.push(col);
      }
    }
    
    // Strategy 3: Find columns that contain all words from search term (in any order)
    // This handles cases like "PA TOM" matching "TOM PA" or "PA Brand TOM"
    if (searchWords.length > 1) {
      for (const col of columns) {
        const colLower = col.toLowerCase().trim();
        if (candidates.includes(col)) continue; // Already added
        
        let allWordsFound = true;
        for (const word of searchWords) {
          // Use word boundary matching for better accuracy
          const wordRegex = new RegExp(`\\b${word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
          if (!wordRegex.test(colLower)) {
            allWordsFound = false;
            break;
          }
        }
        if (allWordsFound) {
          candidates.push(col);
        }
      }
    }
    
    // Strategy 4: Find columns that start with first word (prefix match)
    // This handles cases like "PA" matching "PA TOM", "PA nGRP", etc.
    if (searchWords.length > 0) {
      const firstWord = searchWords[0];
      for (const col of columns) {
        const colLower = col.toLowerCase().trim();
        if (candidates.includes(col)) continue; // Already added
        
        // Check if column starts with first word followed by space or is exactly the first word
        if ((colLower.startsWith(firstWord + ' ') || colLower === firstWord) && colLower.length > firstWord.length) {
          candidates.push(col);
        }
      }
    }
    
    // Sort candidates by relevance (exact matches first, then prefix matches, then word matches)
    const sortedCandidates = candidates.sort((a, b) => {
      const aLower = a.toLowerCase();
      const bLower = b.toLowerCase();
      
      // Exact match gets highest priority
      if (aLower === searchLower) return -1;
      if (bLower === searchLower) return 1;
      
      // Prefix match gets second priority
      if (aLower.startsWith(searchLower)) return -1;
      if (bLower.startsWith(searchLower)) return 1;
      
      // Then by length (shorter matches are better)
      return a.length - b.length;
    });
    
    // Return unique candidates, prioritizing exact matches
    return Array.from(new Set(sortedCandidates)).slice(0, 10);
  }
}

