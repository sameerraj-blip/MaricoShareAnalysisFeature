import { BaseHandler, HandlerContext, HandlerResponse } from './baseHandler.js';
import { AnalysisIntent } from '../intentClassifier.js';
import { analyzeCorrelations } from '../../correlationAnalyzer.js';
import { findMatchingColumn } from '../utils/columnMatcher.js';

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

    // Extract context-based variable lists (e.g., "sisters brands") from permanent context
    const question = intent.originalQuestion || intent.customRequest || '';
    const questionLower = question.toLowerCase();
    const permanentContext = context.permanentContext || '';
    
    // Check if user mentions "sisters brands" or similar terms
    const mentionsSistersBrands = /\b(sisters?\s+brands?|sister\s+brands?)\b/i.test(question);
    
    // Extract variable lists from permanent context
    let contextVariableList: string[] = [];
    if (permanentContext && mentionsSistersBrands) {
      // Try to extract list after "sisters brands" or similar patterns
      // Pattern: "sisters brand-> X, Y, Z" or "sisters brands: X, Y, Z" or "sisters brands are X, Y, Z"
      const sistersBrandsMatch = permanentContext.match(/sisters?\s+brands?[:\->]?\s*([^\n]+)/i);
      if (sistersBrandsMatch && sistersBrandsMatch[1]) {
        // Split by comma and clean up
        contextVariableList = sistersBrandsMatch[1]
          .split(',')
          .map(v => v.trim())
          .filter(v => v.length > 0);
        console.log(`📋 Extracted ${contextVariableList.length} variables from permanent context:`, contextVariableList);
      } else {
        // Try alternative pattern: look for comma-separated list after "sisters brands"
        const altMatch = permanentContext.match(/sisters?\s+brands?[^\n]*?([A-Z][^,\n]+(?:,\s*[A-Z][^,\n]+)+)/i);
        if (altMatch && altMatch[1]) {
          contextVariableList = altMatch[1]
            .split(',')
            .map(v => v.trim())
            .filter(v => v.length > 0);
          console.log(`📋 Extracted ${contextVariableList.length} variables from permanent context (alt pattern):`, contextVariableList);
        }
      }
    }

    // Apply variable filters if specified
    let filteredComparisonColumns = comparisonColumns;
    
    // If user mentioned "sisters brands" and we found a list in context, filter to only those
    if (mentionsSistersBrands && contextVariableList.length > 0) {
      const allColumns = context.summary.columns.map(c => c.name);
      // Match context variables to actual column names
      const matchedContextVariables = contextVariableList
        .map(contextVar => findMatchingColumn(contextVar.trim(), allColumns))
        .filter((v): v is string => v !== null && v !== targetCol);
      
      if (matchedContextVariables.length > 0) {
        filteredComparisonColumns = filteredComparisonColumns.filter(
          col => matchedContextVariables.includes(col) || matchedContextVariables.includes(col.trim())
        );
        console.log(`✅ Filtered to ${filteredComparisonColumns.length} context-specified variables:`, filteredComparisonColumns);
      } else {
        console.warn(`⚠️ Could not match any context variables to actual columns. Context vars:`, contextVariableList);
      }
    }
    
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

      // Detect if user asked for "all variables" - they want a complete list
      // Patterns: "correlation of X with all variables", "impact of X with all variables", 
      // "what impacts X" (when comparing with all), "all variables", etc.
      const wantsAllVariables = 
        /\b(all|every)\s+(the\s+other\s+)?variables?\b/i.test(originalQuestion) ||
        /\bcorrelation\s+(of|with)\s+.+?\s+with\s+all/i.test(originalQuestion) ||
        /\bimpact\s+of\s+.+?\s+with\s+all/i.test(originalQuestion) ||
        /\bwhat\s+(impacts?|affects?|influences?)\s+.+?\?/i.test(originalQuestion) && filteredComparisonColumns.length > 5; // If asking "what impacts X" and there are many comparison columns, likely wants all

      // Check if user explicitly requested charts
      const wantsCharts = isExplicitChartRequest(originalQuestion);
      
      // Call correlation analyzer (always calculate correlations, but conditionally generate charts)
      let { charts, insights } = await analyzeCorrelations(
        context.data,
        targetCol,
        filteredComparisonColumns,
          filter,
          sortOrder,
          context.chatInsights,
          maxResults,
          undefined, // onProgress
          context.sessionId, // sessionId for caching
          wantsCharts // Pass flag to control chart generation
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

      // Get correlations data for conversational answer (even if charts weren't generated)
      // We need to calculate correlations separately if charts weren't generated
      let correlations: Array<{ variable: string; correlation: number; nPairs: number }> = [];
      if (!wantsCharts || charts.length === 0) {
        // Calculate correlations for the answer even if charts weren't generated
        const correlationAnalyzer = await import('../../correlationAnalyzer.js');
        const rawCorrelations = correlationAnalyzer.calculateCorrelations(context.data, targetCol, filteredComparisonColumns);
        // Map to ensure nPairs is always a number
        const mappedCorrelations: Array<{ variable: string; correlation: number; nPairs: number }> = rawCorrelations.map(c => ({
          variable: c.variable,
          correlation: c.correlation,
          nPairs: c.nPairs ?? 0
        }));
        correlations = mappedCorrelations;
        // Apply filters and sorting
        let filtered = correlations;
        if (filter === 'positive') {
          filtered = filtered.filter(c => c.correlation > 0);
        } else if (filter === 'negative') {
          filtered = filtered.filter(c => c.correlation < 0);
        }
        if (sortOrder === 'descending') {
          filtered = filtered.sort((a, b) => b.correlation - a.correlation);
        } else if (sortOrder === 'ascending') {
          filtered = filtered.sort((a, b) => a.correlation - b.correlation);
        } else {
          filtered = filtered.sort((a, b) => Math.abs(b.correlation) - Math.abs(a.correlation));
        }
        if (maxResults) {
          filtered = filtered.slice(0, maxResults);
        }
        correlations = filtered;
      } else {
        // Extract correlations from charts if they were generated
        const barChart = charts.find(c => c.type === 'bar');
        if (barChart && barChart.data) {
          correlations = (barChart.data as any[]).map((item: any) => ({
            variable: item.variable || item[barChart.x || 'variable'],
            correlation: item.correlation || item[barChart.y || 'correlation'],
            nPairs: 0
          }));
        }
      }
      
      // If user asked for "sisters brands" and we have context variables, ensure ALL are included
      // Even if some have weak or zero correlations
      if (mentionsSistersBrands && contextVariableList.length > 0) {
        const allColumns = context.summary.columns.map(c => c.name);
        const matchedContextVariables = contextVariableList
          .map(contextVar => findMatchingColumn(contextVar.trim(), allColumns))
          .filter((v): v is string => v !== null && v !== targetCol);
        
        // Add any context variables that weren't found in correlations (they might have zero/very weak correlations)
        const correlationAnalyzer = await import('../../correlationAnalyzer.js');
        for (const contextVar of matchedContextVariables) {
          const exists = correlations.some(c => c.variable === contextVar || c.variable === contextVar.trim());
          if (!exists) {
            // Calculate correlation for this variable if it wasn't included
            try {
              const singleCorr = correlationAnalyzer.calculateCorrelations(context.data, targetCol, [contextVar]);
              if (singleCorr.length > 0) {
                correlations.push({
                  variable: contextVar,
                  correlation: singleCorr[0].correlation,
                  nPairs: singleCorr[0].nPairs ?? 0
                });
              } else {
                // If no correlation found, add with zero correlation
                correlations.push({
                  variable: contextVar,
                  correlation: 0,
                  nPairs: 0
                });
              }
            } catch (e) {
              // If calculation fails, add with zero correlation
              correlations.push({
                variable: contextVar,
                correlation: 0,
                nPairs: 0
              });
            }
          }
        }
        
        // Filter correlations to only include context variables (in case some non-context variables were included)
        correlations = correlations.filter(c => {
          return matchedContextVariables.some(mv => mv === c.variable || mv === c.variable.trim());
        });
        
        console.log(`✅ Ensured all ${matchedContextVariables.length} context variables are included in results`);
      }
      
      // Calculate correlation summary for conversational answer
      const topCorrelation = correlations.length > 0 ? correlations[0] : null;
      const topPositive = correlations.filter(c => c.correlation > 0).sort((a, b) => b.correlation - a.correlation)[0];
      const topNegative = correlations.filter(c => c.correlation < 0).sort((a, b) => a.correlation - b.correlation)[0];

      // Build answer - conversational style for general questions
      let answer: string = '';

      if (isYesNoCorrelationQuestion && filteredComparisonColumns.length === 1) {
        const comparedVar = filteredComparisonColumns[0];
        const correlation = correlations.find(c => c.variable === comparedVar);
        
        if (correlation) {
          const absR = Math.abs(correlation.correlation);
          let strengthText = 'very weak';
          if (absR >= 0.6) strengthText = 'strong';
          else if (absR >= 0.4) strengthText = 'moderate';
          else if (absR >= 0.2) strengthText = 'weak';
          
          const direction = correlation.correlation > 0 ? 'positive' : 'negative';
          answer = `Yes, ${comparedVar} does ${direction === 'positive' ? 'positively' : 'negatively'} impact ${targetCol}. The correlation is r ≈ ${correlation.correlation.toFixed(2)}, which indicates a ${strengthText} ${direction} relationship.`;
          
          if (wantsCharts) {
            answer += ` I've included visualizations to show this relationship.`;
          }
        } else {
          answer = `Yes, we can analyze the correlation between ${comparedVar} and ${targetCol}.`;
        }
      } else {
        // Conversational answer for general correlation questions
        // If user asked for "sisters brands" or "all variables", list them all
        if ((wantsAllVariables || mentionsSistersBrands) && correlations.length > 0) {
          // Sort by absolute correlation value (strongest first)
          const sortedCorrelations = [...correlations].sort((a, b) => Math.abs(b.correlation) - Math.abs(a.correlation));
          
          const label = mentionsSistersBrands ? 'sisters brands' : 'variables';
          answer = `I found ${correlations.length} ${label} with correlations with ${targetCol}:\n\n`;
          
          // List all variables with their correlations
          sortedCorrelations.forEach((corr, index) => {
            const absR = Math.abs(corr.correlation);
            let strengthText = 'very weak';
            if (absR >= 0.6) strengthText = 'strong';
            else if (absR >= 0.4) strengthText = 'moderate';
            else if (absR >= 0.2) strengthText = 'weak';
            
            const direction = corr.correlation > 0 ? 'positive' : (corr.correlation < 0 ? 'negative' : 'no');
            const sign = corr.correlation > 0 ? '+' : '';
            
            if (absR < 0.001) {
              answer += `${index + 1}. **${corr.variable}**: ~0.000 (no correlation)\n`;
            } else {
              answer += `${index + 1}. **${corr.variable}**: ${sign}${corr.correlation.toFixed(3)} (${strengthText} ${direction})\n`;
            }
          });
          
          if (wantsCharts) {
            answer += `\nI've created visualizations showing these relationships.`;
          } else {
            answer += `\nWould you like me to create a chart to visualize these relationships?`;
          }
        } else if (topCorrelation) {
          // Default behavior: summarize top correlations
          const absR = Math.abs(topCorrelation.correlation);
          let strengthText = 'very weak';
          if (absR >= 0.6) strengthText = 'strong';
          else if (absR >= 0.4) strengthText = 'moderate';
          else if (absR >= 0.2) strengthText = 'weak';
          
          const direction = topCorrelation.correlation > 0 ? 'positive' : 'negative';
          answer = `Based on the analysis, ${topCorrelation.variable} has the ${absR >= 0.6 ? 'strongest' : absR >= 0.4 ? 'stronger' : 'strongest'} ${direction} correlation with ${targetCol} (r ≈ ${topCorrelation.correlation.toFixed(2)}), which is a ${strengthText} relationship.`;
          
          if (topPositive && topPositive !== topCorrelation) {
            answer += ` The strongest positive correlation is with ${topPositive.variable} (r ≈ ${topPositive.correlation.toFixed(2)}).`;
          }
          if (topNegative && topNegative !== topCorrelation) {
            answer += ` The strongest negative correlation is with ${topNegative.variable} (r ≈ ${topNegative.correlation.toFixed(2)}).`;
          }
          
          if (maxResults && correlations.length > maxResults) {
            answer += ` I've analyzed the top ${maxResults} relationships.`;
          } else if (correlations.length > 1) {
            answer += ` I found ${correlations.length} variables with measurable correlations.`;
          }
          
          if (wantsCharts) {
            answer += ` I've created visualizations showing these relationships.`;
          } else {
            answer += ` Would you like me to create a chart to visualize these relationships?`;
          }
        } else {
          answer = `I couldn't find significant correlations between ${targetCol} and other variables in your dataset.`;
        }
      }

      // Only include insights if charts were generated or user explicitly asked
      if (!wantsCharts) {
        insights = []; // Don't show insights if no charts
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

