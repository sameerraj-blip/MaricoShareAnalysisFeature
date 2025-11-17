import { ChartSpec, Insight, DataSummary } from '../../shared/schema.js';
import { openai, MODEL } from './openai.js';
import { generateChartInsights } from './insightGenerator.js';

// Helper to clean numeric values (strip %, commas, etc.)
function toNumber(value: any): number {
  if (value === null || value === undefined || value === '') return NaN;
  const cleaned = String(value).replace(/[%,]/g, '').trim();
  return Number(cleaned);
}

interface CorrelationResult {
  variable: string;
  correlation: number;
  nPairs?: number;
}

// Calculate linear regression (slope and intercept) for trend line
function linearRegression(xValues: number[], yValues: number[]): { slope: number; intercept: number } | null {
  const n = Math.min(xValues.length, yValues.length);
  if (n === 0) return null;

  let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
  for (let i = 0; i < n; i++) {
    const x = xValues[i];
    const y = yValues[i];
    if (isNaN(x) || isNaN(y)) continue;
    sumX += x;
    sumY += y;
    sumXY += x * y;
    sumX2 += x * x;
  }

  const denominator = n * sumX2 - sumX * sumX;
  if (denominator === 0) return null;

  const slope = (n * sumXY - sumX * sumY) / denominator;
  const intercept = (sumY - slope * sumX) / n;

  return { slope, intercept };
}

export async function analyzeCorrelations(
  data: Record<string, any>[],
  targetVariable: string,
  numericColumns: string[],
  filter: 'all' | 'positive' | 'negative' = 'all',
  sortOrder?: 'ascending' | 'descending',
  chatInsights?: Insight[],
  maxResults?: number
): Promise<{ charts: ChartSpec[]; insights: Insight[] }> {
  console.log('=== CORRELATION ANALYSIS DEBUG ===');
  console.log('Target variable:', targetVariable);
  console.log('Numeric columns to analyze:', numericColumns);
  console.log('Data rows:', data.length);
  
  // Calculate correlations
  const correlations = calculateCorrelations(data, targetVariable, numericColumns);
  console.log('Correlations calculated:', correlations);
  console.log('=== RAW CORRELATION VALUES DEBUG ===');
  correlations.forEach((corr, idx) => {
    console.log(`RAW ${idx + 1}. ${corr.variable}: ${corr.correlation} (${corr.correlation > 0 ? 'POSITIVE' : 'NEGATIVE'})`);
  });
  console.log('=== END RAW CORRELATION DEBUG ===');

  if (correlations.length === 0) {
    console.error('No correlations found!');
    return { charts: [], insights: [] };
  }

  // Apply filter if requested
  let filteredCorrelations = correlations;
  if (filter === 'positive') {
    filteredCorrelations = correlations.filter(c => c.correlation > 0);
    console.log(`Filtering: Showing only POSITIVE correlations (${filteredCorrelations.length} of ${correlations.length})`);
  } else if (filter === 'negative') {
    filteredCorrelations = correlations.filter(c => c.correlation < 0);
    console.log(`Filtering: Showing only NEGATIVE correlations (${filteredCorrelations.length} of ${correlations.length})`);
  }

  if (filteredCorrelations.length === 0) {
    const filterMessage = filter === 'positive' 
      ? 'No positive correlations found.' 
      : filter === 'negative' 
      ? 'No negative correlations found.' 
      : 'No correlations found.';
    console.warn(filterMessage);
    return { 
      charts: [], 
      insights: [{
        id: 1,
        text: `**No ${filter === 'positive' ? 'positive' : 'negative'} correlations found:** ${filterMessage} All correlations with ${targetVariable} are ${filter === 'positive' ? 'negative' : 'positive'}.`
      }] 
    };
  }

  // Get top correlations (by absolute value, then apply filter)
  const sortedCorrelations = filteredCorrelations.sort((a, b) => Math.abs(b.correlation) - Math.abs(a.correlation));
  
  // Apply limit if specified (e.g., "top 10"), otherwise show all
  const topCorrelations = maxResults 
    ? sortedCorrelations.slice(0, maxResults)
    : sortedCorrelations;
  
  if (maxResults) {
    console.log(`Limiting to top ${maxResults} correlations as requested`);
  }

  // Generate scatter plots for top 3 correlations
  // IMPORTANT: For correlation/impact questions, target variable ALWAYS goes on Y-axis
  // X-axis = factor variable (what we can change), Y-axis = target variable (what we want to improve)
  const scatterCharts: ChartSpec[] = topCorrelations.slice(0, 3).map((corr, idx) => {
    const scatterData = data
      .map((row) => ({
        [corr.variable]: toNumber(row[corr.variable]),
        [targetVariable]: toNumber(row[targetVariable]),
      }))
      .filter((row) => !isNaN(row[corr.variable]) && !isNaN(row[targetVariable]))
      .slice(0, 1000);
    
    // For correlation analysis: X-axis = factor variable, Y-axis = target variable
    // This ensures recommendations are about "how to change X to improve Y"
    const xAxis = corr.variable;  // Factor we can change
    const yAxis = targetVariable; // Target we want to improve
    
    // Calculate smart axis domains with padding (only if we have valid data)
    let xDomain: [number, number] | undefined;
    let yDomain: [number, number] | undefined;
    
    // Calculate trend line
    let trendLine: Array<Record<string, number>> | undefined;
    
    if (scatterData.length > 0) {
      const xValues = scatterData.map(row => row[xAxis]);
      const yValues = scatterData.map(row => row[yAxis]);
      
      const xMin = Math.min(...xValues);
      const xMax = Math.max(...xValues);
      const yMin = Math.min(...yValues);
      const yMax = Math.max(...yValues);
      
      // Add 10% padding to the range (or 1 if range is 0)
      const xRange = xMax - xMin;
      const yRange = yMax - yMin;
      const xPadding = xRange > 0 ? xRange * 0.1 : 1;
      const yPadding = yRange > 0 ? yRange * 0.1 : 1;
      
      // Only set domains if values are finite
      if (isFinite(xMin) && isFinite(xMax)) {
        xDomain = [xMin - xPadding, xMax + xPadding];
      }
      if (isFinite(yMin) && isFinite(yMax)) {
        yDomain = [yMin - yPadding, yMax + yPadding];
      }
      
      // Calculate linear regression for trend line
      const regression = linearRegression(xValues, yValues);
      
      if (regression) {
        // Calculate trend line endpoints using the calculated domain (or actual min/max if domain not set)
        const xMinForLine = xDomain ? xDomain[0] : xMin;
        const xMaxForLine = xDomain ? xDomain[1] : xMax;
        const yAtMin = regression.slope * xMinForLine + regression.intercept;
        const yAtMax = regression.slope * xMaxForLine + regression.intercept;
        
        trendLine = [
          { [xAxis]: xMinForLine, [yAxis]: yAtMin },
          { [xAxis]: xMaxForLine, [yAxis]: yAtMax },
        ];
      }
    }
    
    console.log(`Scatter chart ${idx}: ${corr.variable} (X-axis, factor) vs ${targetVariable} (Y-axis, target), data points: ${scatterData.length}${xDomain ? `, xDomain: [${xDomain[0].toFixed(1)}, ${xDomain[1].toFixed(1)}]` : ''}${yDomain ? `, yDomain: [${yDomain[0].toFixed(1)}, ${yDomain[1].toFixed(1)}]` : ''}${trendLine ? ', trend line: yes' : ', trend line: no'}`);
    
    return {
      type: 'scatter',
      title: `${corr.variable} vs ${targetVariable} (r=${corr.correlation.toFixed(2)})`,
      x: xAxis,  // Factor variable (what we can change)
      y: yAxis,  // Target variable (what we want to improve)
      xLabel: xAxis,
      yLabel: yAxis,
      data: scatterData,
      ...(xDomain && { xDomain }),
      ...(yDomain && { yDomain }),
      ...(trendLine && { trendLine }),
      // Mark this as a correlation chart for insight generation
      _isCorrelationChart: true,
      _targetVariable: targetVariable,
      _factorVariable: corr.variable,
    };
  });

  // Only add bar chart if we have multiple correlations
  const charts: ChartSpec[] = [...scatterCharts];
  
  if (topCorrelations.length > 1) {
    // IMPORTANT: Do NOT modify correlation signs - show actual positive/negative values
    console.log('=== BAR CHART CORRELATION VALUES DEBUG ===');
    topCorrelations.forEach((corr, idx) => {
      console.log(`${idx + 1}. ${corr.variable}: ${corr.correlation} (${corr.correlation > 0 ? 'POSITIVE' : 'NEGATIVE'})`);
    });
    console.log('=== END BAR CHART DEBUG ===');
    
    // Sort by correlation value only if user explicitly requested a sort order
    let sortedForBar: typeof topCorrelations;
    if (sortOrder === 'descending') {
      // Descending: highest to lowest (positive to negative)
      sortedForBar = [...topCorrelations].sort((a, b) => b.correlation - a.correlation);
    } else if (sortOrder === 'ascending') {
      // Ascending: lowest to highest (negative to positive)
      sortedForBar = [...topCorrelations].sort((a, b) => a.correlation - b.correlation);
    } else {
      // No explicit sort order requested - use default order (already sorted by absolute value)
      sortedForBar = topCorrelations;
    }
    
    const correlationBarChart: ChartSpec = {
      type: 'bar',
      title: `Correlation Between ${targetVariable} and Variables`,
      x: 'variable',
      y: 'correlation',
      xLabel: 'variable',
      yLabel: 'correlation',
      data: sortedForBar.map((corr) => ({
        variable: corr.variable,
        correlation: corr.correlation, // CRITICAL: Keep original sign (positive/negative)
      })),
    };
    
    console.log('=== FINAL BAR CHART DATA DEBUG ===');
    console.log('Bar chart data being sent to frontend:');
    const barData = (correlationBarChart.data || []) as Array<{ variable: string; correlation: number }>;
    barData.forEach((item, idx) => {
      const corrVal = Number(item.correlation);
      console.log(`FINAL ${idx + 1}. ${item.variable}: ${corrVal} (${corrVal > 0 ? 'POSITIVE' : 'NEGATIVE'})`);
    });
    console.log('=== END FINAL BAR CHART DEBUG ===');
    
    charts.push(correlationBarChart);
  }

  console.log('Total charts generated:', charts.length);
  console.log('=== END CORRELATION DEBUG ===');

  // Enrich each chart with keyInsight and recommendation
  try {
    const summaryStub: DataSummary = {
      rowCount: data.length,
      columnCount: Object.keys(data[0] || {}).length,
      columns: Object.keys(data[0] || {}).map((name) => ({ name, type: typeof (data[0] || {})[name], sampleValues: [] as any })),
      numericColumns: numericColumns,
      dateColumns: [],
    } as unknown as DataSummary;

    const chartsWithInsights = await Promise.all(
      charts.map(async (c) => {
        const chartInsights = await generateChartInsights(c, c.data || [], summaryStub, chatInsights);
        return { ...c, keyInsight: chartInsights.keyInsight } as ChartSpec;
      })
    );
    charts.splice(0, charts.length, ...chartsWithInsights);
  } catch (e) {
    console.error('Failed to enrich correlation charts with insights:', e);
  }

  // Generate AI insights about correlations (use the same correlations shown in charts)
  // Pass data and summary for quantified recommendations
  const summaryStub: DataSummary = {
    rowCount: data.length,
    columnCount: Object.keys(data[0] || {}).length,
    columns: Object.keys(data[0] || {}).map((name) => ({ name, type: typeof (data[0] || {})[name], sampleValues: [] as any })),
    numericColumns: numericColumns,
    dateColumns: [],
  } as unknown as DataSummary;
  // Pass topCorrelations (same as used in charts) to ensure insights match what's displayed
  const insights = await generateCorrelationInsights(targetVariable, topCorrelations, data, summaryStub, filter);

  return { charts, insights };
}

function calculateCorrelations(
  data: Record<string, any>[],
  targetVariable: string,
  numericColumns: string[]
): CorrelationResult[] {
  const correlations: CorrelationResult[] = [];

  // Precompute target values (keep row alignment; NA preserved as NaN)
  const targetValuesAllRows = data.map((row) => toNumber(row[targetVariable]));
  const hasAnyTarget = targetValuesAllRows.some((v) => !isNaN(v));
  if (!hasAnyTarget) return [];

  for (const col of numericColumns) {
    if (col === targetVariable) continue;

    // Build row-aligned pairs; skip rows where either side is NA (pairwise deletion)
    const x: number[] = []; // target
    const y: number[] = []; // column
    for (let i = 0; i < data.length; i++) {
      const tv = targetValuesAllRows[i];
      const cv = toNumber(data[i][col]);
      if (!isNaN(tv) && !isNaN(cv)) {
        x.push(tv);
        y.push(cv);
      }
    }

    if (x.length === 0) continue;

    // Calculate Pearson correlation on paired arrays
    const correlation = pearsonCorrelation(x, y);

    if (!isNaN(correlation)) {
      correlations.push({ variable: col, correlation, nPairs: x.length });
    }
  }

  return correlations;
}

function pearsonCorrelation(x: number[], y: number[]): number {
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

async function generateCorrelationInsights(
  targetVariable: string,
  correlations: CorrelationResult[],
  data?: Record<string, any>[],
  summary?: DataSummary,
  filter: 'all' | 'positive' | 'negative' = 'all'
): Promise<Insight[]> {
  // Ensure filter is defined (defensive check)
  const correlationFilter: 'all' | 'positive' | 'negative' = filter || 'all';
  
  // Calculate quantified statistics for correlations if data is available
  // Include statistics for all correlations (or top 10 if there are many) to help AI generate better insights
  let quantifiedStats = '';
  if (data && data.length > 0 && summary) {
    const correlationsForStats = correlations.slice(0, Math.min(correlations.length, 10));
    quantifiedStats = '\n\nQUANTIFIED STATISTICS FOR FACTORS:\n';
    
    for (const corr of correlationsForStats) {
      const factorValues = data
        .map(row => Number(String(row[corr.variable]).replace(/[%,,]/g, '')))
        .filter(v => !isNaN(v));
      const targetValues = data
        .map(row => Number(String(row[targetVariable]).replace(/[%,,]/g, '')))
        .filter(v => !isNaN(v));
      
      if (factorValues.length > 0 && targetValues.length > 0) {
        const factorAvg = factorValues.reduce((a, b) => a + b, 0) / factorValues.length;
        const factorMin = Math.min(...factorValues);
        const factorMax = Math.max(...factorValues);
        const factorP25 = factorValues.sort((a, b) => a - b)[Math.floor(factorValues.length * 0.25)];
        const factorP75 = factorValues.sort((a, b) => a - b)[Math.floor(factorValues.length * 0.75)];
        
        const targetAvg = targetValues.reduce((a, b) => a + b, 0) / targetValues.length;
        const targetMin = Math.min(...targetValues);
        const targetMax = Math.max(...targetValues);
        const targetP75 = targetValues.sort((a, b) => a - b)[Math.floor(targetValues.length * 0.75)];
        const targetP90 = targetValues.sort((a, b) => a - b)[Math.floor(targetValues.length * 0.9)];
        
        // Find factor values for top target performers
        const pairs = data
          .map(row => ({
            factor: Number(String(row[corr.variable]).replace(/[%,,]/g, '')),
            target: Number(String(row[targetVariable]).replace(/[%,,]/g, ''))
          }))
          .filter(p => !isNaN(p.factor) && !isNaN(p.target));
        
        const topTargetPairs = pairs
          .sort((a, b) => b.target - a.target)
          .slice(0, Math.min(10, Math.floor(pairs.length * 0.2)));
        
        const optimalFactorRange = topTargetPairs.length > 0 ? {
          min: Math.min(...topTargetPairs.map(p => p.factor)),
          max: Math.max(...topTargetPairs.map(p => p.factor)),
          avg: topTargetPairs.reduce((sum, p) => sum + p.factor, 0) / topTargetPairs.length
        } : null;
        
        const formatValue = (val: number, isPercent: boolean = false): string => {
          if (!isFinite(val)) return 'N/A';
          const abs = Math.abs(val);
          const fmt = abs >= 100 ? val.toFixed(0) : abs >= 10 ? val.toFixed(1) : abs >= 1 ? val.toFixed(2) : val.toFixed(3);
          return isPercent ? `${fmt}%` : fmt;
        };
        
        const factorIsPercent = data.some(row => typeof row[corr.variable] === 'string' && row[corr.variable].includes('%'));
        const targetIsPercent = data.some(row => typeof row[targetVariable] === 'string' && row[targetVariable].includes('%'));
        
        quantifiedStats += `\n${corr.variable} (r=${corr.correlation.toFixed(2)}):
- Factor range: ${formatValue(factorMin, factorIsPercent)} to ${formatValue(factorMax, factorIsPercent)} (avg: ${formatValue(factorAvg, factorIsPercent)}, 25th-75th percentile range: ${formatValue(factorP25, factorIsPercent)}-${formatValue(factorP75, factorIsPercent)})
- Target range: ${formatValue(targetMin, targetIsPercent)} to ${formatValue(targetMax, targetIsPercent)} (avg: ${formatValue(targetAvg, targetIsPercent)}, 75th percentile: ${formatValue(targetP75, targetIsPercent)}, 90th percentile: ${formatValue(targetP90, targetIsPercent)})
${optimalFactorRange ? `- Optimal ${corr.variable} range for top ${targetVariable} performers: ${formatValue(optimalFactorRange.min, factorIsPercent)}-${formatValue(optimalFactorRange.max, factorIsPercent)} (avg: ${formatValue(optimalFactorRange.avg, factorIsPercent)})` : ''}
`;
      }
    }
  }
  
  // Determine dynamic insight limit based on number of correlations
  // Generate insights for all correlations shown (matching what's in charts)
  const insightCount = correlations.length;
  
  const filterContext = correlationFilter === 'positive' 
    ? '\nIMPORTANT: The user specifically requested ONLY POSITIVE correlations. All correlations shown are positive. Focus your insights on these positive relationships only.'
    : correlationFilter === 'negative'
    ? '\nIMPORTANT: The user specifically requested ONLY NEGATIVE correlations. All correlations shown are negative. Focus your insights on these negative relationships only.'
    : '';

  const prompt = `Analyze these correlations with ${targetVariable}.${filterContext}

DATA HANDLING RULES (must follow exactly):
- Pearson correlation using pairwise deletion: if either value is NA on a row, exclude that row; do not impute.
- Use the EXACT signed correlation values provided; never change the sign.
- Cover ALL variables at least once in the insights (do not omit any listed below).
${correlationFilter === 'positive' ? '- All correlations shown are POSITIVE (user filtered out negative ones).' : ''}
${correlationFilter === 'negative' ? '- All correlations shown are NEGATIVE (user filtered out positive ones).' : ''}

VALUES (variable: r, nPairs):
${correlations.map((c) => `- ${c.variable}: ${c.correlation.toFixed(3)}, n=${c.nPairs ?? 'NA'}`).join('\n')}
${quantifiedStats}

CRITICAL CONTEXT:
- ${targetVariable} is the TARGET VARIABLE we want to IMPROVE (Y-axis)
- The listed variables are FACTOR VARIABLES we can CHANGE (X-axis)
- Suggestions MUST explain: "How to change [FACTOR] to improve [TARGET]"

Write exactly ${insightCount} insights (one for each variable listed above). Each must include:
1. **Bold headline** with the key finding
2. Exact r and nPairs values
3. Interpretation of the relationship
4. **Actionable suggestion** that includes:
   - Keep the current contextual suggestion (explaining the relationship)
   - ADD a quantified suggestion with specific targets: "To improve ${targetVariable} to [target value], adjust [factor variable] to [specific value/range]"
   - Use specific numbers from the quantified statistics above (optimal ranges, percentiles, averages)
   - NEVER use percentile labels like "P75", "P90", "P25", "P75 level", "P90 level", "P75 value", "P90 value" - ONLY use the numeric values themselves
   - Example format: "**Current suggestion:** [explain relationship]. **Quantified Action:** To improve ${targetVariable} to [target value], adjust [factor] from current average ([current]) to optimal range ([optimal range]) or target value ([target value])."
5. End with: "Reminder: Correlation does not imply causation."

IMPORTANT: Generate exactly ${insightCount} insights - one for each of the ${insightCount} variables listed above, in order of correlation strength (strongest first).

Output JSON only: {"insights":[{"text":"..."}]}`;

  const response = await openai.chat.completions.create({
    model: MODEL || "gpt-4o",
    messages: [
      {
        role: 'system',
        content: 'You are a senior data analyst providing detailed correlation insights. Be specific, use correlation values, and provide actionable suggestions. Always end correlation insights with exactly: "Reminder: Correlation does not imply causation." (never use "correlation != causation" or variations). Output valid JSON.',
      },
      {
        role: 'user',
        content: prompt,
      },
    ],
    response_format: { type: 'json_object' },
    temperature: 0.7,
    // Scale tokens dynamically with insight count: base 2000 + ~200 per additional insight beyond 7
    // Cap at 10,000 to allow for many insights, but scales down for fewer insights
    max_tokens: Math.min(2000 + Math.max(0, (insightCount - 7) * 200), 10000),
  });

  const content = response.choices[0].message.content || '{}';

  try {
    const parsed = JSON.parse(content);
    const insightArray = parsed.insights || [];
    
    // Return all insights up to the expected count (no hard limit)
    // If AI generated fewer, return what we got; if more, cap at expected count
    return insightArray.slice(0, insightCount).map((item: any, index: number) => ({
      id: index + 1,
      text: item.text || item.insight || String(item),
    }));
  } catch (error) {
    console.error('Error parsing correlation insights:', error);
    return [];
  }
}
