import { ChartSpec, DataSummary, Insight } from '../shared/schema.js';
import { openai, MODEL } from './openai.js';

const KEY_INSIGHT_MAX_CHARS = 220;

const normalizeInsightText = (value: string) => (value || '').replace(/\s+/g, ' ').trim();
const enforceInsightLimit = (value: string) => {
  if (value.length > KEY_INSIGHT_MAX_CHARS) {
    console.warn(`⚠️ keyInsight exceeded ${KEY_INSIGHT_MAX_CHARS} characters`, {
      length: value.length,
      preview: value,
    });
  }
  return value;
};

export async function generateChartInsights(
  chartSpec: ChartSpec,
  chartData: Record<string, any>[],
  summary: DataSummary,
  chatInsights?: Insight[]
): Promise<{ keyInsight: string }> {
  if (!chartData || chartData.length === 0) {
    return {
      keyInsight: "No data available for analysis"
    };
  }

  // Check if this is a dual-axis line chart
  const isDualAxis = chartSpec.type === 'line' && !!(chartSpec as any).y2;
  const y2Variable = (chartSpec as any).y2;
  const y2Label = (chartSpec as any).y2Label || y2Variable;

  const xValues = chartData.map(row => row[chartSpec.x]).filter(v => v !== null && v !== undefined);
  const yValues = chartData.map(row => row[chartSpec.y]).filter(v => v !== null && v !== undefined);
  const y2Values = isDualAxis ? chartData.map(row => row[y2Variable]).filter(v => v !== null && v !== undefined) : [];

  const numericX: number[] = xValues.map(v => Number(String(v).replace(/[%,,]/g, ''))).filter(v => !isNaN(v));
  const numericY: number[] = yValues.map(v => Number(String(v).replace(/[%,,]/g, ''))).filter(v => !isNaN(v));
  const numericY2: number[] = isDualAxis ? y2Values.map(v => Number(String(v).replace(/[%,,]/g, ''))).filter(v => !isNaN(v)) : [];

  const maxY = numericY.length > 0 ? Math.max(...numericY) : 0;
  const minY = numericY.length > 0 ? Math.min(...numericY) : 0;
  const avgY = numericY.length > 0 ? numericY.reduce((a, b) => a + b, 0) / numericY.length : 0;

  // Helper functions for deterministic, numeric insights
  const percentile = (arr: number[], p: number): number => {
    if (arr.length === 0) return NaN;
    const sorted = [...arr].sort((a, b) => a - b);
    const idx = (sorted.length - 1) * p;
    const lo = Math.floor(idx);
    const hi = Math.ceil(idx);
    if (lo === hi) return sorted[lo];
    return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
  };

  const roundSmart = (v: number): string => {
    if (!isFinite(v)) return String(v);
    const abs = Math.abs(v);
    if (abs >= 100) return v.toFixed(0);
    if (abs >= 10) return v.toFixed(1);
    if (abs >= 1) return v.toFixed(2);
    return v.toFixed(3);
  };

  // Calculate statistics for Y2 if dual-axis
  const maxY2 = numericY2.length > 0 ? Math.max(...numericY2) : 0;
  const minY2 = numericY2.length > 0 ? Math.min(...numericY2) : 0;
  const avgY2 = numericY2.length > 0 ? numericY2.reduce((a, b) => a + b, 0) / numericY2.length : 0;

  // Detect if Y-axis appears to be a percentage column (contains '%' in raw values)
  const yIsPercent = yValues.some(v => typeof v === 'string' && v.includes('%'));
  const y2IsPercent = isDualAxis ? y2Values.some(v => typeof v === 'string' && v.includes('%')) : false;
  const formatY = (val: number): string => yIsPercent ? `${roundSmart(val)}%` : roundSmart(val);
  const formatY2 = (val: number): string => y2IsPercent ? `${roundSmart(val)}%` : roundSmart(val);

  // Calculate standard deviation
  const stdDev = (arr: number[]): number => {
    if (arr.length === 0) return 0;
    const mean = arr.reduce((a, b) => a + b, 0) / arr.length;
    const variance = arr.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / arr.length;
    return Math.sqrt(variance);
  };

  // Find top/bottom performers
  const findTopPerformers = (data: Record<string, any>[], yKey: string, limit: number = 3): Array<{x: any, y: number}> => {
    return data
      .map(row => ({ x: row[chartSpec.x], y: Number(String(row[yKey]).replace(/[%,,]/g, '')) }))
      .filter(item => !isNaN(item.y))
      .sort((a, b) => b.y - a.y)
      .slice(0, limit);
  };

  const findBottomPerformers = (data: Record<string, any>[], yKey: string, limit: number = 3): Array<{x: any, y: number}> => {
    return data
      .map(row => ({ x: row[chartSpec.x], y: Number(String(row[yKey]).replace(/[%,,]/g, '')) }))
      .filter(item => !isNaN(item.y))
      .sort((a, b) => a.y - b.y)
      .slice(0, limit);
  };

  // Calculate percentiles for Y values
  const yP25 = percentile(numericY, 0.25);
  const yP50 = percentile(numericY, 0.5);
  const yP75 = percentile(numericY, 0.75);
  const yP90 = percentile(numericY, 0.9);
  const yStdDev = stdDev(numericY);
  const yMedian = yP50;

  // Calculate statistics for Y2 if dual-axis
  const y2P25 = isDualAxis && numericY2.length > 0 ? percentile(numericY2, 0.25) : NaN;
  const y2P50 = isDualAxis && numericY2.length > 0 ? percentile(numericY2, 0.5) : NaN;
  const y2P75 = isDualAxis && numericY2.length > 0 ? percentile(numericY2, 0.75) : NaN;
  const y2P90 = isDualAxis && numericY2.length > 0 ? percentile(numericY2, 0.9) : NaN;
  const y2StdDev = isDualAxis ? stdDev(numericY2) : 0;
  const y2Median = y2P50;
  const y2CV = isDualAxis && avgY2 !== 0 ? (y2StdDev / Math.abs(avgY2)) * 100 : 0;
  const y2Variability = isDualAxis ? (y2CV > 30 ? 'high' : y2CV > 15 ? 'moderate' : 'low') : '';

  const pearsonR = (xs: number[], ys: number[]): number => {
    const n = Math.min(xs.length, ys.length);
    if (n < 3) return NaN;
    const x = xs.slice(0, n);
    const y = ys.slice(0, n);
    const mean = (a: number[]) => a.reduce((s, v) => s + v, 0) / a.length;
    const mx = mean(x);
    const my = mean(y);
    let num = 0, dx2 = 0, dy2 = 0;
    for (let i = 0; i < n; i++) {
      const dx = x[i] - mx;
      const dy = y[i] - my;
      num += dx * dy;
      dx2 += dx * dx;
      dy2 += dy * dy;
    }
    const den = Math.sqrt(dx2 * dy2);
    return den === 0 ? NaN : num / den;
  };

  // Detect if this is a correlation chart (impact analysis) - check this BEFORE early return
  const isCorrelationChart = (chartSpec as any)._isCorrelationChart === true;
  const targetVariable = (chartSpec as any)._targetVariable || chartSpec.y;
  const factorVariable = (chartSpec as any)._factorVariable || chartSpec.x;

  // If both axes are numeric (especially scatter), produce a quantified X-range tied to high Y
  // BUT: For correlation charts, we need to use the correlation context, not early return
  const bothNumeric = numericX.length > 0 && numericY.length > 0;
  if (bothNumeric && !isCorrelationChart) {
    // Identify X range corresponding to top 20% and top 10% of Y outcomes
    const yP80 = percentile(numericY, 0.8);
    const yP90 = percentile(numericY, 0.9);
    const yP75 = percentile(numericY, 0.75);
    const pairs = chartData
      .map(r => [Number(String(r[chartSpec.x]).replace(/[%,,]/g, '')), Number(String(r[chartSpec.y]).replace(/[%,,]/g, ''))] as [number, number])
      .filter(([vx, vy]) => !isNaN(vx) && !isNaN(vy));
    const top20Pairs = pairs.filter(([, vy]) => vy >= yP80);
    const top10Pairs = pairs.filter(([, vy]) => vy >= yP90);
    const xInTop20 = top20Pairs.map(([vx]) => vx);
    const xInTop10 = top10Pairs.map(([vx]) => vx);
    
    // Calculate optimal X ranges for different performance levels
    const xLow20 = percentile(xInTop20.length ? xInTop20 : numericX, 0.1);
    const xHigh20 = percentile(xInTop20.length ? xInTop20 : numericX, 0.9);
    const xLow10 = percentile(xInTop10.length ? xInTop10 : numericX, 0.1);
    const xHigh10 = percentile(xInTop10.length ? xInTop10 : numericX, 0.9);
    
    // Calculate average X for top performers
    const avgXTop20 = xInTop20.length > 0 ? xInTop20.reduce((a, b) => a + b, 0) / xInTop20.length : NaN;
    const avgXTop10 = xInTop10.length > 0 ? xInTop10.reduce((a, b) => a + b, 0) / xInTop10.length : NaN;

    const r = pearsonR(numericX, numericY);
    const trend = isNaN(r) ? '' : r > 0 ? 'positive' : 'negative';
    const strength = isNaN(r) ? '' : Math.abs(r) > 0.7 ? 'strong' : Math.abs(r) > 0.4 ? 'moderate' : 'weak';

    // Concise insight (single sentence) focused on chart specifics
    const keyInsight = isNaN(r)
      ? `${chartSpec.y} spans ${formatY(minY)}–${formatY(maxY)} (avg ${formatY(avgY)}). Top 20% outcomes are ≥${formatY(yP80)}.`
      : `${strength} ${trend} correlation (r=${roundSmart(r)}) between ${chartSpec.x} and ${chartSpec.y}. ${chartSpec.y} ranges ${formatY(minY)}–${formatY(maxY)} (avg ${formatY(avgY)}).`;

    return { keyInsight };
  }

  // Enhanced statistics for all chart types
  const topPerformers = findTopPerformers(chartData, chartSpec.y, 3);
  const bottomPerformers = findBottomPerformers(chartData, chartSpec.y, 3);
  const topPerformerStr = topPerformers.length > 0 
    ? topPerformers.map(p => `${p.x} (${formatY(p.y)})`).join(', ')
    : 'N/A';
  const bottomPerformerStr = bottomPerformers.length > 0
    ? bottomPerformers.map(p => `${p.x} (${formatY(p.y)})`).join(', ')
    : 'N/A';

  // Y2 statistics for dual-axis charts
  const topPerformersY2 = isDualAxis ? findTopPerformers(chartData, y2Variable, 3) : [];
  const bottomPerformersY2 = isDualAxis ? findBottomPerformers(chartData, y2Variable, 3) : [];
  const topPerformerStrY2 = isDualAxis && topPerformersY2.length > 0 
    ? topPerformersY2.map(p => `${p.x} (${formatY2(p.y)})`).join(', ')
    : 'N/A';
  const bottomPerformerStrY2 = isDualAxis && bottomPerformersY2.length > 0
    ? bottomPerformersY2.map(p => `${p.x} (${formatY2(p.y)})`).join(', ')
    : 'N/A';

  // Calculate coefficient of variation (CV) to measure variability
  const cv = avgY !== 0 ? (yStdDev / Math.abs(avgY)) * 100 : 0;
  const variability = cv > 30 ? 'high' : cv > 15 ? 'moderate' : 'low';

  // For bar/pie charts with categorical X, identify top categories
  const isCategoricalX = numericX.length === 0;
  let topCategories = '';
  if (isCategoricalX && chartData.length > 0) {
    const categoryStats = chartData
      .map(row => ({ x: row[chartSpec.x], y: Number(String(row[chartSpec.y]).replace(/[%,,]/g, '')) }))
      .filter(item => !isNaN(item.y))
      .sort((a, b) => b.y - a.y)
      .slice(0, 3);
    topCategories = categoryStats.map(c => `${c.x} (${formatY(c.y)})`).join(', ');
  }

  // For correlation charts, calculate X-axis statistics for insights
  const numericXValues = chartData.map(row => Number(String(row[chartSpec.x]).replace(/[%,,]/g, ''))).filter(v => !isNaN(v));
  const xP25 = numericXValues.length > 0 ? percentile(numericXValues, 0.25) : NaN;
  const xP50 = numericXValues.length > 0 ? percentile(numericXValues, 0.5) : NaN;
  const xP75 = numericXValues.length > 0 ? percentile(numericXValues, 0.75) : NaN;
  const xP90 = numericXValues.length > 0 ? percentile(numericXValues, 0.9) : NaN;
  const avgX = numericXValues.length > 0 ? numericXValues.reduce((a, b) => a + b, 0) / numericXValues.length : NaN;
  const minX = numericXValues.length > 0 ? Math.min(...numericXValues) : NaN;
  const maxX = numericXValues.length > 0 ? Math.max(...numericXValues) : NaN;
  
  // Find X values corresponding to top Y performers (to identify optimal X range)
  const topYIndices = chartData
    .map((row, idx) => ({ idx, y: Number(String(row[chartSpec.y]).replace(/[%,,]/g, '')) }))
    .filter(item => !isNaN(item.y))
    .sort((a, b) => b.y - a.y)
    .slice(0, Math.min(10, Math.floor(chartData.length * 0.2))) // Top 20% or top 10, whichever is smaller
    .map(item => item.idx);
  const xValuesForTopY = topYIndices.map(idx => Number(String(chartData[idx][chartSpec.x]).replace(/[%,,]/g, ''))).filter(v => !isNaN(v));
  const avgXForTopY = xValuesForTopY.length > 0 ? xValuesForTopY.reduce((a, b) => a + b, 0) / xValuesForTopY.length : NaN;
  const xRangeForTopY = xValuesForTopY.length > 0 ? {
    min: Math.min(...xValuesForTopY),
    max: Math.max(...xValuesForTopY),
    p25: percentile(xValuesForTopY, 0.25),
    p75: percentile(xValuesForTopY, 0.75),
  } : null;

  // Detect if X-axis is percentage
  const xIsPercent = chartData.some(row => {
    const xVal = row[chartSpec.x];
    return typeof xVal === 'string' && xVal.includes('%');
  });
  const formatX = (val: number): string => {
    if (isNaN(val)) return 'N/A';
    if (xIsPercent) return `${roundSmart(val)}%`;
    return roundSmart(val);
  };

  // Fallback to model for non-numeric cases; request quantified insights explicitly
  const correlationContext = isCorrelationChart ? `
CRITICAL: This is a CORRELATION/IMPACT ANALYSIS chart.
- Y-axis (${chartSpec.y}) = TARGET VARIABLE we want to IMPROVE (${targetVariable})
- X-axis (${chartSpec.x}) = FACTOR VARIABLE we can CHANGE (${factorVariable})
- Suggestions MUST focus on: "How to change ${factorVariable} to improve ${targetVariable}"

X-AXIS STATISTICS (${factorVariable} - what we can change):
- Range: ${formatX(minX)} to ${formatX(maxX)}
- Average: ${formatX(avgX)}
- Median: ${formatX(xP50)}
- 25th percentile: ${formatX(xP25)}, 75th percentile: ${formatX(xP75)}, 90th percentile: ${formatX(xP90)}
${xRangeForTopY ? `- Optimal ${factorVariable} range for top Y performers: ${formatX(xRangeForTopY.min)}-${formatX(xRangeForTopY.max)} (avg: ${formatX(avgXForTopY)}, 25th-75th percentile range: ${formatX(xRangeForTopY.p25)}-${formatX(xRangeForTopY.p75)})` : ''}

SUGGESTION FORMAT:
- Must explain how to CHANGE ${factorVariable} (X-axis) to IMPROVE ${targetVariable} (Y-axis)
- Use specific X-axis values/ranges from statistics above
- NEVER use percentile labels like "P75", "P90", "P25", "P75 level", "P90 level", "P75 value", "P90 value" - ONLY use the numeric values themselves
- Example: "To improve ${targetVariable} to ${formatY(yP75)} or higher, adjust ${factorVariable} to ${formatX(xRangeForTopY?.p75 || xP75)}" (NOT "to P75 level (${formatY(yP75)})")
- Focus on actionable steps: "Adjust ${factorVariable} from current average of ${formatX(avgX)} to target range of ${formatX(xRangeForTopY?.p25 || xP25)}-${formatX(xRangeForTopY?.p75 || xP75)}"

` : '';

  // Build chat insights context if available
  const chatInsightsContext = chatInsights && chatInsights.length > 0
    ? `\n\nRELEVANT CHAT-LEVEL INSIGHTS (use these to inform the chart insight):
${chatInsights.map((insight, idx) => `${idx + 1}. ${insight.text}`).join('\n')}

IMPORTANT: The keyInsight should be a concise summary (exactly one sentence, ≤${KEY_INSIGHT_MAX_CHARS} chars) that relates this specific chart to the relevant chat-level insights above. Focus on insights that mention variables in this chart (${chartSpec.x}, ${chartSpec.y}${isDualAxis ? `, ${y2Label}` : ''}).`
    : '';

  const prompt = `Return JSON with exactly one short field for this chart: keyInsight. It must be one line (a single sentence, ≤${KEY_INSIGHT_MAX_CHARS} chars), chart-specific, and include concrete numbers. No bullets or line breaks.

CHART CONTEXT
- Type: ${chartSpec.type}
- Title: ${chartSpec.title}
- X: ${chartSpec.x}${isCorrelationChart ? ' (FACTOR)' : ''}
- Y: ${chartSpec.y}${isCorrelationChart ? ' (TARGET)' : ''}${isDualAxis ? ` | Y2: ${y2Label}` : ''}
- Points: ${chartData.length}
- Y stats: ${formatY(minY)}–${formatY(maxY)} (avg ${formatY(avgY)}, 75th percentile: ${formatY(yP75)})${isDualAxis ? ` | Y2: ${formatY2(minY2)}–${formatY2(maxY2)} (avg ${formatY2(avgY2)})` : ''}

${correlationContext}${chatInsightsContext}

OUTPUT JSON (exact keys only):
{
  "keyInsight": "One sentence, chart-specific with numbers${chatInsights && chatInsights.length > 0 ? ' that summarizes relevant chat insights' : ''}. NEVER use percentile labels like P75, P90, P25 - only use numeric values. Do not exceed ${KEY_INSIGHT_MAX_CHARS} characters."
}`;

  try {
    const response = await openai.chat.completions.create({
      model: MODEL as string,
      messages: [
        {
          role: 'system',
          content: `You are a precise data analyst. Output JSON with exactly one short field: keyInsight. It must be a single sentence (≤${KEY_INSIGHT_MAX_CHARS} chars), chart-specific, include numbers, and be actionable. NEVER use percentile labels like P75, P90, P25, P75 level, P90 level - only use numeric values. No bullets or line breaks.`
        },
        { role: 'user', content: prompt },
      ],
      response_format: { type: 'json_object' },
      temperature: 0.35,
      max_tokens: 220,
    });

    const content = response.choices[0].message.content || '{}';
    const result = JSON.parse(content);

    // Format multiple insights into a single string for keyInsight
    if (result.insights && Array.isArray(result.insights) && result.insights.length > 0) {
      // For dual-axis charts, validate that both variables are covered
      if (isDualAxis) {
        const insightsText = result.insights.map((i: any) => 
          `${i.title || ''} ${i.observation || ''} ${i.text || ''}`.toLowerCase()
        ).join(' ');
        
        const mentionsY = insightsText.includes(chartSpec.y.toLowerCase());
        const mentionsY2 = insightsText.includes(y2Label.toLowerCase()) || insightsText.includes(y2Variable.toLowerCase());
        
        // If only one variable is mentioned, add a fallback insight for the missing one
        if (mentionsY && !mentionsY2) {
          console.warn(`⚠️ Dual-axis chart insights only mention ${chartSpec.y}, missing ${y2Label}. Adding fallback insight.`);
          
          // Generate comprehensive fallback insight for y2 using all calculated statistics
          const y2TopPerformer = topPerformersY2.length > 0 ? topPerformersY2[0] : null;
          const y2BottomPerformer = bottomPerformersY2.length > 0 ? bottomPerformersY2[0] : null;
          
          // Build comprehensive observation using all available statistics
          let observationParts: string[] = [];
          
          // Range and central tendency
          observationParts.push(`${y2Label} ranges from ${formatY2(minY2)} to ${formatY2(maxY2)} (average: ${formatY2(avgY2)}, median: ${formatY2(y2Median)})`);
          
          // Variability
          if (!isNaN(y2CV)) {
            observationParts.push(`demonstrates ${y2Variability} variability (CV: ${roundSmart(y2CV)}%)`);
          }
          
          // Percentiles
          if (!isNaN(y2P90) && !isNaN(y2P25)) {
            observationParts.push(`with 90th percentile at ${formatY2(y2P90)} and 25th percentile at ${formatY2(y2P25)}`);
          }
          
          // Top performers
          if (y2TopPerformer) {
            observationParts.push(`Peak performance observed at ${y2TopPerformer.x} (${formatY2(y2TopPerformer.y)})`);
          }
          
          // Bottom performers
          if (y2BottomPerformer) {
            observationParts.push(`lowest value recorded at ${y2BottomPerformer.x} (${formatY2(y2BottomPerformer.y)})`);
          }
          
          const fallbackObservation = observationParts.join('. ') + '.';
          
          result.insights.push({
            title: `**${y2Label} Performance Analysis**`,
            observation: fallbackObservation,
            whyItMatters: `Monitoring ${y2Label} performance is critical for understanding overall business trends and identifying optimization opportunities. Consistent performance above benchmark levels indicates strong operational efficiency.`
          });
        } else if (!mentionsY && mentionsY2) {
          console.warn(`⚠️ Dual-axis chart insights only mention ${y2Label}, missing ${chartSpec.y}. Adding fallback insight.`);
          
          // Generate comprehensive fallback insight for y using all calculated statistics
          const yTopPerformer = topPerformers.length > 0 ? topPerformers[0] : null;
          const yBottomPerformer = bottomPerformers.length > 0 ? bottomPerformers[0] : null;
          
          // Build comprehensive observation using all available statistics
          let observationParts: string[] = [];
          
          // Range and central tendency
          observationParts.push(`${chartSpec.y} ranges from ${formatY(minY)} to ${formatY(maxY)} (average: ${formatY(avgY)}, median: ${formatY(yMedian)})`);
          
          // Variability
          if (!isNaN(cv)) {
            observationParts.push(`demonstrates ${variability} variability (CV: ${roundSmart(cv)}%)`);
          }
          
          // Percentiles
          if (!isNaN(yP90) && !isNaN(yP25)) {
            observationParts.push(`with 90th percentile at ${formatY(yP90)} and 25th percentile at ${formatY(yP25)}`);
          }
          
          // Top performers
          if (yTopPerformer) {
            observationParts.push(`Peak performance observed at ${yTopPerformer.x} (${formatY(yTopPerformer.y)})`);
          }
          
          // Bottom performers
          if (yBottomPerformer) {
            observationParts.push(`lowest value recorded at ${yBottomPerformer.x} (${formatY(yBottomPerformer.y)})`);
          }
          
          const fallbackObservation = observationParts.join('. ') + '.';
          
          result.insights.unshift({
            title: `**${chartSpec.y} Performance Analysis**`,
            observation: fallbackObservation,
            whyItMatters: `Monitoring ${chartSpec.y} performance is critical for understanding overall business trends and identifying optimization opportunities. Consistent performance above benchmark levels indicates strong operational efficiency.`
          });
        }
      }
      
      // Build ultra-concise per-chart keyInsight (single sentence)
      const first = result.insights[0] || {};
      const title = normalizeInsightText(first.title || 'Insight');
      const observation = normalizeInsightText(first.observation || first.text || '');
      const combined = normalizeInsightText([title, observation].filter(Boolean).join(': '));
      const conciseInsight = enforceInsightLimit(combined);

      return {
        keyInsight: conciseInsight,
      };
    }

    // Fallback to single insight format if AI didn't return array
    const normalized = normalizeInsightText(result.keyInsight || "Data shows interesting patterns worth investigating");
    return {
      keyInsight: enforceInsightLimit(normalized),
    };
  } catch (error) {
    console.error('Error generating chart insights:', error);
    return {
      keyInsight: `This ${chartSpec.type} chart shows ${chartData.length} data points with values ranging from ${minY.toFixed(2)} to ${maxY.toFixed(2)}`
    };
  }
}


