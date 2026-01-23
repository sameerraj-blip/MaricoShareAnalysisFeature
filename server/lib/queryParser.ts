import { openai, MODEL } from './openai.js';
import { ParsedQuery, TimeFilter, ValueFilter, ExclusionFilter, AggregationRequest, SortRequest, TopBottomRequest } from '../shared/queryTypes.js';
import { DataSummary, Message } from '../shared/schema.js';
import { detectPeriodFromQuery, DatePeriod, extractDatesFromQuery, ExtractedDate } from './dateUtils.js';

interface QueryParserResult extends ParsedQuery {
  confidence: number;
}

type Nullable<T> = { [K in keyof T]?: T[K] | null };

const MONTH_ALIASES: Record<string, string> = {
  jan: 'January',
  january: 'January',
  feb: 'February',
  february: 'February',
  mar: 'March',
  march: 'March',
  apr: 'April',
  april: 'April',
  may: 'May',
  jun: 'June',
  june: 'June',
  jul: 'July',
  july: 'July',
  aug: 'August',
  august: 'August',
  sep: 'September',
  sept: 'September',
  september: 'September',
  oct: 'October',
  october: 'October',
  nov: 'November',
  november: 'November',
  dec: 'December',
  december: 'December',
};

function normaliseMonthName(name: string): string | undefined {
  const key = name.trim().toLowerCase();
  return MONTH_ALIASES[key] || undefined;
}

function sanitiseTimeFilters(filters?: Nullable<TimeFilter>[]): TimeFilter[] | undefined {
  if (!filters) return undefined;
  const cleaned: TimeFilter[] = [];
  for (const filter of filters) {
    if (!filter || !filter.type) continue;
    const entry: TimeFilter = { type: filter.type } as TimeFilter;
    if (filter.column) entry.column = filter.column;
    if (filter.years) entry.years = filter.years.filter((y): y is number => typeof y === 'number' && !isNaN(y));
    if (filter.months) {
      const months = filter.months
        .map((m) => (typeof m === 'string' ? normaliseMonthName(m) : undefined))
        .filter((m): m is string => Boolean(m));
      if (months.length) entry.months = months;
    }
    if (filter.quarters) {
      const quarters = filter.quarters.filter((q): q is 1 | 2 | 3 | 4 => [1, 2, 3, 4].includes(q as number));
      if (quarters.length) entry.quarters = quarters;
    }
    if (filter.startDate) entry.startDate = filter.startDate;
    if (filter.endDate) entry.endDate = filter.endDate;
    if (filter.relative && filter.relative.amount && filter.relative.unit && filter.relative.direction) {
      entry.relative = {
        unit: filter.relative.unit,
        direction: filter.relative.direction,
        amount: filter.relative.amount,
      };
    }
    cleaned.push(entry);
  }
  return cleaned.length ? cleaned : undefined;
}

/**
 * Converts Indian number units (crore, lakh) to actual numbers
 */
function convertIndianNumberUnits(value: number | string | null | undefined): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number') return value;
  
  const str = String(value).toLowerCase().trim();
  
  // Handle crore (10 million)
  const croreMatch = str.match(/(\d+(?:\.\d+)?)\s*crore/i);
  if (croreMatch) {
    const num = parseFloat(croreMatch[1]);
    if (!isNaN(num)) {
      return num * 10000000; // 10 million
    }
  }
  
  // Handle lakh (100 thousand)
  const lakhMatch = str.match(/(\d+(?:\.\d+)?)\s*lakh/i);
  if (lakhMatch) {
    const num = parseFloat(lakhMatch[1]);
    if (!isNaN(num)) {
      return num * 100000; // 100 thousand
    }
  }
  
  // Try to parse as regular number
  const num = parseFloat(str);
  return !isNaN(num) ? num : null;
}

function sanitiseValueFilters(filters?: Nullable<ValueFilter>[]): ValueFilter[] | undefined {
  if (!filters) return undefined;
  const cleaned: ValueFilter[] = [];
  for (const filter of filters) {
    if (!filter || !filter.column || !filter.operator) continue;
    const entry: ValueFilter = {
      column: filter.column,
      operator: filter.operator,
    };
    
    // Convert Indian number units if present
    const convertedValue = convertIndianNumberUnits(filter.value);
    if (convertedValue !== null) {
      entry.value = convertedValue;
    }
    
    const convertedValue2 = convertIndianNumberUnits(filter.value2);
    if (convertedValue2 !== null) {
      entry.value2 = convertedValue2;
    }
    
    if (filter.reference) entry.reference = filter.reference;
    cleaned.push(entry);
  }
  return cleaned.length ? cleaned : undefined;
}

function sanitiseExclusionFilters(filters?: Nullable<ExclusionFilter>[]): ExclusionFilter[] | undefined {
  if (!filters) return undefined;
  const cleaned: ExclusionFilter[] = [];
  for (const filter of filters) {
    if (!filter || !filter.column || !Array.isArray(filter.values)) continue;
    const values = filter.values.filter((v) => v !== null && v !== undefined);
    if (!values.length) continue;
    cleaned.push({ column: filter.column, values });
  }
  return cleaned.length ? cleaned : undefined;
}

function sanitiseAggregations(aggs?: Nullable<AggregationRequest>[]): AggregationRequest[] | undefined {
  if (!aggs) return undefined;
  const cleaned: AggregationRequest[] = [];
  for (const agg of aggs) {
    if (!agg || !agg.column || !agg.operation) continue;
    cleaned.push({
      column: agg.column,
      operation: agg.operation,
      alias: agg.alias || undefined,
    });
  }
  return cleaned.length ? cleaned : undefined;
}

function sanitiseSort(sort?: Nullable<SortRequest>[]): SortRequest[] | undefined {
  if (!sort) return undefined;
  const cleaned: SortRequest[] = [];
  for (const item of sort) {
    if (!item || !item.column || !item.direction) continue;
    const direction = item.direction === 'asc' ? 'asc' : item.direction === 'desc' ? 'desc' : undefined;
    if (!direction) continue;
    cleaned.push({ column: item.column, direction });
  }
  return cleaned.length ? cleaned : undefined;
}

function sanitiseTopBottom(request?: Nullable<TopBottomRequest>): TopBottomRequest | undefined {
  if (!request || !request.type || !request.column || !request.count) return undefined;
  return {
    type: request.type === 'bottom' ? 'bottom' : 'top',
    column: request.column,
    count: Math.max(1, Math.round(request.count)),
  };
}

function sanitiseParsedQuery(raw: Nullable<QueryParserResult>, summary?: DataSummary): QueryParserResult {
  const parsed: QueryParserResult = {
    rawQuestion: raw?.rawQuestion || '',
    confidence: raw?.confidence ?? 0,
  };
  if (raw?.chartTypeHint) parsed.chartTypeHint = raw.chartTypeHint;
  if (raw?.variables) parsed.variables = raw.variables.filter(Boolean) as string[];
  if (raw?.secondaryVariables) parsed.secondaryVariables = raw.secondaryVariables.filter(Boolean) as string[];
  if (raw?.groupBy) parsed.groupBy = raw.groupBy.filter(Boolean) as string[];
  
  // Sanitize dateAggregationPeriod
  const validPeriods: DatePeriod[] = ['day', 'month', 'monthOnly', 'quarter', 'year'];
  if (raw?.dateAggregationPeriod && validPeriods.includes(raw.dateAggregationPeriod as DatePeriod)) {
    parsed.dateAggregationPeriod = raw.dateAggregationPeriod as DatePeriod;
  } else if (raw?.rawQuestion) {
    // Fallback: try to detect from the query if AI didn't detect it
    const detected = detectPeriodFromQuery(raw.rawQuestion);
    if (detected) {
      parsed.dateAggregationPeriod = detected;
    }
  }
  
  // Fix groupBy: if dateAggregationPeriod is set AND aggregations are requested, ensure groupBy uses actual date column
  // Only auto-add groupBy if aggregations are explicitly present (user requested aggregation)
  if (parsed.dateAggregationPeriod && parsed.aggregations && parsed.aggregations.length > 0 && summary && summary.dateColumns.length > 0) {
    const dateColumn = summary.dateColumns[0]; // Use first date column
    const periodNames = ['year', 'month', 'quarter', 'day', 'years', 'months', 'quarters', 'days'];
    
    if (!parsed.groupBy || parsed.groupBy.length === 0) {
      // If groupBy is not set but dateAggregationPeriod and aggregations are, automatically add date column
      console.log(`🔧 Auto-adding date column "${dateColumn}" to groupBy for period "${parsed.dateAggregationPeriod}" (aggregation requested)`);
      parsed.groupBy = [dateColumn];
    } else {
      // Check if groupBy contains period names instead of actual date columns
      const hasPeriodNameInGroupBy = parsed.groupBy.some(col => periodNames.includes(col.toLowerCase()));
      const hasDateColumn = parsed.groupBy.some(col => summary.dateColumns.includes(col));
      
      if (hasPeriodNameInGroupBy && !hasDateColumn) {
        // Replace period names with actual date column
        console.log(`🔧 Fixing groupBy: replacing period names with actual date column "${dateColumn}"`);
        parsed.groupBy = parsed.groupBy.map(col => 
          periodNames.includes(col.toLowerCase()) ? dateColumn : col
        );
        // Remove duplicates
        parsed.groupBy = Array.from(new Set(parsed.groupBy));
      } else if (!hasDateColumn) {
        // If no date column in groupBy but dateAggregationPeriod is set with aggregations, add it
        console.log(`🔧 Adding date column "${dateColumn}" to groupBy for period aggregation`);
        parsed.groupBy = [dateColumn, ...parsed.groupBy];
      }
    }
  } else if (parsed.dateAggregationPeriod && (!parsed.aggregations || parsed.aggregations.length === 0)) {
    // If dateAggregationPeriod is set but no aggregations, clear it (user didn't request aggregation)
    console.log(`⚠️ dateAggregationPeriod set but no aggregations requested - clearing dateAggregationPeriod to return raw data`);
    parsed.dateAggregationPeriod = undefined;
  }
  
  parsed.timeFilters = sanitiseTimeFilters(raw?.timeFilters as Nullable<TimeFilter>[]);
  parsed.valueFilters = sanitiseValueFilters(raw?.valueFilters as Nullable<ValueFilter>[]);
  parsed.exclusionFilters = sanitiseExclusionFilters(raw?.exclusionFilters as Nullable<ExclusionFilter>[]);
  parsed.logicalOperator = raw?.logicalOperator === 'OR' ? 'OR' : 'AND';
  parsed.aggregations = sanitiseAggregations(raw?.aggregations as Nullable<AggregationRequest>[]);
  parsed.sort = sanitiseSort(raw?.sort as Nullable<SortRequest>[]);
  parsed.topBottom = sanitiseTopBottom(raw?.topBottom as Nullable<TopBottomRequest>);
  if (typeof raw?.limit === 'number') parsed.limit = Math.max(1, Math.round(raw.limit));
  if (raw?.notes) parsed.notes = raw.notes.filter(Boolean) as string[];
  return parsed;
}

/**
 * Enhance parsed query with extracted dates from the query string
 * This adds time filters for specific dates mentioned in the query
 */
function enhanceTimeFiltersWithExtractedDates(
  parsed: QueryParserResult,
  query: string,
  summary: DataSummary
): QueryParserResult {
  const extractedDates = extractDatesFromQuery(query);
  
  if (extractedDates.length === 0) return parsed;
  
  // If dateAggregationPeriod is 'monthOnly', don't add time filters for specific dates
  // because the user wants month-only aggregation (combining all years)
  if (parsed.dateAggregationPeriod === 'monthOnly') {
    console.log('📅 Month-only aggregation detected, skipping specific date filters');
    return parsed;
  }
  
  console.log(`📅 Extracted ${extractedDates.length} date(s) from query:`, extractedDates.map(d => d.originalText));
  
  // If no time filters exist, create them
  if (!parsed.timeFilters) {
    parsed.timeFilters = [];
  }
  
  const dateColumn = summary.dateColumns[0];
  if (!dateColumn) {
    console.log('⚠️ No date columns available, skipping date extraction');
    return parsed;
  }
  
  for (const extracted of extractedDates) {
    // Check if we already have a filter for this
    const existingFilter = parsed.timeFilters.find(f => {
      if (extracted.type === 'year' && f.type === 'year') {
        return f.years?.includes(extracted.year!);
      }
      if (extracted.type === 'month' && f.type === 'month') {
        const monthNames = ['January', 'February', 'March', 'April', 'May', 'June',
                           'July', 'August', 'September', 'October', 'November', 'December'];
        const extractedMonthName = monthNames[extracted.month!];
        return f.months?.some(m => m.toLowerCase() === extractedMonthName.toLowerCase());
      }
      if (extracted.type === 'dateRange' && f.type === 'dateRange') {
        return f.startDate === extracted.startDate!.toISOString().split('T')[0] &&
               f.endDate === extracted.endDate!.toISOString().split('T')[0];
      }
      return false;
    });
    
    if (existingFilter) {
      console.log(`⏭️ Skipping duplicate date filter for: ${extracted.originalText}`);
      continue; // Skip if already exists
    }
    
    switch (extracted.type) {
      case 'year':
        parsed.timeFilters.push({
          type: 'year',
          column: dateColumn,
          years: [extracted.year!],
        });
        console.log(`✅ Added year filter: ${extracted.year}`);
        break;
      case 'month':
        // If we have both month and year, use dateRange for precise filtering
        // Otherwise, use month filter for month-only matching
        if (extracted.year !== undefined) {
          // Use dateRange to match specific month-year (e.g., "Apr-24" = April 2024)
          const startDate = new Date(extracted.year!, extracted.month!, 1);
          const endDate = new Date(extracted.year!, extracted.month! + 1, 0); // Last day of the month
          parsed.timeFilters.push({
            type: 'dateRange',
            column: dateColumn,
            startDate: startDate.toISOString().split('T')[0],
            endDate: endDate.toISOString().split('T')[0],
          });
          const monthNames = ['January', 'February', 'March', 'April', 'May', 'June',
                             'July', 'August', 'September', 'October', 'November', 'December'];
          console.log(`✅ Added month-year filter (dateRange): ${monthNames[extracted.month!]} ${extracted.year}`);
        } else {
          // Month only, no year specified
          const monthNames = ['January', 'February', 'March', 'April', 'May', 'June',
                             'July', 'August', 'September', 'October', 'November', 'December'];
          parsed.timeFilters.push({
            type: 'month',
            column: dateColumn,
            months: [monthNames[extracted.month!]],
          });
          console.log(`✅ Added month filter: ${monthNames[extracted.month!]}`);
        }
        break;
      case 'date':
        // Use dateRange with same start and end for exact date match
        const dateStr = extracted.date!.toISOString().split('T')[0];
        parsed.timeFilters.push({
          type: 'dateRange',
          column: dateColumn,
          startDate: dateStr,
          endDate: dateStr,
        });
        console.log(`✅ Added date filter: ${dateStr}`);
        break;
      case 'dateRange':
        parsed.timeFilters.push({
          type: 'dateRange',
          column: dateColumn,
          startDate: extracted.startDate!.toISOString().split('T')[0],
          endDate: extracted.endDate!.toISOString().split('T')[0],
        });
        console.log(`✅ Added date range filter: ${extracted.startDate!.toISOString().split('T')[0]} to ${extracted.endDate!.toISOString().split('T')[0]}`);
        break;
    }
  }
  
  return parsed;
}

export async function parseUserQuery(
  question: string,
  summary: DataSummary,
  chatHistory: Message[] = []
): Promise<QueryParserResult> {
  const recentHistory = chatHistory
    .slice(-6)
    .map((msg) => `${msg.role}: ${msg.content}`)
    .join('\n');

  const prompt = `You are a data analysis query parser. Interpret the user's question and extract structured filters.

USER QUESTION:
"""
${question}
"""

CONTEXT (recent conversation):
${recentHistory || 'N/A'}

AVAILABLE COLUMNS:
${summary.columns.map((c) => `${c.name} [${c.type}]`).join(', ')}

NUMERIC COLUMNS: ${summary.numericColumns.join(', ') || 'None'}
DATE COLUMNS: ${summary.dateColumns.join(', ') || 'None'}

YOUR TASK:
- Extract the user's intent into structured filters.
- Use ONLY the columns provided.
- CRITICAL: DO NOT add aggregations, groupBy, or pivots UNLESS the user explicitly requests them using words like:
  * "aggregate", "aggregated", "aggregation", "sum", "total", "average", "mean", "count", "group by", "grouped by", 
  * "pivot", "pivoted", "by category", "by month" (when combined with aggregation words), etc.
- DEFAULT BEHAVIOR: Return raw, unaggregated data. Only apply filters (time, value, exclusion) unless aggregation is explicitly requested.
- If the user references time (years, months, quarters, ranges), capture it in timeFilters.
- If the user mentions specific dates like "Apr-24", "March 2022", "2024", "15/01/2024", extract them and create timeFilters.
  * For month-year formats (e.g., "Apr-24", "March 2022"), create a timeFilter with type "month" and include the full month name (e.g., "April", "March").
  * For year-only mentions (e.g., "2024"), create a timeFilter with type "year" and include the year number.
  * For specific dates (e.g., "2024-01-15", "15/01/2024"), create a timeFilter with type "dateRange" using that date as both startDate and endDate.
  * For date ranges (e.g., "from Apr-24 to Jun-24"), create a timeFilter with type "dateRange" with startDate and endDate.
- ONLY if the user explicitly asks for aggregation with time periods (e.g., "monthly revenue", "yearly sales", "aggregate by month", "aggregated over year"), 
  set "dateAggregationPeriod" to "month", "monthOnly", "year", "quarter", or "day" accordingly. This indicates that 
  date columns should be normalized to this period before grouping/aggregation.
- IMPORTANT: Questions about "month-over-month growth", "consistent month-over-month growth", "seasonal patterns", etc. 
  should ONLY apply time filters and return raw data. DO NOT automatically add aggregations or groupBy unless the user 
  explicitly asks to "aggregate", "sum", "group by", etc.
- IMPORTANT: Distinguish between:
  * "month" - groups by month-year (e.g., "Jan 2024", "Jan 2022" are separate)
  * "monthOnly" - groups by month name only, combining all years (e.g., all "Jan" values combined regardless of year)
  Use "monthOnly" when user says "aggregated months", "across months", "by month name" without mentioning specific dates.
  Use "month" when user mentions specific month-year combinations or wants to see trends over time.
- IMPORTANT: When "dateAggregationPeriod" is set, the "groupBy" should use the ACTUAL DATE COLUMN NAME from the available columns 
  (e.g., "Month", "Date", "Year" - whatever date column exists), NOT the period name like "year" or "month". 
  For example, if dateAggregationPeriod is "year" and the date column is "Month", set groupBy to ["Month"], not ["year"].
- If the user specifies numeric conditions (>, <, between, etc.), capture in valueFilters.
- CRITICAL: Handle Indian number units in value filters:
  * "crore" = 10,000,000 (10 million) - convert "₹2 crore" to value: 20000000
  * "lakh" = 100,000 (hundred thousand) - convert "₹5 lakh" to value: 500000
  * Examples: "more than ₹2 crore" → operator: ">", value: 20000000
  * Examples: "exceeding ₹5 crore" → operator: ">", value: 50000000
  * Examples: "less than ₹10 lakh" → operator: "<", value: 1000000
  * Extract the numeric value and multiply by the unit multiplier
- CRITICAL: Handle "above average", "below average", "above the yearly monthly average", "above the monthly average", "above average", "below average" patterns:
  * These are comparisons to calculated averages/means
  * Extract the reference: "average", "mean", "yearly monthly average", "monthly average"
  * Set reference: "mean" for average comparisons
  * Extract the column being compared (e.g., "total revenue", "revenue", "total", "value")
  * Example: "above the yearly monthly average" → operator: ">", reference: "mean", column: "total" (or revenue column)
  * Example: "below average" → operator: "<", reference: "mean", column: (infer from context)
  * Example: "Which months had total revenue above the yearly monthly average" → valueFilter: {column: "total", operator: ">", reference: "mean"}
  * The column should be the metric being compared - match to available numeric columns
- If the user wants to exclude categories, use exclusionFilters.
- ONLY if the user explicitly requests aggregation (using words like "aggregate", "sum", "total", "group by"), then:
  * Extract the aggregation column if mentioned (e.g., "sum of total" → column: "total", operation: "sum")
  * Extract the grouping columns if mentioned (e.g., "group by category" → groupBy: ["category"])
  * Set aggregations and groupBy only when explicitly requested
  * Patterns that indicate explicit aggregation requests:
    - "sum of [column] for [category]"
    - "aggregate [column] by [dimension]"
    - "total revenue by category"
    - "group by [dimension]"
    - "pivot by [dimension]"
- If the user asks for top/bottom N, populate topBottom.
- ONLY capture aggregations (sum, mean, count, etc.) and groupings when explicitly requested by the user.
- Identify chart type hints (line, bar, scatter, pie, area) if strongly implied.
- List key variables mentioned.
- Provide a confidence score between 0 and 1.

Output valid JSON with the following structure:
{
  "rawQuestion": string,
  "confidence": number between 0 and 1,
  "chartTypeHint": "line" | "bar" | "scatter" | "pie" | "area" | null,
  "variables": string[] | null,
  "secondaryVariables": string[] | null,
  "groupBy": string[] | null,
  "dateAggregationPeriod": "day" | "month" | "monthOnly" | "quarter" | "year" | null,
  "timeFilters": [
    {
      "type": "year" | "month" | "quarter" | "dateRange" | "relative",
      "column": string | null,
      "years": number[] | null,
      "months": string[] | null,
      "quarters": [1,2,3,4] | null,
      "startDate": string | null,
      "endDate": string | null,
      "relative": { "unit": "month"|"quarter"|"year"|"week", "direction": "past"|"future", "amount": number } | null
    }
  ] | null,
  "valueFilters": [
    { "column": string, "operator": ">"|">="|"<"|"<="|"="|"between"|"!=", "value": number | null, "value2": number | null, "reference": "mean"|"median"|"p75"|"p25"|"max"|"min"|null }
  ] | null,
  "exclusionFilters": [ { "column": string, "values": (string|number)[] } ] | null,
  "logicalOperator": "AND" | "OR" | null,
  "aggregations": [ { "column": string, "operation": "sum"|"mean"|"avg"|"count"|"min"|"max"|"median", "alias": string | null } ] | null,
  "sort": [ { "column": string, "direction": "asc"|"desc" } ] | null,
  "topBottom": { "type": "top"|"bottom", "column": string, "count": number } | null,
  "limit": number | null,
  "notes": string[] | null
}
Ensure the JSON is strict and contains no comments.`;

  const response = await openai.chat.completions.create({
    model: MODEL as string,
    messages: [
      { role: 'system', content: 'You convert natural language questions into structured filter objects for data analysis.' },
      { role: 'user', content: prompt },
    ],
    temperature: 0,
    max_tokens: 700,
    response_format: { type: 'json_object' },
  });

  const content = response.choices[0]?.message?.content;
  if (!content) {
    return {
      rawQuestion: question,
      confidence: 0,
    };
  }

  try {
    const parsed = JSON.parse(content) as Nullable<QueryParserResult>;
    const sanitized = sanitiseParsedQuery(parsed, summary);
    
    // Enhance with extracted dates from the query
    return enhanceTimeFiltersWithExtractedDates(sanitized, question, summary);
  } catch (error) {
    console.error('❌ Failed to parse query parser response:', error, content);
    return {
      rawQuestion: question,
      confidence: 0,
    };
  }
}
