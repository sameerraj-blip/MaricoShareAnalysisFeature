/**
 * Analytical Query Engine
 * New architecture for analytical (non-visualization) questions:
 * User Question → Azure OpenAI (Execution Plan) → Python Engine → Results → Azure OpenAI (Explanation)
 */

import { openai, MODEL } from './openai.js';
import { DataSummary, Message } from '../shared/schema.js';
import { checkPythonServiceHealth } from './dataOps/pythonService.js';

export interface ExecutionPlan {
  steps: ExecutionStep[];
  description: string;
}

export interface ExecutionStep {
  step_number: number;
  operation: 'filter' | 'aggregate' | 'group_by' | 'sort' | 'pivot' | 'join' | 'calculate' | 'select';
  description: string;
  parameters: Record<string, any>;
  input_data?: string; // Reference to previous step output or 'original_data'
  output_alias?: string; // Alias for this step's output
}

export interface QueryExecutionResult {
  success: boolean;
  data?: Record<string, any>[];
  error?: string;
  execution_log?: string[];
}

/**
 * Detects if a question is an information-seeking query (extracting specific data from CSV/Excel)
 * These queries should use the query-only reasoning layer (return only query/plan, no explanations)
 */
export function isInformationSeekingQuery(question: string): boolean {
  const lowerQuestion = question.toLowerCase();
  
  // Exclude visualization/analysis keywords - these should use current architecture
  // Also exclude explicit data operation keywords - these should go through DataOpsHandler
  const excludeKeywords = [
    'chart', 'graph', 'plot', 'visualize', 'visualization', 'diagram',
    'bar chart', 'line chart', 'pie chart', 'scatter plot', 'histogram',
    'show me a', 'create a chart', 'draw a', 'plot a', 'graph of',
    'correlation', 'correlate', 'impact', 'affect', 'influence', 'relationship',
    'analyze', 'analysis', 'trend', 'pattern', 'insight', 'statistic',
    // Explicit data operation keywords - these should create/modify tables
    'aggregate', 'aggregation', 'group by', 'grouped by', 'create a table', 'create table',
    'create new table', 'new table', 'pivot table', 'pivot', 'modify table', 'change table',
    'transform table', 'restructure', 'reorganize', 'save as', 'export as'
  ];
  
  // If it contains exclusion keywords, it's NOT an information-seeking query
  if (excludeKeywords.some(keyword => lowerQuestion.includes(keyword))) {
    return false;
  }
  
  // Information-seeking patterns - queries that extract specific data
  // These typically ask "which/what/how many" entities meet certain criteria
  const informationSeekingPatterns = [
    // "Which X..." - asking for specific entities (e.g., "Which regions generated...")
    /\bwhich\s+\w+(\s+\w+)*\s+(generated|made|sold|earned|had|exceeded|crossed|reached|achieved|have|has)/i,
    /\bwhich\s+\w+(\s+\w+)*\s+(more than|less than|above|below|exceeding|between)/i,
    
    // "What X..." - asking for specific values/entities
    /\bwhat\s+\w+(\s+\w+)*\s+(generated|made|sold|earned|had|exceeded|crossed|reached|achieved|have|has)/i,
    /\bwhat\s+\w+(\s+\w+)*\s+(more than|less than|above|below|exceeding|between)/i,
    
    // "How many X..." - counting entities
    /\bhow many\s+\w+(\s+\w+)*/i,
    
    // "How much X..." - asking for amounts
    /\bhow much\s+\w+(\s+\w+)*/i,
    
    // "Find X..." - searching for specific entities
    /\bfind\s+\w+(\s+\w+)*\s+(that|which|where)/i,
    
    // "List X..." - listing entities
    /\blist\s+\w+(\s+\w+)*\s+(that|which|where)/i,
    
    // "Show me X..." (but NOT "show me a chart")
    /\bshow me\s+\w+(\s+\w+)*\s+(that|which|where|with|having)/i,
    
    // "Give me X..." (but NOT "give me a chart")
    /\bgive me\s+\w+(\s+\w+)*\s+(that|which|where|with|having)/i,
    
    // Queries with filters/conditions asking for specific results
    // (e.g., "Regions that generated more than...")
    /\b\w+(\s+\w+)*\s+(generated|made|sold|earned|had|exceeded|crossed|reached|achieved|have|has)\s+(more than|less than|above|below|exceeding)/i,
  ];
  
  return informationSeekingPatterns.some(pattern => pattern.test(question));
}

/**
 * Detects if a question is analytical (not visualization)
 * @deprecated Use isInformationSeekingQuery() for query-only layer routing
 */
export function isAnalyticalQuery(question: string): boolean {
  const lowerQuestion = question.toLowerCase();
  
  // Visualization keywords
  const visualizationKeywords = [
    'chart', 'graph', 'plot', 'visualize', 'visualization', 'diagram',
    'bar chart', 'line chart', 'pie chart', 'scatter plot', 'histogram',
    'show me a', 'create a chart', 'draw a', 'plot a', 'graph of'
  ];
  
  // Check if it's a visualization request
  const isVisualization = visualizationKeywords.some(keyword => lowerQuestion.includes(keyword));
  
  if (isVisualization) {
    return false;
  }
  
  // Analytical question patterns
  const analyticalPatterns = [
    /\b(what|which|how many|how much|show me|give me|tell me|find|calculate|compute|count|sum|total|average|mean)\b/i,
    /\b(more than|less than|above|below|exceeding|between|during|in|from|to)\b/i,
    /\b(for|where|with|having|that|specific)\b/i,
  ];
  
  return analyticalPatterns.some(pattern => pattern.test(lowerQuestion));
}

/**
 * Identifies relevant columns and filters data based on query conditions
 */
export async function identifyRelevantColumnsAndFilterData(
  question: string,
  data: Record<string, any>[],
  summary: DataSummary,
  chatHistory: Message[] = []
): Promise<{
  identifiedColumns: string[];
  columnMapping: Record<string, string>; // Maps query terms to actual column names
  reasoning: string;
  filteredData: Record<string, any>[]; // Data filtered based on query conditions
  filterDescription: string; // Description of filters applied
}> {
  const recentHistory = chatHistory
    .slice(-6)
    .map((msg) => `${msg.role}: ${msg.content}`)
    .join('\n');

  const columnsInfo = summary.columns.map(c => `${c.name} [${c.type}]`).join(', ');
  const numericColumns = summary.numericColumns.join(', ') || 'None';
  const dateColumns = summary.dateColumns.join(', ') || 'None';
  const categoricalColumns = summary.columns
    .filter(c => !summary.numericColumns.includes(c.name) && !summary.dateColumns.includes(c.name))
    .map(c => c.name)
    .join(', ') || 'None';

  // Sample data for context (first 5 rows)
  const sampleData = data.slice(0, 5).map(row => {
    const sample: Record<string, any> = {};
    Object.keys(row).slice(0, 10).forEach(key => {
      sample[key] = row[key];
    });
    return sample;
  });

  const prompt = `You are an expert data analyst. Your task is to analyze the user's question and:
1. Identify which columns from the dataset should be used
2. Identify what filters/conditions should be applied to extract relevant rows

USER QUESTION:
"""
${question}
"""

CONTEXT (recent conversation):
${recentHistory || 'N/A'}

AVAILABLE COLUMNS IN DATASET:
${columnsInfo}

NUMERIC COLUMNS: ${numericColumns}
DATE COLUMNS: ${dateColumns}
CATEGORICAL COLUMNS: ${categoricalColumns}

SAMPLE DATA (first 5 rows, showing first 10 columns):
${JSON.stringify(sampleData, null, 2)}

YOUR TASK:
1. Analyze the user's question carefully
2. Identify ALL columns mentioned or implied in the question
3. Match query terms to actual column names in the dataset (use fuzzy matching, synonyms, partial matches)
4. Identify filters/conditions from the question:
   - Date filters: "May 2020", "in 2020", "before 2020", etc. → filter rows where date column matches
   - Value filters: "above 10", "more than ₹1 crore", etc. → filter rows where value column matches condition
   - Category filters: specific category names, product types, etc. → filter rows where category column matches
5. Extract relevant rows based on these conditions BEFORE generating execution plan
   - Example: If question says "May 2020", filter to only rows where date column contains "May 2020" or "2020-05"
   - Example: If question says "SKUs with average order size above 10", filter to SKUs meeting that condition
   - Example: If question says "repeat customers (customers since before 2020)", filter to customers where customer_since < 2020

COLUMN MATCHING RULES:
- Use EXACT column names from the available columns list
- Match synonyms: "revenue" = "total", "sales", "amount", "value"
- Match partial names: "order size" might match "order_size", "avg_order_size", "order_qty"
- Match related terms: "category" = "Category", "product_category", "cat"
- For time references: match to date columns
- For aggregations: match to numeric columns
- Be case-insensitive and flexible with spaces/underscores

FILTER IDENTIFICATION RULES:
- Extract date conditions: "May 2020", "in 2020", "during 2020", "before 2020", "after 2020", etc.
- Extract value conditions: "above X", "more than X", "less than X", "exceeding X", etc.
- Extract category conditions: specific category names, product types mentioned
- Extract time-based conditions: "single month", "in a month", "monthly", etc.

OUTPUT FORMAT (JSON only):
{
  "identifiedColumns": ["column1", "column2", ...],
  "columnMapping": {
    "query_term_1": "actual_column_name_1",
    "query_term_2": "actual_column_name_2",
    ...
  },
  "filters": [
    {
      "type": "date" | "value" | "category" | "text",
      "column": "column_name",
      "operator": "=" | ">" | "<" | ">=" | "<=" | "contains" | "starts_with" | "ends_with" | "between",
      "value": "filter_value" | number | null,
      "value2": number | null, // for "between" operator
      "description": "Human-readable description of the filter"
    }
  ],
  "reasoning": "Brief explanation of columns chosen, filters identified, and how they map to the query"
}

Example:
Question: "Which categories crossed ₹1 crore in revenue in a single month, counting only SKUs with an average order size above 10 units?"
{
  "identifiedColumns": ["Category", "Month", "total", "SKU", "qty_ordered", "order_id"],
  "columnMapping": {
    "categories": "Category",
    "revenue": "total",
    "single month": "Month",
    "SKUs": "SKU",
    "average order size": "qty_ordered",
    "order size": "qty_ordered"
  },
  "reasoning": "Category column for grouping categories, Month column for filtering by single month, total column for revenue (₹1 crore = 10000000), SKU column for filtering SKUs, qty_ordered for calculating average order size. Need to: 1) Filter SKUs with avg order size > 10, 2) Group by Category and Month, 3) Sum revenue per category-month, 4) Filter where sum > 10000000"
}

Output ONLY valid JSON, no markdown, no explanations:`;

  try {
    const response = await openai.chat.completions.create({
      model: MODEL as string,
      messages: [
        {
          role: 'system',
          content: 'You are an expert data analyst who identifies relevant columns from datasets. Output only valid JSON.',
        },
        {
          role: 'user',
          content: prompt,
        },
      ],
      temperature: 0.1,
      max_tokens: 1000,
      response_format: { type: 'json_object' },
    });

    const content = response.choices[0]?.message?.content;
    if (!content) {
      throw new Error('No response from OpenAI for column identification');
    }

    const result = JSON.parse(content) as {
      identifiedColumns: string[];
      columnMapping: Record<string, string>;
      filters?: Array<{
        type: 'date' | 'value' | 'category' | 'text';
        column: string;
        operator: '=' | '>' | '<' | '>=' | '<=' | 'contains' | 'starts_with' | 'ends_with' | 'between';
        value: string | number | null;
        value2?: number | null;
        description: string;
      }>;
      reasoning: string;
    };

    console.log('✅ Identified columns:', result.identifiedColumns);
    console.log('📋 Column mapping:', result.columnMapping);
    console.log('🔍 Filters identified:', result.filters?.length || 0);
    console.log('💭 Reasoning:', result.reasoning);

    // Apply filters to data
    let filteredData = data;
    const filterDescriptions: string[] = [];

    if (result.filters && result.filters.length > 0) {
      console.log('🔧 Applying filters to data...');
      
      for (const filter of result.filters) {
        const beforeCount = filteredData.length;
        
        filteredData = filteredData.filter((row) => {
          const rowValue = row[filter.column];
          
          if (rowValue === null || rowValue === undefined) {
            return false;
          }

          // Special handling for date filters
          if (filter.type === 'date') {
            const rowDateStr = String(rowValue).toLowerCase();
            const filterValueStr = String(filter.value).toLowerCase();
            
            // Handle various date formats
            // "May 2020" should match "2020-05", "May-2020", "2020/05", "May 2020", etc.
            if (filter.operator === 'contains' || filter.operator === '=') {
              // Extract year and month from filter value
              const yearMatch = filterValueStr.match(/\b(20\d{2})\b/);
              const monthMatch = filterValueStr.match(/\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|january|february|march|april|may|june|july|august|september|october|november|december)\b/i);
              
              if (yearMatch && monthMatch) {
                const year = yearMatch[1];
                const monthName = monthMatch[1].substring(0, 3).toLowerCase();
                const monthMap: Record<string, string> = {
                  'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'may': '05', 'jun': '06',
                  'jul': '07', 'aug': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
                };
                const monthNum = monthMap[monthName] || '01';
                
                // Check if row date contains both year and month
                const hasYear = rowDateStr.includes(year);
                const hasMonth = rowDateStr.includes(monthNum) || 
                                rowDateStr.includes(monthName) ||
                                rowDateStr.includes(monthMatch[1].toLowerCase());
                
                return hasYear && hasMonth;
              } else if (yearMatch) {
                // Just year match
                return rowDateStr.includes(yearMatch[1]);
              }
            }
          }

          switch (filter.operator) {
            case '=':
              return String(rowValue).toLowerCase() === String(filter.value).toLowerCase();
            case '>':
              return Number(rowValue) > Number(filter.value);
            case '>=':
              return Number(rowValue) >= Number(filter.value);
            case '<':
              return Number(rowValue) < Number(filter.value);
            case '<=':
              return Number(rowValue) <= Number(filter.value);
            case 'contains':
              return String(rowValue).toLowerCase().includes(String(filter.value).toLowerCase());
            case 'starts_with':
              return String(rowValue).toLowerCase().startsWith(String(filter.value).toLowerCase());
            case 'ends_with':
              return String(rowValue).toLowerCase().endsWith(String(filter.value).toLowerCase());
            case 'between':
              if (filter.value === null || filter.value2 === null) return true;
              const num = Number(rowValue);
              return num >= Number(filter.value) && num <= Number(filter.value2);
            default:
              return true;
          }
        });

        const afterCount = filteredData.length;
        filterDescriptions.push(`${filter.description}: ${beforeCount} → ${afterCount} rows`);
        console.log(`  ✓ ${filter.description}: ${beforeCount} → ${afterCount} rows`);
      }
    }

    return {
      identifiedColumns: result.identifiedColumns,
      columnMapping: result.columnMapping,
      reasoning: result.reasoning,
      filteredData,
      filterDescription: filterDescriptions.join('; ') || 'No filters applied',
    };
  } catch (error) {
    console.error('❌ Error identifying columns:', error);
    // Fallback: return all columns with no filtering
    return {
      identifiedColumns: summary.columns.map(c => c.name),
      columnMapping: {},
      reasoning: 'Failed to identify columns, using all available columns',
      filteredData: data,
      filterDescription: 'No filters applied (fallback)',
    };
  }
}

/**
 * Generates execution plan using Azure OpenAI
 */
export async function generateExecutionPlan(
  question: string,
  summary: DataSummary,
  chatHistory: Message[] = [],
  columnIdentification?: { 
    identifiedColumns: string[]; 
    columnMapping: Record<string, string>; 
    reasoning: string;
    filteredData?: Record<string, any>[];
    filterDescription?: string;
  },
  data?: Record<string, any>[]
): Promise<ExecutionPlan> {
  const recentHistory = chatHistory
    .slice(-6)
    .map((msg) => `${msg.role}: ${msg.content}`)
    .join('\n');

  // Use identified columns if provided, otherwise use all columns
  const relevantColumns = columnIdentification?.identifiedColumns || summary.columns.map(c => c.name);
  const columnMapping = columnIdentification?.columnMapping || {};
  
  const columnsInfo = summary.columns
    .filter(c => relevantColumns.includes(c.name))
    .map(c => `${c.name} [${c.type}]`)
    .join(', ');
  const numericColumns = summary.numericColumns.filter(c => relevantColumns.includes(c)).join(', ') || 'None';
  const dateColumns = summary.dateColumns.filter(c => relevantColumns.includes(c)).join(', ') || 'None';

  const prompt = `You are an expert data analyst. Your task is to create a step-by-step execution plan to answer the user's analytical question.

USER QUESTION:
"""
${question}
"""

CONTEXT (recent conversation):
${recentHistory || 'N/A'}

RELEVANT COLUMNS IDENTIFIED:
${columnsInfo}

COLUMN MAPPING (query terms → actual column names):
${JSON.stringify(columnMapping, null, 2)}

NUMERIC COLUMNS: ${numericColumns}
DATE COLUMNS: ${dateColumns}

${columnIdentification?.filterDescription ? `DATA FILTERING APPLIED:
${columnIdentification.filterDescription}
The data has been pre-filtered based on query conditions. Use this filtered dataset for the execution plan.

FILTERED DATA STATISTICS:
${data ? `- Original rows: ${data.length}` : ''}
- Filtered rows: ${columnIdentification.filteredData?.length || 0}
${data && columnIdentification.filteredData ? `- Rows removed: ${data.length - columnIdentification.filteredData.length}` : ''}
` : ''}

CRITICAL: Use ONLY the columns listed above. Use the EXACT column names from the column mapping or relevant columns list.

🚨 ABSOLUTE REQUIREMENTS:
1. If the question mentions "categories", use the column mapped to "categories" in the column mapping (e.g., "Category")
2. If the question mentions "revenue", use the column mapped to "revenue" in the column mapping (e.g., "total")
3. If the question mentions "SKUs", use the column mapped to "SKUs" in the column mapping (e.g., "SKU")
4. DO NOT use column names that are NOT in the relevant columns list
5. DO NOT substitute "region" for "category" or vice versa - use the EXACT mapping
6. If calculating "average order size", use the columns identified for order size calculations

YOUR TASK:
Create a detailed execution plan as a JSON object with steps that can be executed using Python/Pandas/DuckDB.
The plan MUST use the columns from the column mapping above.

EXECUTION PLAN STRUCTURE:
Each step should be one of these operations:
- "filter": Filter rows based on conditions (e.g., date ranges, value comparisons)
- "aggregate": Aggregate data (sum, mean, count, etc.)
- "group_by": Group data by one or more columns
- "sort": Sort data by columns
- "pivot": Create pivot table
- "select": Select specific columns
- "calculate": Perform calculations (derived columns, ratios, etc.)

Each step should reference:
- input_data: "original_data" for first step, or "step_N" for subsequent steps
- output_alias: "step_N" where N is the step number
- parameters: Operation-specific parameters

EXAMPLE EXECUTION PLAN:
Question: "Which categories crossed ₹1 crore in revenue in a single month, counting only SKUs with an average order size above 10 units?"

{
  "steps": [
    {
      "step_number": 1,
      "operation": "calculate",
      "description": "Calculate average order size per SKU",
      "parameters": {
        "new_column_name": "avg_order_size",
        "expression": "qty_ordered / COUNT(order_id) GROUP BY SKU"
      },
      "input_data": "original_data",
      "output_alias": "step_1"
    },
    {
      "step_number": 2,
      "operation": "filter",
      "description": "Filter SKUs with average order size > 10",
      "parameters": {
        "column": "avg_order_size",
        "operator": ">",
        "value": 10
      },
      "input_data": "step_1",
      "output_alias": "step_2"
    },
    {
      "step_number": 3,
      "operation": "group_by",
      "description": "Group by Category and Month",
      "parameters": {
        "group_by_column": "Category",
        "additional_group_by": ["Month"]
      },
      "input_data": "step_2",
      "output_alias": "step_3"
    },
    {
      "step_number": 4,
      "operation": "aggregate",
      "description": "Sum revenue (total) per category-month",
      "parameters": {
        "agg_column": "total",
        "agg_function": "sum"
      },
      "input_data": "step_3",
      "output_alias": "step_4"
    },
    {
      "step_number": 5,
      "operation": "filter",
      "description": "Filter categories where revenue > 10000000 (₹1 crore) in a single month",
      "parameters": {
        "column": "total_sum",
        "operator": ">",
        "value": 10000000
      },
      "input_data": "step_4",
      "output_alias": "step_5"
    }
  ],
  "description": "Find categories that crossed ₹1 crore in revenue in a single month, counting only SKUs with average order size above 10 units"
}

IMPORTANT RULES:
1. Use EXACT column names from the RELEVANT COLUMNS IDENTIFIED above - DO NOT use column names that are not in that list
2. Use the COLUMN MAPPING to translate query terms to actual column names
3. For date filters, use ISO format (YYYY-MM-DD) or relative dates
4. For numeric comparisons, convert Indian units (crore = 10000000, lakh = 100000)
5. Chain steps logically - each step uses output from previous step
6. The final step's output_alias will contain the answer
7. Be specific with parameters - include all necessary details
8. For aggregations after group_by, reference the grouped data
9. Handle time filters properly (before 2020, during 2020, etc.)
10. If the question asks about "categories", use the column mapped to "categories" in the column mapping
11. If the question asks about "revenue", use the column mapped to "revenue" in the column mapping
12. If the question asks about "SKUs", use the column mapped to "SKUs" in the column mapping
13. If the question asks about "average order size", calculate it from the mapped columns
14. If the question asks about "single month", group by month and filter to find months where the condition is met

Output ONLY valid JSON, no markdown, no explanations:
`;

  try {
    const response = await openai.chat.completions.create({
      model: MODEL as string,
      messages: [
        {
          role: 'system',
          content: 'You are an expert data analyst who creates execution plans for analytical queries. Output only valid JSON.',
        },
        {
          role: 'user',
          content: prompt,
        },
      ],
      temperature: 0.1,
      max_tokens: 2000,
      response_format: { type: 'json_object' },
    });

    const content = response.choices[0]?.message?.content;
    if (!content) {
      throw new Error('No response from OpenAI');
    }

    const plan = JSON.parse(content) as ExecutionPlan;
    
    // Validate plan structure
    if (!plan.steps || !Array.isArray(plan.steps) || plan.steps.length === 0) {
      throw new Error('Invalid execution plan: no steps found');
    }

    // Ensure step numbering and input/output references are correct
    plan.steps.forEach((step, index) => {
      step.step_number = index + 1;
      if (index === 0) {
        step.input_data = 'original_data';
      } else {
        step.input_data = `step_${index}`;
      }
      step.output_alias = `step_${step.step_number}`;
    });

    console.log('✅ Generated execution plan:', JSON.stringify(plan, null, 2));
    return plan;
  } catch (error) {
    console.error('❌ Error generating execution plan:', error);
    throw error;
  }
}

/**
 * Executes the execution plan using Python service
 */
export async function executePlan(
  plan: ExecutionPlan,
  data: Record<string, any>[]
): Promise<QueryExecutionResult> {
  const executionLog: string[] = [];
  let currentData = data;
  const stepResults: Record<string, Record<string, any>[]> = {
    original_data: data,
  };

  try {
    // Check Python service health
    const isHealthy = await checkPythonServiceHealth();
    if (!isHealthy) {
      throw new Error('Python service is not available');
    }

    executionLog.push(`Starting execution of ${plan.steps.length} steps`);

    const skippedSteps = new Set<number>();

    for (let i = 0; i < plan.steps.length; i++) {
      const step = plan.steps[i];
      
      // Skip if this step was already combined with previous step
      if (skippedSteps.has(step.step_number)) {
        continue;
      }
      
      executionLog.push(`Executing step ${step.step_number}: ${step.description}`);
      
      // Get input data
      const inputData = stepResults[step.input_data || 'original_data'];
      if (!inputData) {
        throw new Error(`Input data not found for step ${step.step_number}: ${step.input_data}`);
      }

      // Execute step based on operation
      let result: Record<string, any>[];
      
      // Check if this is group_by and next step is aggregate (combine them)
      const nextStep = i + 1 < plan.steps.length ? plan.steps[i + 1] : null;
      const isGroupByBeforeAggregate = step.operation === 'group_by' && 
                                       nextStep && 
                                       nextStep.operation === 'aggregate' &&
                                       !skippedSteps.has(nextStep.step_number);
      
      if (isGroupByBeforeAggregate) {
        // Combine group_by and aggregate into single operation
        executionLog.push(`Combining group_by (step ${step.step_number}) and aggregate (step ${nextStep.step_number}) steps`);
        const groupByColumn = step.parameters.group_by_column || step.parameters.columns?.[0];
        result = await executeAggregateStep(inputData, {
          ...nextStep.parameters,
          group_by_column: groupByColumn,
        });
        // Mark next step as skipped
        skippedSteps.add(nextStep.step_number);
        executionLog.push(`Skipping step ${nextStep.step_number} (already combined with step ${step.step_number})`);
        // Store result with next step's alias so subsequent steps can reference it
        const nextOutputAlias = nextStep.output_alias || `step_${nextStep.step_number}`;
        stepResults[nextOutputAlias] = result;
        currentData = result;
        executionLog.push(`Step ${step.step_number} completed: ${result.length} rows`);
        continue;
      }
      
      switch (step.operation) {
        case 'filter':
          result = await executeFilterStep(inputData, step.parameters);
          break;
        case 'aggregate':
          result = await executeAggregateStep(inputData, step.parameters);
          break;
        case 'group_by':
          result = await executeGroupByStep(inputData, step.parameters);
          break;
        case 'sort':
          result = await executeSortStep(inputData, step.parameters);
          break;
        case 'pivot':
          result = await executePivotStep(inputData, step.parameters);
          break;
        case 'select':
          result = await executeSelectStep(inputData, step.parameters);
          break;
        case 'calculate':
          result = await executeCalculateStep(inputData, step.parameters);
          break;
        default:
          throw new Error(`Unknown operation: ${step.operation}`);
      }

      // Store result
      const outputAlias = step.output_alias || `step_${step.step_number}`;
      stepResults[outputAlias] = result;
      currentData = result;
      
      executionLog.push(`Step ${step.step_number} completed: ${result.length} rows`);
    }

    // Get final result
    const finalStep = plan.steps[plan.steps.length - 1];
    const finalOutputAlias = finalStep.output_alias || `step_${plan.steps.length}`;
    const finalData = stepResults[finalOutputAlias];

    return {
      success: true,
      data: finalData,
      execution_log: executionLog,
    };
  } catch (error) {
    console.error('❌ Error executing plan:', error);
    return {
      success: false,
      error: error instanceof Error ? error.message : String(error),
      execution_log: executionLog,
    };
  }
}

/**
 * Execute filter step
 */
async function executeFilterStep(
  data: Record<string, any>[],
  parameters: Record<string, any>
): Promise<Record<string, any>[]> {
  const { column, operator, value } = parameters;
  
  if (!column) {
    throw new Error('Filter step requires column parameter');
  }

  return data.filter((row) => {
    const rowValue = row[column];
    
    if (rowValue === null || rowValue === undefined) {
      return false;
    }

    switch (operator) {
      case '>':
        return Number(rowValue) > Number(value);
      case '>=':
        return Number(rowValue) >= Number(value);
      case '<':
        return Number(rowValue) < Number(value);
      case '<=':
        return Number(rowValue) <= Number(value);
      case '=':
      case '==':
        return String(rowValue).toLowerCase() === String(value).toLowerCase();
      case '!=':
        return String(rowValue).toLowerCase() !== String(value).toLowerCase();
      case 'contains':
        return String(rowValue).toLowerCase().includes(String(value).toLowerCase());
      case 'starts_with':
        return String(rowValue).toLowerCase().startsWith(String(value).toLowerCase());
      case 'ends_with':
        return String(rowValue).toLowerCase().endsWith(String(value).toLowerCase());
      default:
        return true;
    }
  });
}

/**
 * Execute aggregate step (requires group_by first)
 */
async function executeAggregateStep(
  data: Record<string, any>[],
  parameters: Record<string, any>
): Promise<Record<string, any>[]> {
  // Import Python service functions
  const { aggregateData } = await import('./dataOps/pythonService.js');
  
  const { 
    group_by_column, 
    agg_column, 
    agg_columns,
    agg_function = 'sum',
    agg_functions
  } = parameters;
  
  if (!group_by_column) {
    throw new Error('Aggregate step requires group_by_column');
  }

  // Determine which columns to aggregate
  const columnsToAggregate = agg_columns || (agg_column ? [agg_column] : []);
  
  if (columnsToAggregate.length === 0) {
    // If no columns specified, try to infer from data
    // This is a fallback - ideally the plan should specify columns
    throw new Error('Aggregate step requires agg_column or agg_columns');
  }

  // Build aggregation functions map
  const aggFuncs: Record<string, 'sum' | 'avg' | 'mean' | 'min' | 'max' | 'count' | 'median' | 'std' | 'var'> = {};
  
  if (agg_functions) {
    // Use provided function map
    Object.assign(aggFuncs, agg_functions);
  } else {
    // Use single function for all columns
    columnsToAggregate.forEach((col: string) => {
      aggFuncs[col] = agg_function as 'sum' | 'avg' | 'mean' | 'min' | 'max' | 'count';
    });
  }

  const result = await aggregateData(
    data,
    group_by_column,
    columnsToAggregate,
    aggFuncs
  );

  return result.data || [];
}

/**
 * Execute group_by step
 */
async function executeGroupByStep(
  data: Record<string, any>[],
  parameters: Record<string, any>
): Promise<Record<string, any>[]> {
  // Group by is typically combined with aggregate, so we just return the data
  // The actual grouping happens in the aggregate step
  return data;
}

/**
 * Execute sort step
 */
async function executeSortStep(
  data: Record<string, any>[],
  parameters: Record<string, any>
): Promise<Record<string, any>[]> {
  const { column, direction = 'asc' } = parameters;
  
  if (!column) {
    throw new Error('Sort step requires column parameter');
  }

  const sorted = [...data].sort((a, b) => {
    const aVal = a[column];
    const bVal = b[column];
    
    if (aVal === null || aVal === undefined) return 1;
    if (bVal === null || bVal === undefined) return -1;
    
    if (typeof aVal === 'number' && typeof bVal === 'number') {
      return direction === 'asc' ? aVal - bVal : bVal - aVal;
    }
    
    const aStr = String(aVal).toLowerCase();
    const bStr = String(bVal).toLowerCase();
    return direction === 'asc' 
      ? aStr.localeCompare(bStr)
      : bStr.localeCompare(aStr);
  });

  return sorted;
}

/**
 * Execute pivot step
 */
async function executePivotStep(
  data: Record<string, any>[],
  parameters: Record<string, any>
): Promise<Record<string, any>[]> {
  const { createPivotTable } = await import('./dataOps/pythonService.js');
  
  const { index_column, value_columns, pivot_funcs } = parameters;
  
  if (!index_column) {
    throw new Error('Pivot step requires index_column parameter');
  }

  const result = await createPivotTable(
    data,
    index_column,
    value_columns,
    pivot_funcs
  );

  return result.data || [];
}

/**
 * Execute select step
 */
async function executeSelectStep(
  data: Record<string, any>[],
  parameters: Record<string, any>
): Promise<Record<string, any>[]> {
  const { columns } = parameters;
  
  if (!columns || !Array.isArray(columns)) {
    throw new Error('Select step requires columns array');
  }

  return data.map((row) => {
    const selected: Record<string, any> = {};
    columns.forEach((col: string) => {
      if (col in row) {
        selected[col] = row[col];
      }
    });
    return selected;
  });
}

/**
 * Execute calculate step
 */
async function executeCalculateStep(
  data: Record<string, any>[],
  parameters: Record<string, any>
): Promise<Record<string, any>[]> {
  const { createDerivedColumn } = await import('./dataOps/pythonService.js');
  
  const { new_column_name, expression } = parameters;
  
  if (!new_column_name || !expression) {
    throw new Error('Calculate step requires new_column_name and expression');
  }

  const result = await createDerivedColumn(
    data,
    new_column_name,
    expression
  );

  return result.data || [];
}

/**
 * Generates ONLY the query plan for information-seeking queries
 * Returns the execution plan as a structured JSON string (no explanations, no execution)
 */
export async function generateQueryPlanOnly(
  question: string,
  summary: DataSummary,
  chatHistory: Message[] = []
): Promise<{ queryPlan: ExecutionPlan; columnMapping: Record<string, string> }> {
  console.log('📋 Generating query plan only (no execution, no explanation)...');
  
  // Step 1: Identify relevant columns (without filtering data - we don't need actual data)
  const recentHistory = chatHistory
    .slice(-6)
    .map((msg) => `${msg.role}: ${msg.content}`)
    .join('\n');

  const columnsInfo = summary.columns.map(c => `${c.name} [${c.type}]`).join(', ');
  const numericColumns = summary.numericColumns.join(', ') || 'None';
  const dateColumns = summary.dateColumns.join(', ') || 'None';
  const categoricalColumns = summary.columns
    .filter(c => !summary.numericColumns.includes(c.name) && !summary.dateColumns.includes(c.name))
    .map(c => c.name)
    .join(', ') || 'None';

  const columnIdentificationPrompt = `You are an expert data analyst. Your task is to analyze the user's question and identify which columns from the dataset should be used.

USER QUESTION:
"""
${question}
"""

CONTEXT (recent conversation):
${recentHistory || 'N/A'}

AVAILABLE COLUMNS IN DATASET:
${columnsInfo}

NUMERIC COLUMNS: ${numericColumns}
DATE COLUMNS: ${dateColumns}
CATEGORICAL COLUMNS: ${categoricalColumns}

YOUR TASK:
1. Analyze the user's question carefully
2. Identify ALL columns mentioned or implied in the question
3. Match query terms to actual column names in the dataset (use fuzzy matching, synonyms, partial matches)

COLUMN MATCHING RULES:
- Use EXACT column names from the available columns list
- Match synonyms: "revenue" = "total", "sales", "amount", "value"
- Match partial names: "order size" might match "order_size", "avg_order_size", "order_qty"
- Match related terms: "category" = "Category", "product_category", "cat"
- For time references: match to date columns
- For aggregations: match to numeric columns
- Be case-insensitive and flexible with spaces/underscores

OUTPUT FORMAT (JSON only):
{
  "identifiedColumns": ["column1", "column2", ...],
  "columnMapping": {
    "query_term_1": "actual_column_name_1",
    "query_term_2": "actual_column_name_2",
    ...
  },
  "reasoning": "Brief explanation of columns chosen and how they map to the query"
}

Output ONLY valid JSON, no markdown, no explanations:`;

  try {
    const columnResponse = await openai.chat.completions.create({
      model: MODEL as string,
      messages: [
        {
          role: 'system',
          content: 'You are an expert data analyst who identifies relevant columns from datasets. Output only valid JSON.',
        },
        {
          role: 'user',
          content: columnIdentificationPrompt,
        },
      ],
      temperature: 0.1,
      max_tokens: 1000,
      response_format: { type: 'json_object' },
    });

    const columnContent = columnResponse.choices[0]?.message?.content;
    if (!columnContent) {
      throw new Error('No response from OpenAI for column identification');
    }

    const columnResult = JSON.parse(columnContent) as {
      identifiedColumns: string[];
      columnMapping: Record<string, string>;
      reasoning: string;
    };

    // Step 2: Generate execution plan using identified columns
    const relevantColumns = columnResult.identifiedColumns;
    const columnMapping = columnResult.columnMapping;
    
    const columnsInfoFiltered = summary.columns
      .filter(c => relevantColumns.includes(c.name))
      .map(c => `${c.name} [${c.type}]`)
      .join(', ');
    const numericColumnsFiltered = summary.numericColumns.filter(c => relevantColumns.includes(c)).join(', ') || 'None';
    const dateColumnsFiltered = summary.dateColumns.filter(c => relevantColumns.includes(c)).join(', ') || 'None';

    const executionPlan = await generateExecutionPlan(question, summary, chatHistory, {
      identifiedColumns: relevantColumns,
      columnMapping: columnMapping,
      reasoning: columnResult.reasoning,
    }, undefined); // data not available in generateQueryPlanOnly

    return {
      queryPlan: executionPlan,
      columnMapping: columnMapping,
    };
  } catch (error) {
    console.error('❌ Error generating query plan:', error);
    throw error;
  }
}

/**
 * Generates explanation using Azure OpenAI
 */
export async function generateExplanation(
  question: string,
  executionPlan: ExecutionPlan,
  results: Record<string, any>[],
  summary: DataSummary
): Promise<string> {
  const resultsPreview = results.length <= 20 
    ? JSON.stringify(results, null, 2)
    : `First 10 results:\n${JSON.stringify(results.slice(0, 10), null, 2)}\n\n... (${results.length - 10} more rows) ...`;

  const prompt = `You are a helpful data analyst assistant. The user asked an analytical question, and we executed a query plan to answer it.

USER QUESTION:
"""
${question}
"""

EXECUTION PLAN:
${JSON.stringify(executionPlan, null, 2)}

QUERY RESULTS:
${resultsPreview}

AVAILABLE COLUMNS:
${summary.columns.map(c => `${c.name} [${c.type}]`).join(', ')}

YOUR TASK:
Provide a clear, conversational answer to the user's question based on the query results above.

GUIDELINES:
- Answer the question directly using the actual values from the results
- Be conversational and friendly
- Format numbers clearly (e.g., ₹2.5 crore instead of 25000000)
- If results show multiple rows, summarize or list them appropriately
- If no results found, explain why
- Reference the specific data from the results
- Don't describe the execution plan - just answer the question

Answer:`;

  try {
    const response = await openai.chat.completions.create({
      model: MODEL as string,
      messages: [
        {
          role: 'system',
          content: 'You are a helpful data analyst assistant who explains query results in a clear, conversational way.',
        },
        {
          role: 'user',
          content: prompt,
        },
      ],
      temperature: 0.7,
      max_tokens: 1000,
    });

    return response.choices[0]?.message?.content || 'Unable to generate explanation.';
  } catch (error) {
    console.error('❌ Error generating explanation:', error);
    return 'Query executed successfully, but unable to generate explanation.';
  }
}
