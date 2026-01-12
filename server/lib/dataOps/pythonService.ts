/**
 * Python Service Client
 * Communicates with the Python microservice for data operations
 */
// Use global fetch (Node.js 18+) or provide polyfill
let fetchFn: typeof fetch;
if (typeof fetch !== 'undefined') {
  fetchFn = fetch;
} else {
  try {
    // Try to use node-fetch if available
    fetchFn = require('node-fetch') as typeof fetch;
  } catch {
    // Fallback: throw error if fetch is not available
    throw new Error('fetch is not available. Please use Node.js 18+ or install node-fetch');
  }
}
const PYTHON_SERVICE_URL = process.env.PYTHON_SERVICE_URL || 'http://localhost:8001';
const REQUEST_TIMEOUT = 300000; // 5 minutes

// Import fs for file operations (to handle large responses)
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';


interface RemoveNullsRequest {
  data: Record<string, any>[];
  column?: string;
  method: 'delete' | 'mean' | 'median' | 'mode' | 'custom';
  custom_value?: any;
}

interface RemoveNullsResponse {
  data: Record<string, any>[];
  rows_before: number;
  rows_after: number;
  nulls_removed: number;
}

interface PreviewRequest {
  data: Record<string, any>[];
  limit: number;
}

interface PreviewResponse {
  data: Record<string, any>[];
  total_rows: number;
  returned_rows: number;
}

interface SummaryResponse {
  summary: Array<{
    variable: string;
    datatype: string;
    total_values: number;
    null_values: number;
    non_null_values: number;
    mean?: number | null;
    median?: number | null;
    std_dev?: number | null;
    min?: number | null;
    max?: number | null;
    mode?: any;
  }>;
}

interface CreateDerivedColumnRequest {
  data: Record<string, any>[];
  new_column_name: string;
  expression: string;
}

interface CreateDerivedColumnResponse {
  data: Record<string, any>[];
  errors: string[];
}

interface ConvertTypeRequest {
  data: Record<string, any>[];
  column: string;
  target_type: 'numeric' | 'string' | 'date' | 'percentage' | 'boolean';
}

interface ConvertTypeResponse {
  data: Record<string, any>[];
  conversion_info: {
    column: string;
    original_type: string;
    target_type: string;
    converted_type: string;
    success: boolean;
    errors: string[];
    note?: string;
  };
}

interface AggregateRequest {
  data: Record<string, any>[];
  group_by_column: string;
  agg_columns?: string[];
  agg_funcs?: Record<string, 'sum' | 'avg' | 'mean' | 'min' | 'max' | 'count' | 'median' | 'std' | 'var' | 'p90' | 'p95' | 'p99' | 'any' | 'all'>;
  order_by_column?: string;
  order_by_direction?: 'asc' | 'desc';
  user_intent?: string;  // User's original message for semantic intent detection
}

interface AggregateResponse {
  data: Record<string, any>[];
  rows_before: number;
  rows_after: number;
}

interface PivotRequest {
  data: Record<string, any>[];
  index_column: string;
  value_columns?: string[];
  pivot_funcs?: Record<string, 'sum' | 'avg' | 'mean' | 'min' | 'max' | 'count'>;
}

interface PivotResponse {
  data: Record<string, any>[];
  rows_before: number;
  rows_after: number;
}

interface TrainModelRequest {
  data: Record<string, any>[];
  model_type: 
    | 'linear' | 'log_log' | 'logistic' | 'ridge' | 'lasso' | 'random_forest' | 'decision_tree' 
    | 'gradient_boosting' | 'elasticnet' | 'svm' | 'knn'
    | 'polynomial' | 'bayesian' | 'quantile' | 'poisson' | 'gamma' | 'tweedie'
    | 'extra_trees' | 'xgboost' | 'lightgbm' | 'catboost' | 'gaussian_process' | 'mlp'
    | 'multinomial_logistic' | 'naive_bayes_gaussian' | 'naive_bayes_multinomial' | 'naive_bayes_bernoulli'
    | 'lda' | 'qda'
    | 'kmeans' | 'dbscan' | 'hierarchical_clustering'
    | 'pca' | 'tsne' | 'umap'
    | 'arima' | 'sarima' | 'exponential_smoothing' | 'lstm' | 'gru'
    | 'isolation_forest' | 'one_class_svm' | 'local_outlier_factor' | 'elliptic_envelope'
    | 'matrix_factorization'
    | 'cox_proportional_hazards' | 'kaplan_meier';
  target_variable?: string;  // Optional for unsupervised models
  features: string[];
  test_size?: number;
  random_state?: number;
  // Regression/Classification parameters
  alpha?: number;
  l1_ratio?: number;
  n_estimators?: number;
  max_depth?: number;
  learning_rate?: number;
  kernel?: string;
  C?: number;
  n_neighbors?: number;
  // Additional parameters
  degree?: number;  // Polynomial
  quantile?: number;  // Quantile regression
  power?: number;  // Tweedie
  iterations?: number;  // CatBoost
  depth?: number;  // CatBoost
  hidden_layer_sizes?: number[];  // MLP
  activation?: string;  // MLP
  solver?: string;  // MLP
  max_iter?: number;  // MLP
  variant?: string;  // Naive Bayes
  // Clustering parameters
  n_clusters?: number;  // K-Means, Hierarchical
  eps?: number;  // DBSCAN
  min_samples?: number;  // DBSCAN
  linkage?: string;  // Hierarchical
  // Dimensionality reduction parameters
  n_components?: number;  // PCA, t-SNE, UMAP
  perplexity?: number;  // t-SNE
  min_dist?: number;  // UMAP
}

interface TrainModelResponse {
  model_type: string;
  task_type: 'regression' | 'classification';
  target_variable: string;
  features: string[];
  coefficients?: {
    intercept: number | number[];
    features: Record<string, number | number[]>;
  } | null;
  metrics: {
    train: Record<string, number>;
    test: Record<string, number>;
    cross_validation: Record<string, number>;
  };
  predictions: number[];
  feature_importance?: Record<string, number> | null;
  n_samples: number;
  n_train: number;
  n_test: number;
  alpha?: number;
  n_estimators?: number;
  max_depth?: number;
}

/**
 * Check if Python service is available
 */
export async function checkPythonServiceHealth(): Promise<boolean> {
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000);
    const response = await fetchFn(`${PYTHON_SERVICE_URL}/health`, {
      method: 'GET',
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    return response.ok;
  } catch (error) {
    console.error('Python service health check failed:', error);
    return false;
  }
}

/**
 * Remove null values from data
 */
export async function removeNulls(
  data: Record<string, any>[],
  column?: string,
  method: 'delete' | 'mean' | 'median' | 'mode' | 'custom' = 'delete',
  customValue?: any
): Promise<RemoveNullsResponse> {
  try {
    // Preprocess data: convert string "null" values to actual null
    // This handles cases where data has string "null" instead of actual null/NaN
    const preprocessedData = data.map(row => {
      const processedRow: Record<string, any> = {};
      for (const [key, value] of Object.entries(row)) {
        // Convert string "null" (case-insensitive) to actual null
        if (typeof value === 'string' && value.toLowerCase().trim() === 'null') {
          processedRow[key] = null;
        } else {
          processedRow[key] = value;
        }
      }
      return processedRow;
    });
    
    const request: RemoveNullsRequest = {
      data: preprocessedData,
      method,
    };
    
    if (column) {
      request.column = column;
    }
    
    if (method === 'custom' && customValue !== undefined) {
      request.custom_value = customValue;
    }
    
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
    const response = await fetchFn(`${PYTHON_SERVICE_URL}/remove-nulls`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(request),
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    
    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Unknown error' }));
      throw new Error(error.detail || `HTTP ${response.status}: ${response.statusText}`);
    }
    
    return await response.json() as RemoveNullsResponse;
  } catch (error) {
    console.error('Error calling Python service remove-nulls:', error);
    throw error;
  }
}

/**
 * Get data preview
 */
export async function getDataPreview(
  data: Record<string, any>[],
  limit: number = 50
): Promise<PreviewResponse> {
  try {
    const request: PreviewRequest = {
      data,
      limit,
    };
    
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
    const response = await fetchFn(`${PYTHON_SERVICE_URL}/preview`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(request),
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    
    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Unknown error' }));
      throw new Error(error.detail || `HTTP ${response.status}: ${response.statusText}`);
    }
    
    return await response.json() as PreviewResponse;
  } catch (error) {
    console.error('Error calling Python service preview:', error);
    throw error;
  }
}

/**
 * Get data summary statistics
 */
export async function getDataSummary(data: Record<string, any>[], column?: string): Promise<SummaryResponse> {
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
    const response = await fetchFn(`${PYTHON_SERVICE_URL}/summary`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ data, column }),
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    
    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Unknown error' }));
      throw new Error(error.detail || `HTTP ${response.status}: ${response.statusText}`);
    }
    
    return await response.json() as SummaryResponse;
  } catch (error) {
    console.error('Error calling Python service summary:', error);
    throw error;
  }
}

/**
 * Create a derived column from an expression
 */
export async function createDerivedColumn(
  data: Record<string, any>[],
  newColumnName: string,
  expression: string
): Promise<CreateDerivedColumnResponse> {
  try {
    const request: CreateDerivedColumnRequest = {
      data,
      new_column_name: newColumnName,
      expression,
    };
    
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
    const response = await fetchFn(`${PYTHON_SERVICE_URL}/create-derived-column`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(request),
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    
    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Unknown error' }));
      throw new Error(error.detail || `HTTP ${response.status}: ${response.statusText}`);
    }
    
    return await response.json() as CreateDerivedColumnResponse;
  } catch (error) {
    console.error('Error calling Python service create-derived-column:', error);
    throw error;
  }
}

/**
 * Convert column data type
 */
export async function convertDataType(
  data: Record<string, any>[],
  column: string,
  targetType: 'numeric' | 'string' | 'date' | 'percentage' | 'boolean'
): Promise<ConvertTypeResponse> {
  try {
    const request: ConvertTypeRequest = {
      data,
      column,
      target_type: targetType,
    };
    
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
    const response = await fetchFn(`${PYTHON_SERVICE_URL}/convert-type`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(request),
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    
    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Unknown error' }));
      throw new Error(error.detail || `HTTP ${response.status}: ${response.statusText}`);
    }
    
    return await response.json() as ConvertTypeResponse;
  } catch (error) {
    console.error('Error calling Python service convert-type:', error);
    throw error;
  }
}

/**
 * Aggregate data by grouping on a column
 */
export async function aggregateData(
  data: Record<string, any>[],
  groupByColumn: string,
  aggColumns?: string[],
  aggFuncs?: Record<string, 'sum' | 'avg' | 'mean' | 'min' | 'max' | 'count' | 'median' | 'std' | 'var' | 'p90' | 'p95' | 'p99' | 'any' | 'all'>,
  orderByColumn?: string,
  orderByDirection?: 'asc' | 'desc',
  userIntent?: string  // User's original message for semantic intent detection
): Promise<AggregateResponse> {
  try {
    const request: AggregateRequest = {
      data,
      group_by_column: groupByColumn,
      agg_columns: aggColumns,
      agg_funcs: aggFuncs,
      order_by_column: orderByColumn,
      order_by_direction: orderByDirection || 'asc',
      user_intent: userIntent,
    };
    
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
    const response = await fetchFn(`${PYTHON_SERVICE_URL}/aggregate`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(request),
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    
    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Unknown error' }));
      throw new Error(error.detail || `HTTP ${response.status}: ${response.statusText}`);
    }
    
    return await response.json() as AggregateResponse;
  } catch (error) {
    console.error('Error calling Python service aggregate:', error);
    throw error;
  }
}

/**
 * Create a pivot table
 */
export async function createPivotTable(
  data: Record<string, any>[],
  indexColumn: string,
  valueColumns?: string[],
  pivotFuncs?: Record<string, 'sum' | 'avg' | 'mean' | 'min' | 'max' | 'count'>
): Promise<PivotResponse> {
  try {
    const request: PivotRequest = {
      data,
      index_column: indexColumn,
      value_columns: valueColumns,
      pivot_funcs: pivotFuncs,
    };
    
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
    const response = await fetchFn(`${PYTHON_SERVICE_URL}/pivot`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(request),
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    
    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Unknown error' }));
      throw new Error(error.detail || `HTTP ${response.status}: ${response.statusText}`);
    }
    
    // Check Content-Length to detect potentially large responses
    const contentLength = response.headers.get('content-length');
    const LARGE_RESPONSE_THRESHOLD = 50 * 1024 * 1024; // 50MB
    
    // For large responses, write to temp file first to avoid string length limits
    if (contentLength && parseInt(contentLength, 10) > LARGE_RESPONSE_THRESHOLD) {
      console.warn(`⚠️ Large pivot response detected (${(parseInt(contentLength, 10) / 1024 / 1024).toFixed(2)}MB). Writing to temp file...`);
      
      const tempFile = path.join(os.tmpdir(), `pivot_${Date.now()}_${Math.random().toString(36).substring(7)}.json`);
      
      try {
        // Write response stream directly to file
        const fileStream = fs.createWriteStream(tempFile);
        const reader = response.body?.getReader();
        
        if (!reader) {
          throw new Error('Response body is not readable');
        }
        
        let done = false;
        while (!done) {
          const { value, done: streamDone } = await reader.read();
          done = streamDone;
          if (value) {
            fileStream.write(Buffer.from(value));
          }
        }
        fileStream.end();
        
        // Wait for file to be fully written
        await new Promise<void>((resolve, reject) => {
          fileStream.on('finish', resolve);
          fileStream.on('error', reject);
        });
        
        // For very large files, we can't parse the entire JSON due to string length limits
        // Instead, save the file buffer to blob storage and parse only a preview
        const fileSize = fs.statSync(tempFile).size;
        const VERY_LARGE_THRESHOLD = 200 * 1024 * 1024; // 200MB - be more aggressive
        
        // Read file buffer (we'll save this to blob storage)
        const fileBuffer = fs.readFileSync(tempFile);
        
        // Try to parse a preview from the beginning of the file
        let previewRows: Record<string, any>[] = [];
        let rowsBefore = 0;
        let rowsAfter = 0;
        
        try {
          // Read just the first 2MB to extract preview and metadata
          const previewBuffer = fileBuffer.slice(0, Math.min(2 * 1024 * 1024, fileBuffer.length));
          const previewText = previewBuffer.toString('utf8');
          
          // Try to extract metadata (rows_before, rows_after) from the JSON
          const rowsBeforeMatch = previewText.match(/"rows_before":\s*(\d+)/);
          const rowsAfterMatch = previewText.match(/"rows_after":\s*(\d+)/);
          
          if (rowsBeforeMatch) rowsBefore = parseInt(rowsBeforeMatch[1], 10);
          if (rowsAfterMatch) rowsAfter = parseInt(rowsAfterMatch[1], 10);
          
          // Try to parse first few rows from preview
          const dataStart = previewText.indexOf('"data":[');
          if (dataStart !== -1) {
            // Find the opening bracket
            let bracketPos = dataStart + 7;
            let bracketCount = 0;
            let inString = false;
            let escapeNext = false;
            let currentRow = '';
            let rowsParsed = 0;
            const maxPreviewRows = 50;
            
            for (let i = bracketPos; i < previewText.length && rowsParsed < maxPreviewRows; i++) {
              const char = previewText[i];
              
              if (escapeNext) {
                escapeNext = false;
                currentRow += char;
                continue;
              }
              
              if (char === '\\') {
                escapeNext = true;
                currentRow += char;
                continue;
              }
              
              if (char === '"') {
                inString = !inString;
                currentRow += char;
                continue;
              }
              
              if (!inString) {
                if (char === '{') {
                  if (bracketCount === 0) {
                    currentRow = '{';
                  } else {
                    currentRow += char;
                  }
                  bracketCount++;
                } else if (char === '}') {
                  currentRow += char;
                  bracketCount--;
                  if (bracketCount === 0) {
                    // Complete row object
                    try {
                      const row = JSON.parse(currentRow);
                      previewRows.push(row);
                      rowsParsed++;
                      currentRow = '';
                    } catch (e) {
                      // Skip invalid row
                      currentRow = '';
                    }
                  }
                } else if (char === ']' && bracketCount === 0) {
                  // End of data array
                  break;
                } else {
                  currentRow += char;
                }
              } else {
                currentRow += char;
              }
            }
          }
        } catch (previewError) {
          console.warn('⚠️ Could not parse preview from large file:', previewError);
        }
        
        // Clean up temp file
        try {
          fs.unlinkSync(tempFile);
        } catch (unlinkError) {
          console.warn('⚠️ Could not delete temp file:', unlinkError);
        }
        
        // Return response with file buffer for blob storage and preview only
        return {
          data: previewRows, // Preview only (first 50 rows)
          rows_before: rowsBefore,
          rows_after: rowsAfter,
          _largeFileBuffer: fileBuffer, // Special flag - buffer to save to blob storage
        } as any;
      } catch (error) {
        // Clean up temp file on error (unlinkSync is synchronous, no .catch needed)
        if (fs.existsSync(tempFile)) {
          try {
            fs.unlinkSync(tempFile);
          } catch (unlinkError) {
            console.warn('⚠️ Could not delete temp file on error:', unlinkError);
          }
        }
        
        if (error instanceof Error && (
          error.message.includes('Cannot create a string longer than') ||
          error.message.includes('ERR_STRING_TOO_LONG')
        )) {
          throw new Error(
            `Pivot table result is too large to process (${contentLength ? (parseInt(contentLength, 10) / 1024 / 1024).toFixed(2) + 'MB' : 'very large'}). ` +
            `The pivot operation created too many columns. ` +
            `Please try: 1) Pivoting on a column with fewer unique values, ` +
            `2) Filtering your data first, or 3) Specifying only specific value columns to aggregate.`
          );
        }
        throw error;
      }
    }
    
    // For normal-sized responses, parse JSON normally
    try {
      return await response.json() as PivotResponse;
    } catch (jsonError) {
      // If it's the string length error, provide helpful message
      if (jsonError instanceof Error && (
        jsonError.message.includes('Cannot create a string longer than') ||
        jsonError.message.includes('ERR_STRING_TOO_LONG')
      )) {
        throw new Error(
          `Pivot table result is too large to process in memory. ` +
          `The pivot operation created too many columns. ` +
          `Please try: 1) Pivoting on a column with fewer unique values, ` +
          `2) Filtering your data first, or 3) Specifying only specific value columns to aggregate.`
        );
      }
      throw jsonError;
    }
  } catch (error) {
    // Check if it's the string length error
    if (error instanceof Error && error.message.includes('Cannot create a string longer than')) {
      throw new Error(
        `Pivot table result is too large. The pivot operation created too many columns. ` +
        `This usually happens when the pivot column has many unique values. ` +
        `Please try: 1) Pivoting on a column with fewer unique values, 2) Filtering your data first, ` +
        `or 3) Specifying only specific value columns to aggregate.`
      );
    }
    console.error('Error calling Python service pivot:', error);
    throw error;
  }
}

/**
 * Train a machine learning model
 */
export async function trainMLModel(
  data: Record<string, any>[],
  modelType: 
    | 'linear' | 'log_log' | 'logistic' | 'ridge' | 'lasso' | 'random_forest' | 'decision_tree' 
    | 'gradient_boosting' | 'elasticnet' | 'svm' | 'knn'
    | 'polynomial' | 'bayesian' | 'quantile' | 'poisson' | 'gamma' | 'tweedie'
    | 'extra_trees' | 'xgboost' | 'lightgbm' | 'catboost' | 'gaussian_process' | 'mlp'
    | 'multinomial_logistic' | 'naive_bayes_gaussian' | 'naive_bayes_multinomial' | 'naive_bayes_bernoulli'
    | 'lda' | 'qda'
    | 'kmeans' | 'dbscan' | 'hierarchical_clustering'
    | 'pca' | 'tsne' | 'umap'
    | 'arima' | 'sarima' | 'exponential_smoothing' | 'lstm' | 'gru'
    | 'isolation_forest' | 'one_class_svm' | 'local_outlier_factor' | 'elliptic_envelope'
    | 'matrix_factorization'
    | 'cox_proportional_hazards' | 'kaplan_meier',
  targetVariable: string,
  features: string[],
  options?: {
    testSize?: number;
    randomState?: number;
    alpha?: number;
    l1Ratio?: number;
    nEstimators?: number;
    maxDepth?: number;
    learningRate?: number;
    kernel?: string;
    C?: number;
    nNeighbors?: number;
    degree?: number;
    quantile?: number;
    power?: number;
    iterations?: number;
    depth?: number;
    hiddenLayerSizes?: number[];
    activation?: string;
    solver?: string;
    maxIter?: number;
    variant?: string;
    nClusters?: number;
    eps?: number;
    minSamples?: number;
    linkage?: string;
    nComponents?: number;
    perplexity?: number;
    minDist?: number;
    dateColumn?: string;
    order?: number[];
    seasonalOrder?: number[];
    trend?: string;
    seasonal?: string;
    seasonalPeriods?: number;
    sequenceLength?: number;
    lstmUnits?: number;
    gruUnits?: number;
    epochs?: number;
    contamination?: number;
    nu?: number;
    userColumn?: string;
    itemColumn?: string;
    ratingColumn?: string;
    nFactors?: number;
    nEpochs?: number;
    regularization?: number;
    durationColumn?: string;
    eventColumn?: string;
    groupColumn?: string;
  }
): Promise<TrainModelResponse> {
  try {
    // Unsupervised models don't need target_variable
    const unsupervisedModels = [
      'kmeans', 'dbscan', 'hierarchical_clustering', 'pca', 'tsne', 'umap',
      'isolation_forest', 'one_class_svm', 'local_outlier_factor', 'elliptic_envelope'
    ];
    const isUnsupervised = unsupervisedModels.includes(modelType);
    
    console.log(`📤 Preparing train-model request: model_type="${modelType}", target="${targetVariable}", features=[${features.slice(0, 3).join(', ')}${features.length > 3 ? '...' : ''}]`);
    
    const request: TrainModelRequest = {
      data,
      model_type: modelType,
      features,
      test_size: options?.testSize ?? 0.2,
      random_state: options?.randomState ?? 42,
    };
    
    // Validate model_type is valid
    if (!modelType || typeof modelType !== 'string') {
      throw new Error(`Invalid model type: ${modelType}`);
    }
    
    console.log(`📋 Request payload: model_type="${request.model_type}", target_variable="${request.target_variable}", features count=${request.features.length}`);
    
    // Add target_variable only for supervised models
    if (!isUnsupervised && targetVariable) {
      request.target_variable = targetVariable;
    }
    
    // Add optional parameters based on model type
    // Common parameters
    if (['ridge', 'lasso', 'elasticnet', 'quantile', 'poisson', 'gamma', 'tweedie', 'mlp'].includes(modelType)) {
      if (options?.alpha !== undefined) {
        request.alpha = options.alpha;
      }
    }
    
    if (['random_forest', 'gradient_boosting', 'extra_trees', 'xgboost', 'lightgbm'].includes(modelType)) {
      if (options?.nEstimators !== undefined) {
        request.n_estimators = options.nEstimators;
      }
    }
    
    if (['random_forest', 'decision_tree', 'gradient_boosting', 'extra_trees', 'xgboost', 'lightgbm', 'catboost'].includes(modelType)) {
      if (options?.maxDepth !== undefined) {
        request.max_depth = options.maxDepth;
      }
    }
    
    if (['gradient_boosting', 'xgboost', 'lightgbm', 'catboost'].includes(modelType)) {
      if (options?.learningRate !== undefined) {
        request.learning_rate = options.learningRate;
      }
    }
    
    // Model-specific parameters
    if (modelType === 'elasticnet' && options?.l1Ratio !== undefined) {
      request.l1_ratio = options.l1Ratio;
    }
    
    if (modelType === 'svm') {
      if (options?.kernel !== undefined) request.kernel = options.kernel;
      if (options?.C !== undefined) request.C = options.C;
    }
    
    if (['knn', 'umap'].includes(modelType) && options?.nNeighbors !== undefined) {
      request.n_neighbors = options.nNeighbors;
    }
    
    if (modelType === 'polynomial' && options?.degree !== undefined) {
      request.degree = options.degree;
    }
    
    if (modelType === 'quantile' && options?.quantile !== undefined) {
      request.quantile = options.quantile;
    }
    
    if (modelType === 'tweedie' && options?.power !== undefined) {
      request.power = options.power;
    }
    
    if (modelType === 'catboost') {
      if (options?.iterations !== undefined) request.iterations = options.iterations;
      if (options?.depth !== undefined) request.depth = options.depth;
    }
    
    if (modelType === 'mlp') {
      if (options?.hiddenLayerSizes !== undefined) request.hidden_layer_sizes = options.hiddenLayerSizes;
      if (options?.activation !== undefined) request.activation = options.activation;
      if (options?.solver !== undefined) request.solver = options.solver;
      if (options?.maxIter !== undefined) request.max_iter = options.maxIter;
    }
    
    if (modelType.startsWith('naive_bayes_') && options?.variant !== undefined) {
      request.variant = options.variant;
    }
    
    // Clustering parameters
    if (['kmeans', 'hierarchical_clustering'].includes(modelType) && options?.nClusters !== undefined) {
      request.n_clusters = options.nClusters;
    }
    
    if (modelType === 'dbscan') {
      if (options?.eps !== undefined) request.eps = options.eps;
      if (options?.minSamples !== undefined) request.min_samples = options.minSamples;
    }
    
    if (modelType === 'hierarchical_clustering' && options?.linkage !== undefined) {
      request.linkage = options.linkage;
    }
    
    // Dimensionality reduction parameters
    if (['pca', 'tsne', 'umap'].includes(modelType) && options?.nComponents !== undefined) {
      request.n_components = options.nComponents;
    }
    
    if (modelType === 'tsne' && options?.perplexity !== undefined) {
      request.perplexity = options.perplexity;
    }
    
    if (modelType === 'umap' && options?.minDist !== undefined) {
      request.min_dist = options.minDist;
    }
    
    // Time series parameters
    if (['arima', 'sarima', 'exponential_smoothing'].includes(modelType)) {
      if (options?.dateColumn !== undefined) request.date_column = options.dateColumn;
    }
    
    if (['arima', 'sarima'].includes(modelType)) {
      if (options?.order !== undefined) request.order = options.order;
      if (options?.seasonalOrder !== undefined) request.seasonal_order = options.seasonalOrder;
    }
    
    if (modelType === 'exponential_smoothing') {
      if (options?.trend !== undefined) request.trend = options.trend;
      if (options?.seasonal !== undefined) request.seasonal = options.seasonal;
      if (options?.seasonalPeriods !== undefined) request.seasonal_periods = options.seasonalPeriods;
    }
    
    if (['lstm', 'gru'].includes(modelType)) {
      if (options?.sequenceLength !== undefined) request.sequence_length = options.sequenceLength;
      if (options?.epochs !== undefined) request.epochs = options.epochs;
    }
    
    if (modelType === 'lstm' && options?.lstmUnits !== undefined) {
      request.lstm_units = options.lstmUnits;
    }
    
    if (modelType === 'gru' && options?.gruUnits !== undefined) {
      request.gru_units = options.gruUnits;
    }
    
    // Anomaly detection parameters
    if (['isolation_forest', 'local_outlier_factor', 'elliptic_envelope'].includes(modelType)) {
      if (options?.contamination !== undefined) request.contamination = options.contamination;
    }
    
    if (modelType === 'one_class_svm') {
      if (options?.nu !== undefined) request.nu = options.nu;
      if (options?.kernel !== undefined) request.kernel = options.kernel;
    }
    
    // Recommendation system parameters
    if (modelType === 'matrix_factorization') {
      if (options?.userColumn !== undefined) request.user_column = options.userColumn;
      if (options?.itemColumn !== undefined) request.item_column = options.itemColumn;
      if (options?.ratingColumn !== undefined) request.rating_column = options.ratingColumn;
      if (options?.nFactors !== undefined) request.n_factors = options.nFactors;
      if (options?.nEpochs !== undefined) request.n_epochs = options.nEpochs;
      if (options?.regularization !== undefined) request.regularization = options.regularization;
    }
    
    // Survival analysis parameters
    if (['cox_proportional_hazards', 'kaplan_meier'].includes(modelType)) {
      if (options?.durationColumn !== undefined) request.duration_column = options.durationColumn;
      if (options?.eventColumn !== undefined) request.event_column = options.eventColumn;
    }
    
    if (modelType === 'kaplan_meier' && options?.groupColumn !== undefined) {
      request.group_column = options.groupColumn;
    }
    
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
    const response = await fetchFn(`${PYTHON_SERVICE_URL}/train-model`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(request),
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    
    if (!response.ok) {
      const errorText = await response.text().catch(() => 'Unknown error');
      let errorDetail = 'Unknown error';
      try {
        const errorJson = JSON.parse(errorText);
        errorDetail = errorJson.detail || errorJson.message || errorText;
      } catch {
        errorDetail = errorText || `HTTP ${response.status}: ${response.statusText}`;
      }
      console.error(`❌ Python service error (${response.status}):`, errorDetail);
      throw new Error(errorDetail);
    }
    
    const result = await response.json() as TrainModelResponse;
    console.log(`✅ Python service returned model type: ${result.model_type}`);
    return result;
  } catch (error) {
    console.error('❌ Error calling Python service train-model:', {
      modelType,
      targetVariable,
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : undefined
    });
    throw error;
  }
}

/**
 * Identify outliers in data
 */
interface IdentifyOutliersRequest {
  data: Record<string, any>[];
  column?: string;
  method: 'iqr' | 'zscore' | 'isolation_forest' | 'local_outlier_factor';
  threshold?: number;
}

interface IdentifyOutliersResponse {
  outliers: Array<{
    row_index: number;
    column: string;
    value: number;
    z_score?: number;
    iqr_lower?: number;
    iqr_upper?: number;
    method: string;
  }>;
  summary: {
    total_outliers: number;
    columns_analyzed: string[];
    outliers_by_column: Record<string, number>;
  };
  statistics?: Record<string, {
    mean: number;
    median: number;
    std_dev: number;
    q1: number;
    q3: number;
    iqr: number;
    lower_bound: number;
    upper_bound: number;
  }>;
}

export async function identifyOutliers(
  data: Record<string, any>[],
  column?: string,
  method: 'iqr' | 'zscore' | 'isolation_forest' | 'local_outlier_factor' = 'iqr',
  threshold?: number
): Promise<IdentifyOutliersResponse> {
  try {
    const request: IdentifyOutliersRequest = {
      data,
      method,
    };
    
    if (column) {
      request.column = column;
    }
    
    if (threshold !== undefined) {
      request.threshold = threshold;
    }
    
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
    const response = await fetchFn(`${PYTHON_SERVICE_URL}/identify-outliers`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(request),
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    
    if (!response.ok) {
      const errorText = await response.text().catch(() => 'Unknown error');
      let errorDetail = 'Unknown error';
      try {
        const errorJson = JSON.parse(errorText);
        errorDetail = errorJson.detail || errorJson.message || errorText;
      } catch {
        errorDetail = errorText || `HTTP ${response.status}: ${response.statusText}`;
      }
      console.error(`❌ Python service error (${response.status}):`, errorDetail);
      throw new Error(errorDetail);
    }
    
    return await response.json() as IdentifyOutliersResponse;
  } catch (error) {
    console.error('Error calling Python service identify-outliers:', error);
    throw error;
  }
}

/**
 * Treat outliers in data
 */
interface TreatOutliersRequest {
  data: Record<string, any>[];
  column?: string;
  method: 'iqr' | 'zscore' | 'isolation_forest' | 'local_outlier_factor';
  threshold?: number;
  treatment: 'remove' | 'cap' | 'winsorize' | 'transform' | 'impute';
  treatment_value?: 'mean' | 'median' | 'mode' | 'min' | 'max' | number;
}

interface TreatOutliersResponse {
  data: Record<string, any>[];
  rows_before: number;
  rows_after: number;
  outliers_treated: number;
  treatment_applied: string;
  summary: {
    columns_treated: string[];
    outliers_by_column: Record<string, number>;
  };
}

export async function treatOutliers(
  data: Record<string, any>[],
  column?: string,
  method: 'iqr' | 'zscore' | 'isolation_forest' | 'local_outlier_factor' = 'iqr',
  threshold?: number,
  treatment: 'remove' | 'cap' | 'winsorize' | 'transform' | 'impute' = 'remove',
  treatmentValue?: 'mean' | 'median' | 'mode' | 'min' | 'max' | number
): Promise<TreatOutliersResponse> {
  try {
    const request: TreatOutliersRequest = {
      data,
      method,
      treatment,
    };
    
    if (column) {
      request.column = column;
    }
    
    if (threshold !== undefined) {
      request.threshold = threshold;
    }
    
    if (treatmentValue !== undefined) {
      request.treatment_value = treatmentValue;
    }
    
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
    const response = await fetchFn(`${PYTHON_SERVICE_URL}/treat-outliers`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(request),
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    
    if (!response.ok) {
      const errorText = await response.text().catch(() => 'Unknown error');
      let errorDetail = 'Unknown error';
      try {
        const errorJson = JSON.parse(errorText);
        errorDetail = errorJson.detail || errorJson.message || errorText;
      } catch {
        errorDetail = errorText || `HTTP ${response.status}: ${response.statusText}`;
      }
      console.error(`❌ Python service error (${response.status}):`, errorDetail);
      throw new Error(errorDetail);
    }
    
    return await response.json() as TreatOutliersResponse;
  } catch (error) {
    console.error('Error calling Python service treat-outliers:', error);
    throw error;
  }
}

