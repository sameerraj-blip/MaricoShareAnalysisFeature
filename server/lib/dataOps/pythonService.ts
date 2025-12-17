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

interface TrainModelRequest {
  data: Record<string, any>[];
  model_type: 'linear' | 'logistic' | 'ridge' | 'lasso' | 'random_forest' | 'decision_tree' | 'gradient_boosting' | 'elasticnet' | 'svm' | 'knn';
  target_variable: string;
  features: string[];
  test_size?: number;
  random_state?: number;
  alpha?: number;
  l1_ratio?: number;  // For ElasticNet
  n_estimators?: number;
  max_depth?: number;
  learning_rate?: number;  // For Gradient Boosting
  kernel?: string;  // For SVM
  C?: number;  // For SVM
  n_neighbors?: number;  // For KNN
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
    const request: RemoveNullsRequest = {
      data,
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
 * Train a machine learning model
 */
export async function trainMLModel(
  data: Record<string, any>[],
  modelType: 'linear' | 'logistic' | 'ridge' | 'lasso' | 'random_forest' | 'decision_tree' | 'gradient_boosting' | 'elasticnet' | 'svm' | 'knn',
  targetVariable: string,
  features: string[],
  options?: {
    testSize?: number;
    randomState?: number;
    alpha?: number;
    l1Ratio?: number;  // For ElasticNet
    nEstimators?: number;
    maxDepth?: number;
    learningRate?: number;  // For Gradient Boosting
    kernel?: string;  // For SVM
    C?: number;  // For SVM
    nNeighbors?: number;  // For KNN
  }
): Promise<TrainModelResponse> {
  try {
    const request: TrainModelRequest = {
      data,
      model_type: modelType,
      target_variable: targetVariable,
      features,
      test_size: options?.testSize ?? 0.2,
      random_state: options?.randomState ?? 42,
    };
    
    // Add optional parameters based on model type
    if (modelType === 'ridge' || modelType === 'lasso') {
      if (options?.alpha !== undefined) {
        request.alpha = options.alpha;
      }
    }
    
    if (modelType === 'random_forest') {
      if (options?.nEstimators !== undefined) {
        request.n_estimators = options.nEstimators;
      }
      if (options?.maxDepth !== undefined) {
        request.max_depth = options.maxDepth;
      }
    }
    
    if (modelType === 'decision_tree') {
      if (options?.maxDepth !== undefined) {
        request.max_depth = options.maxDepth;
      }
    }
    
    if (modelType === 'gradient_boosting') {
      if (options?.nEstimators !== undefined) {
        request.n_estimators = options.nEstimators;
      }
      if (options?.maxDepth !== undefined) {
        request.max_depth = options.maxDepth;
      }
      if (options?.learningRate !== undefined) {
        request.learning_rate = options.learningRate;
      }
    }
    
    if (modelType === 'elasticnet') {
      if (options?.alpha !== undefined) {
        request.alpha = options.alpha;
      }
      if (options?.l1Ratio !== undefined) {
        request.l1_ratio = options.l1Ratio;
      }
    }
    
    if (modelType === 'svm') {
      if (options?.kernel !== undefined) {
        request.kernel = options.kernel;
      }
      if (options?.C !== undefined) {
        request.C = options.C;
      }
    }
    
    if (modelType === 'knn') {
      if (options?.nNeighbors !== undefined) {
        request.n_neighbors = options.nNeighbors;
      }
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
      const error = await response.json().catch(() => ({ detail: 'Unknown error' }));
      throw new Error(error.detail || `HTTP ${response.status}: ${response.statusText}`);
    }
    
    return await response.json() as TrainModelResponse;
  } catch (error) {
    console.error('Error calling Python service train-model:', error);
    throw error;
  }
}

