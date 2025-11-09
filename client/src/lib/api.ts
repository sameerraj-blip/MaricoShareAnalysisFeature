import axios, { AxiosError, AxiosRequestConfig, AxiosResponse } from 'axios';
import { getUserEmail } from '@/utils/userStorage';
import { 
  UploadResponse, 
  ChatResponse,
  UserAnalysisSessionsResponse,
  CompleteAnalysisData,
  ColumnStatisticsResponse,
  RawDataResponse,
  Dashboard,
  ChartSpec,
} from '@shared/schema';

// Base configuration for your backend
const API_BASE_URL = import.meta.env.VITE_API_URL || 
  (import.meta.env.PROD 
    ? (typeof window !== 'undefined' ? window.location.origin : 'https://marico-insight-safe.vercel.app')
    : 'http://localhost:3002');

// Create axios instance with default configuration
const apiClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000, // 30 seconds timeout
  withCredentials: true,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor for adding user email to headers
apiClient.interceptors.request.use(
  (config) => {
    console.log(`Making ${config.method?.toUpperCase()} request to: ${config.url}`);
    
    // Add user email to headers if available
    const userEmail = getUserEmail();
    if (userEmail) {
      config.headers = config.headers || {};
      config.headers['X-User-Email'] = userEmail;
      console.log(`Adding user email to headers: ${userEmail}`);
    }
    
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Response interceptor for error handling with CORS retry logic
apiClient.interceptors.response.use(
  (response: AxiosResponse) => {
    return response;
  },
  async (error: AxiosError) => {
    // Handle CORS and network errors with retry logic
    if (error.code === 'ERR_NETWORK' || 
        error.message.includes('CORS') || 
        error.message.includes('Network Error') ||
        error.message.includes('Failed to fetch')) {
      
      console.log('CORS/Network error detected, retrying once...');
      
      // Retry once for CORS/network errors
      try {
        if (error.config) {
          const retryResponse = await apiClient.request(error.config);
          return retryResponse;
        }
      } catch (retryError) {
        console.log('Retry failed:', retryError);
        throw new Error('Network error: CORS issue persists after retry');
      }
    }
    
    if (error.response) {
      // Server responded with error status
      const message = (error.response.data as any)?.message || error.message || 'Request failed';
      throw new Error(`${error.response.status}: ${message}`);
    } else if (error.request) {
      // Request was made but no response received
      throw new Error('Network error: No response from server');
    } else {
      // Something else happened
      throw new Error(`Request error: ${error.message}`);
    }
  }
);

// Generic API request function
export interface ApiRequestOptions {
  method: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH';
  route: string;
  data?: any;
  config?: AxiosRequestConfig;
}

export async function apiRequest<T = any>({
  method,
  route,
  data,
  config = {}
}: ApiRequestOptions): Promise<T> {
  try {
    console.log(`🌐 Making ${method} request to ${route}`);
    const response = await apiClient.request({
      method,
      url: route,
      data,
      ...config,
    });
    console.log(`✅ ${method} ${route} - Status: ${response.status}`);
    console.log('📦 Response data:', response.data);
    return response.data;
  } catch (error) {
    console.error(`❌ ${method} ${route} failed:`, error);
    throw error; // Error is already handled by interceptor
  }
}

// Convenience methods for common HTTP methods
export const api = {
  get: <T = any>(route: string, config?: AxiosRequestConfig) =>
    apiRequest<T>({ method: 'GET', route, config }),
  
  post: <T = any>(route: string, data?: any, config?: AxiosRequestConfig) =>
    apiRequest<T>({ method: 'POST', route, data, config }),
  
  put: <T = any>(route: string, data?: any, config?: AxiosRequestConfig) =>
    apiRequest<T>({ method: 'PUT', route, data, config }),
  
  patch: <T = any>(route: string, data?: any, config?: AxiosRequestConfig) =>
    apiRequest<T>({ method: 'PATCH', route, data, config }),
  
  delete: <T = any>(route: string, config?: AxiosRequestConfig) =>
    apiRequest<T>({ method: 'DELETE', route, config }),
};

// File upload helper
export async function uploadFile<T = any>(
  route: string,
  file: File,
  additionalData?: Record<string, any>
): Promise<T> {
  const formData = new FormData();
  formData.append('file', file);
  
  // Add any additional data to formData
  if (additionalData) {
    Object.entries(additionalData).forEach(([key, value]) => {
      formData.append(key, value);
    });
  }
  
  // Get user email for headers
  const userEmail = getUserEmail();
  const headers: Record<string, string> = {
    'Content-Type': 'multipart/form-data',
  };
  
  if (userEmail) {
    headers['X-User-Email'] = userEmail;
    console.log(`Adding user email to upload headers: ${userEmail}`);
  }
  
  return apiRequest<T>({
    method: 'POST',
    route,
    data: formData,
    config: {
      headers,
    },
  });
}

// Data retrieval API functions
export const dataApi = {
  // Get all analysis sessions for a user
  getUserSessions: (username: string) =>
    api.get<UserAnalysisSessionsResponse>(`/data/user/${username}/sessions`),
  
  // Get complete analysis data for a specific chat
  getAnalysisData: (chatId: string, username: string) =>
    api.get<CompleteAnalysisData>(`/data/chat/${chatId}?username=${username}`),
  
  // Get analysis data by session ID
  getAnalysisDataBySession: (sessionId: string) =>
    api.get<CompleteAnalysisData>(`/data/session/${sessionId}`),
  
  // Get column statistics for a specific analysis
  getColumnStatistics: (chatId: string, username: string) =>
    api.get<ColumnStatisticsResponse>(`/data/chat/${chatId}/statistics?username=${username}`),
  
  // Get raw data for a specific analysis (with pagination)
  getRawData: (chatId: string, username: string, page = 1, limit = 100) =>
    api.get<RawDataResponse>(`/data/chat/${chatId}/raw-data?username=${username}&page=${page}&limit=${limit}`),
};

// Sessions API functions
export const sessionsApi = {
  // Get all sessions for the current user
  getAllSessions: () => api.get('/api/sessions'),
  
  // Get sessions with pagination
  getSessionsPaginated: (pageSize: number = 10, continuationToken?: string) => {
    const params = new URLSearchParams({ pageSize: pageSize.toString() });
    if (continuationToken) {
      params.append('continuationToken', continuationToken);
    }
    return api.get(`/sessions/paginated?${params}`);
  },
  
  // Get sessions with filters
  getSessionsFiltered: (filters: {
    startDate?: string;
    endDate?: string;
    fileName?: string;
    minMessageCount?: number;
    maxMessageCount?: number;
  }) => {
    const params = new URLSearchParams();
    Object.entries(filters).forEach(([key, value]) => {
      if (value !== undefined && value !== null) {
        params.append(key, value.toString());
      }
    });
    return api.get(`/api/sessions/filtered?${params}`);
  },
  
  // Get session statistics
  getSessionStatistics: () => api.get('/api/sessions/statistics'),
  
  // Get detailed session by session ID
  getSessionDetails: (sessionId: string) => api.get(`/api/sessions/details/${sessionId}`),
  
  // Get sessions by specific user
  getSessionsByUser: (username: string) => api.get(`/api/sessions/user/${username}`),
  
  // Delete session by session ID
  deleteSession: (sessionId: string) => api.delete(`/api/sessions/${sessionId}`),
};

export default apiClient;

// Dashboards API
export const dashboardsApi = {
  list: () => api.get<{ dashboards: Dashboard[] }>('/api/dashboards'),
  create: (name: string, charts?: ChartSpec[]) =>
    api.post<Dashboard>('/api/dashboards', { name, charts }),
  remove: (dashboardId: string) =>
    api.delete(`/api/dashboards/${dashboardId}`),
  addChart: (dashboardId: string, chart: ChartSpec) =>
    api.post<Dashboard>(`/api/dashboards/${dashboardId}/charts`, { chart }),
  removeChart: (dashboardId: string, payload: { index?: number; title?: string; type?: ChartSpec['type'] }) =>
    api.delete<Dashboard>(`/api/dashboards/${dashboardId}/charts`, { data: payload as any }),
};
