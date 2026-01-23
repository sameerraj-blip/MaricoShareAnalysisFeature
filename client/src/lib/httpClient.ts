import axios, { AxiosError, AxiosRequestConfig, AxiosResponse } from "axios";
import { getUserEmail } from "@/utils/userStorage";
import { API_BASE_URL } from "@/lib/config";
import { logger } from "@/lib/logger";

// Dedicated axios instance for server communication
export const apiClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 0,
  withCredentials: true,
  headers: {
    "Content-Type": "application/json",
  },
});

apiClient.interceptors.request.use(
  (config) => {
    logger.log(`Making ${config.method?.toUpperCase()} request to: ${config.url}`);
    const userEmail = getUserEmail();
    if (userEmail) {
      config.headers = config.headers || {};
      config.headers["X-User-Email"] = userEmail;
      logger.log(`Adding user email to headers: ${userEmail}`);
    }
    return config;
  },
  (error) => Promise.reject(error)
);

apiClient.interceptors.response.use(
  (response: AxiosResponse) => response,
  async (error: AxiosError) => {
    if (
      axios.isCancel(error) ||
      error?.code === "ERR_CANCELED" ||
      error?.name === "AbortError"
    ) {
      logger.log("🚫 Request was cancelled");
      const cancelError = new Error("Request cancelled");
      (cancelError as any).isCancel = true;
      (cancelError as any).code = "ERR_CANCELED";
      throw cancelError;
    }

    if (
      error.code === "ERR_NETWORK" ||
      error.message.includes("CORS") ||
      error.message.includes("Network Error") ||
      error.message.includes("Failed to fetch")
    ) {
      logger.log("CORS/Network error detected, retrying once...");
      try {
        if (error.config) {
          return await apiClient.request(error.config);
        }
      } catch (retryError) {
        logger.log("Retry failed:", retryError);
        throw new Error("Network error: CORS issue persists after retry");
      }
    }

    if (error.response) {
      const errorData = error.response.data as any;
      const message =
        errorData?.error ||
        errorData?.message ||
        error.message ||
        "Request failed";
      throw new Error(message);
    }

    if (error.request) {
      throw new Error("Network error: No response from server");
    }

    throw new Error(`Request error: ${error.message}`);
  }
);

export interface ApiRequestOptions {
  method: "GET" | "POST" | "PUT" | "DELETE" | "PATCH";
  route: string;
  data?: any;
  config?: AxiosRequestConfig;
  signal?: AbortSignal;
}

export async function apiRequest<T = any>({
  method,
  route,
  data,
  config = {},
  signal,
}: ApiRequestOptions): Promise<T> {
  try {
    logger.log(`🌐 Making ${method} request to ${route}`);
    const response = await apiClient.request({
      method,
      url: route,
      data,
      signal,
      ...config,
    });
    logger.log(`✅ ${method} ${route} - Status: ${response.status}`);
    logger.log("📦 Response data:", response.data);
    return response.data;
  } catch (error: any) {
    if (
      axios.isCancel(error) ||
      error?.name === "AbortError" ||
      error?.code === "ERR_CANCELED" ||
      error?.isCancel === true ||
      error?.message === "Request cancelled"
    ) {
      logger.log(`🚫 ${method} ${route} was cancelled`);
      throw new Error("Request cancelled");
    }
    logger.error(`❌ ${method} ${route} failed:`, error);
    throw error;
  }
}

export const api = {
  get: <T = any>(
    route: string,
    config?: AxiosRequestConfig & { signal?: AbortSignal }
  ) => apiRequest<T>({ method: "GET", route, config, signal: config?.signal }),

  post: <T = any>(
    route: string,
    data?: any,
    config?: AxiosRequestConfig & { signal?: AbortSignal }
  ) => apiRequest<T>({ method: "POST", route, data, config, signal: config?.signal }),

  put: <T = any>(
    route: string,
    data?: any,
    config?: AxiosRequestConfig & { signal?: AbortSignal }
  ) => apiRequest<T>({ method: "PUT", route, data, config, signal: config?.signal }),

  patch: <T = any>(
    route: string,
    data?: any,
    config?: AxiosRequestConfig & { signal?: AbortSignal }
  ) => apiRequest<T>({ method: "PATCH", route, data, config, signal: config?.signal }),

  delete: <T = any>(
    route: string,
    config?: AxiosRequestConfig & { signal?: AbortSignal }
  ) => apiRequest<T>({ method: "DELETE", route, config, signal: config?.signal }),
};

export async function uploadFile<T = any>(
  route: string,
  file: File,
  additionalData?: Record<string, any>
): Promise<T> {
  const formData = new FormData();
  formData.append("file", file);

  if (additionalData) {
    Object.entries(additionalData).forEach(([key, value]) => {
      formData.append(key, value);
    });
  }

  const userEmail = getUserEmail();
  const headers: Record<string, string> = {
    "Content-Type": "multipart/form-data",
  };

  if (userEmail) {
    headers["X-User-Email"] = userEmail;
    logger.log(`Adding user email to upload headers: ${userEmail}`);
  }

  return apiRequest<T>({
    method: "POST",
    route,
    data: formData,
    config: { headers },
  });
}


