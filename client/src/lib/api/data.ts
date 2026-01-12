import { api } from "@/lib/httpClient";
import {
  ColumnStatisticsResponse,
  CompleteAnalysisData,
  RawDataResponse,
  UserAnalysisSessionsResponse,
} from "@/shared/schema";

export const dataApi = {
  getUserSessions: (username: string) =>
    api.get<UserAnalysisSessionsResponse>(`/data/user/${username}/sessions`),

  getAnalysisData: (chatId: string, username: string) =>
    api.get<CompleteAnalysisData>(`/data/chat/${chatId}?username=${username}`),

  getAnalysisDataBySession: (sessionId: string) =>
    api.get<CompleteAnalysisData>(`/data/session/${sessionId}`),

  getColumnStatistics: (chatId: string, username: string) =>
    api.get<ColumnStatisticsResponse>(
      `/data/chat/${chatId}/statistics?username=${username}`
    ),

  getRawData: (chatId: string, username: string, page = 1, limit = 100) =>
    api.get<RawDataResponse>(
      `/data/chat/${chatId}/raw-data?username=${username}&page=${page}&limit=${limit}`
    ),

  getDataSummary: (sessionId: string) =>
    api.get<{
      summary: Array<{
        variable: string;
        datatype: string;
        total_values: number;
        null_values: number;
        non_null_values: number;
        mean?: number | null;
        median?: number | null;
        mode?: any;
        std_dev?: number | null;
        min?: number | string | null;
        max?: number | string | null;
      }>;
      qualityScore: number;
      recommendedQuestions: string[];
    }>(`/api/sessions/${sessionId}/data-summary`),
};


