import { api } from "@/lib/httpClient";

export const sessionsApi = {
  getAllSessions: () => api.get("/api/sessions"),

  getSessionsPaginated: (pageSize: number = 10, continuationToken?: string) => {
    const params = new URLSearchParams({ pageSize: pageSize.toString() });
    if (continuationToken) {
      params.append("continuationToken", continuationToken);
    }
    return api.get(`/sessions/paginated?${params}`);
  },

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

  getSessionStatistics: () => api.get("/api/sessions/statistics"),

  getSessionDetails: (sessionId: string) =>
    api.get(`/api/sessions/details/${sessionId}`),

  getSessionsByUser: (username: string) =>
    api.get(`/api/sessions/user/${username}`),

  updateSessionName: (sessionId: string, fileName: string) =>
    api.patch(`/api/sessions/${sessionId}`, { fileName }),

  updateSessionContext: (sessionId: string, permanentContext: string) =>
    api.patch(`/api/sessions/${sessionId}/context`, { permanentContext }),

  deleteSession: (sessionId: string) => api.delete(`/api/sessions/${sessionId}`),
};


