import { api } from "@/lib/httpClient";
import {
  AnalysisSessionSummary,
  SharedAnalysesResponse,
  SharedAnalysisInvite,
} from "@/shared/schema";

export const sharedAnalysesApi = {
  share: (payload: { 
    sessionId: string; 
    targetEmail: string; 
    note?: string; 
    dashboardId?: string; 
    isEditable?: boolean;
    dashboardIds?: string[];
    dashboardPermissions?: Record<string, 'view' | 'edit'>;
  }) =>
    api.post<{ invite: SharedAnalysisInvite }>("/api/shared-analyses", payload),

  getIncoming: () => api.get<SharedAnalysesResponse>("/api/shared-analyses/incoming"),

  getSent: () =>
    api.get<{ invitations: SharedAnalysisInvite[] }>("/api/shared-analyses/sent"),

  getInvite: (inviteId: string) =>
    api.get<{ invite: SharedAnalysisInvite }>(`/api/shared-analyses/${inviteId}`),

  accept: (inviteId: string) =>
    api.post<{ invite: SharedAnalysisInvite; acceptedSession: AnalysisSessionSummary }>(
      `/api/shared-analyses/${inviteId}/accept`
    ),

  decline: (inviteId: string) =>
    api.post<{ invite: SharedAnalysisInvite }>(
      `/api/shared-analyses/${inviteId}/decline`
    ),
};


