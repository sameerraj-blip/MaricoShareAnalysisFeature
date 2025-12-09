import { api } from "@/lib/httpClient";
import {
  SharedDashboardsResponse,
  SharedDashboardInvite,
  Dashboard,
} from "@/shared/schema";

export const sharedDashboardsApi = {
  share: (payload: { dashboardId: string; targetEmail: string; permission: "view" | "edit"; note?: string }) =>
    api.post<{ invite: SharedDashboardInvite }>("/api/shared-dashboards", payload),

  getIncoming: () => api.get<SharedDashboardsResponse>("/api/shared-dashboards/incoming"),

  getSent: () =>
    api.get<{ invitations: SharedDashboardInvite[] }>("/api/shared-dashboards/sent"),

  getInvite: (inviteId: string) =>
    api.get<{ invite: SharedDashboardInvite }>(`/api/shared-dashboards/${inviteId}`),

  accept: (inviteId: string) =>
    api.post<{ invite: SharedDashboardInvite; dashboard: Dashboard }>(
      `/api/shared-dashboards/${inviteId}/accept`
    ),

  decline: (inviteId: string) =>
    api.post<{ invite: SharedDashboardInvite }>(
      `/api/shared-dashboards/${inviteId}/decline`
    ),
};
