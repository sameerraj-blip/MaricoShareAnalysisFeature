import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  SharedDashboardsResponse,
  SharedDashboardInvite,
  Dashboard,
} from "@/shared/schema";
import { sharedDashboardsApi } from "@/lib/api";
import { useToast } from "@/hooks/use-toast";
import { getUserEmail } from "@/utils/userStorage";
import { API_BASE_URL } from "@/lib/config";
import { useEventStream } from "@/hooks/useEventStream";

interface SharedDashboardsHookState {
  pending: SharedDashboardInvite[];
  accepted: SharedDashboardInvite[];
  loading: boolean;
  error: string | null;
}

const initialState: SharedDashboardsHookState = {
  pending: [],
  accepted: [],
  loading: true,
  error: null,
};

export const useSharedDashboards = () => {
  const [{ pending, accepted, loading, error }, setState] =
    useState<SharedDashboardsHookState>(initialState);
  const [isMutating, setIsMutating] = useState(false);
  const { toast } = useToast();
  const fallbackIntervalRef = useRef<NodeJS.Timeout | null>(null);
  const [userEmail, setUserEmail] = useState<string | null>(null);

  const loadSharedDashboards = useCallback(async () => {
    setState((prev) => ({ ...prev, loading: true, error: null }));
    try {
      const response = await sharedDashboardsApi.getIncoming();
      setState({
        pending: response.pending,
        accepted: response.accepted,
        loading: false,
        error: null,
      });
    } catch (err) {
      console.error("Failed to load shared dashboards:", err);
      setState((prev) => ({
        ...prev,
        loading: false,
        error: err instanceof Error ? err.message : "Unable to fetch shared dashboards.",
      }));
    }
  }, []);

  const stopFallbackPolling = useCallback(() => {
    if (fallbackIntervalRef.current) {
      clearInterval(fallbackIntervalRef.current);
      fallbackIntervalRef.current = null;
    }
  }, []);

  const startFallbackPolling = useCallback(() => {
    console.error("❌ Max reconnect attempts reached, falling back to polling");
    setState((prev) => ({
      ...prev,
      error: "Connection lost. Falling back to polling.",
    }));
    loadSharedDashboards();
    stopFallbackPolling();
    fallbackIntervalRef.current = setInterval(loadSharedDashboards, 10000);
  }, [loadSharedDashboards, stopFallbackPolling]);

  useEffect(() => {
    const email = getUserEmail();
    if (!email) {
      setState((prev) => ({
        ...prev,
        loading: false,
        error: "User email not found",
      }));
      return;
    }
    setUserEmail(email);
    return () => {
      stopFallbackPolling();
    };
  }, [stopFallbackPolling]);

  useEffect(() => {
    if (userEmail) {
      loadSharedDashboards();
    }
  }, [loadSharedDashboards, userEmail]);

  const sseUrl = useMemo(() => {
    if (!userEmail) {
      return null;
    }
    const params = new URLSearchParams({
      username: userEmail,
    });
    return `${API_BASE_URL}/api/shared-dashboards/incoming/stream?${params.toString()}`;
  }, [userEmail]);

  const handleUpdateEvent = useCallback((event: MessageEvent) => {
    try {
      const data = JSON.parse(event.data) as SharedDashboardsResponse;
      console.log("📥 Shared dashboards update received:", data);
      setState({
        pending: data.pending,
        accepted: data.accepted,
        loading: false,
        error: null,
      });
    } catch (err) {
      console.error("Failed to parse SSE update data:", err);
      setState((prev) => ({
        ...prev,
        error: "Failed to parse update data",
      }));
    }
  }, []);

  const handleMessageEvent = useCallback((event: MessageEvent) => {
    console.log("📨 SSE message received:", event.data);
  }, []);

  const streamHandlers = useMemo(
    () => ({
      update: handleUpdateEvent,
      message: handleMessageEvent,
    }),
    [handleMessageEvent, handleUpdateEvent]
  );

  const handleStreamError = useCallback((event: Event) => {
    console.error("❌ SSE connection error:", event);
    setState((prev) => ({
      ...prev,
      error: "Connection error",
    }));
  }, []);

  const handleStreamOpen = useCallback(() => {
    console.log("✅ SSE connection opened for shared dashboards");
    stopFallbackPolling();
    setState((prev) => ({ ...prev, loading: false, error: null }));
  }, [stopFallbackPolling]);

  useEventStream({
    url: sseUrl,
    eventHandlers: streamHandlers,
    onOpen: handleStreamOpen,
    onError: handleStreamError,
    onFallback: startFallbackPolling,
  });

  const acceptInvite = useCallback(
    async (inviteId: string): Promise<{ invite: SharedDashboardInvite; dashboard: Dashboard } | null> => {
      setIsMutating(true);
      try {
        const { invite, dashboard } = await sharedDashboardsApi.accept(inviteId);
        setState((prev) => ({
          ...prev,
          pending: prev.pending.filter((item) => item.id !== inviteId),
          accepted: [invite, ...prev.accepted.filter((item) => item.id !== inviteId)],
        }));
        toast({
          title: "Dashboard access granted",
          description: "You can now view this shared dashboard.",
        });
        return { invite, dashboard };
      } catch (err) {
        const message =
          err instanceof Error ? err.message : "Unable to accept shared dashboard.";
        toast({
          title: "Failed to accept dashboard",
          description: message,
          variant: "destructive",
        });
        throw err;
      } finally {
        setIsMutating(false);
      }
    },
    [toast]
  );

  const declineInvite = useCallback(
    async (inviteId: string) => {
      setIsMutating(true);
      try {
        await sharedDashboardsApi.decline(inviteId);
        setState((prev) => ({
          ...prev,
          pending: prev.pending.filter((item) => item.id !== inviteId),
        }));
        toast({
          title: "Invite declined",
          description: "The shared dashboard invite has been removed.",
        });
      } catch (err) {
        const message =
          err instanceof Error ? err.message : "Unable to decline shared dashboard.";
        toast({
          title: "Failed to decline",
          description: message,
          variant: "destructive",
        });
        throw err;
      } finally {
        setIsMutating(false);
      }
    },
    [toast]
  );

  const shareDashboard = useCallback(
    async (payload: { dashboardId: string; targetEmail: string; permission: "view" | "edit"; note?: string }) => {
      setIsMutating(true);
      try {
        const result = await sharedDashboardsApi.share(payload);
        toast({
          title: "Dashboard shared",
          description: `Invite sent to ${payload.targetEmail}.`,
        });
        return result.invite;
      } catch (err) {
        const message =
          err instanceof Error ? err.message : "Unable to share this dashboard.";
        toast({
          title: "Share failed",
          description: message,
          variant: "destructive",
        });
        throw err;
      } finally {
        setIsMutating(false);
      }
    },
    [toast]
  );

  return {
    pending,
    accepted,
    loading,
    error,
    isMutating,
    refresh: loadSharedDashboards,
    acceptInvite,
    declineInvite,
    shareDashboard,
    hasSharedItems: useMemo(() => pending.length + accepted.length > 0, [pending, accepted]),
  };
};
