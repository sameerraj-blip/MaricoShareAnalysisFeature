import { useMemo } from "react";
import { SharedDashboardInvite, Dashboard } from "@/shared/schema";
import { useSharedDashboards } from "@/hooks/useSharedDashboards";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Loader2, Mail, RefreshCcw, Share2, Users, Clock, CheckCircle2, BarChart3, Eye, Edit, User } from "lucide-react";

interface SharedDashboardsPanelProps {
  onAccepted?: (data: { invite: SharedDashboardInvite; dashboard: Dashboard }) => void;
  onViewDashboard?: (dashboardId: string, permission: "view" | "edit") => void;
}

const formatTimestamp = (value?: number) => {
  if (!value) return "—";
  const date = new Date(value);
  return `${date.toLocaleDateString()} · ${date.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
  })}`;
};

const PreviewStats = ({ invite }: { invite: SharedDashboardInvite }) => {
  if (!invite.preview) return null;
  const { preview } = invite;
  return (
    <div className="flex items-center gap-4 text-xs text-gray-600">
      <div className="flex items-center gap-1.5">
        <BarChart3 className="h-3.5 w-3.5 text-blue-500" />
        <span className="font-medium">{preview.sheetsCount}</span>
        <span className="text-gray-500">views</span>
      </div>
      <div className="flex items-center gap-1.5">
        <BarChart3 className="h-3.5 w-3.5 text-purple-500" />
        <span className="font-medium">{preview.chartsCount}</span>
        <span className="text-gray-500">charts</span>
      </div>
    </div>
  );
};

export const SharedDashboardsPanel = ({ onAccepted, onViewDashboard }: SharedDashboardsPanelProps) => {
  const {
    pending,
    accepted,
    loading,
    error,
    refresh,
    acceptInvite,
    declineInvite,
    isMutating,
    hasSharedItems,
  } = useSharedDashboards();

  const pendingInvites = useMemo(() => pending.slice(0, 5), [pending]);
  const acceptedInvites = useMemo(() => accepted.slice(0, 5), [accepted]);

  const renderInviteCard = (invite: SharedDashboardInvite) => (
    <div
      key={invite.id}
      className="rounded-xl border border-gray-200 bg-gradient-to-br from-white to-gray-50/50 p-5 flex flex-col gap-4 shadow-sm hover:shadow-md transition-all duration-200"
    >
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-2">
            <div className="h-2 w-2 rounded-full bg-blue-500 animate-pulse"></div>
            <p className="text-base font-semibold text-gray-900 truncate">
              {invite.preview?.name ?? "Shared dashboard"}
            </p>
          </div>
          <div className="flex items-center gap-2 text-xs text-gray-600">
            <User className="h-3.5 w-3.5 text-gray-400" />
            <span className="truncate">{invite.ownerEmail}</span>
          </div>
          <div className="flex items-center gap-2 text-xs text-gray-500 mt-1">
            <Clock className="h-3.5 w-3.5 text-gray-400" />
            <span>{formatTimestamp(invite.createdAt)}</span>
          </div>
        </div>
        <div className="flex flex-col gap-2 items-end">
          <Badge variant="secondary" className="gap-1.5 text-xs px-2.5 py-1 bg-blue-50 text-blue-700 border-blue-200 flex-shrink-0">
            <Mail className="h-3 w-3" />
            <span className="hidden sm:inline">{invite.targetEmail}</span>
          </Badge>
          <Badge 
            variant="outline" 
            className={`text-xs px-2.5 py-1 flex-shrink-0 ${
              invite.permission === "edit" 
                ? "bg-green-50 text-green-700 border-green-200" 
                : "bg-gray-50 text-gray-700 border-gray-200"
            }`}
          >
            {invite.permission === "edit" ? (
              <>
                <Edit className="h-3 w-3 mr-1" />
                Edit
              </>
            ) : (
              <>
                <Eye className="h-3 w-3 mr-1" />
                View
              </>
            )}
          </Badge>
        </div>
      </div>
      
      {invite.note && (
        <div className="bg-blue-50/50 border-l-4 border-blue-400 rounded-r-md p-3">
          <p className="text-sm text-gray-700 italic leading-relaxed">
            "{invite.note}"
          </p>
        </div>
      )}
      
      <div className="pt-2 border-t border-gray-100">
        <PreviewStats invite={invite} />
      </div>
      
      <div className="flex gap-2.5 pt-1">
        <Button
          size="sm"
          className="flex-1 bg-blue-600 hover:bg-blue-700 text-white font-medium shadow-sm"
          disabled={isMutating}
          onClick={async () => {
            const result = await acceptInvite(invite.id);
            if (result && onAccepted) {
              onAccepted(result);
            }
          }}
        >
          Accept & View
        </Button>
        <Button
          size="sm"
          variant="outline"
          className="border-gray-300 text-gray-700 hover:bg-gray-50 font-medium"
          disabled={isMutating}
          onClick={() => declineInvite(invite.id)}
        >
          Decline
        </Button>
      </div>
    </div>
  );

  const renderAcceptedCard = (invite: SharedDashboardInvite) => (
    <div
      key={`accepted-${invite.id}`}
      className="rounded-lg border border-gray-200 bg-gradient-to-br from-green-50/50 to-white p-4 hover:shadow-sm transition-shadow duration-200 cursor-pointer"
      onClick={() => {
        if (onViewDashboard) {
          onViewDashboard(invite.sourceDashboardId, invite.permission);
        }
      }}
    >
      <div className="flex items-center justify-between gap-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1.5">
            <CheckCircle2 className="h-4 w-4 text-green-600 flex-shrink-0" />
            <p className="font-semibold text-gray-900 text-sm truncate">
              {invite.preview?.name ?? "Shared dashboard"}
            </p>
          </div>
          <div className="flex items-center gap-2 text-xs text-gray-600 ml-6">
            <span>Accepted {formatTimestamp(invite.acceptedAt)}</span>
            <span className="text-gray-400">·</span>
            <span className="truncate">from {invite.ownerEmail}</span>
          </div>
        </div>
        <Badge 
          variant="outline" 
          className={`text-xs px-2.5 py-1 flex-shrink-0 ${
            invite.permission === "edit" 
              ? "bg-green-50 text-green-700 border-green-200" 
              : "bg-gray-50 text-gray-700 border-gray-200"
          }`}
        >
          {invite.permission === "edit" ? (
            <>
              <Edit className="h-3 w-3 mr-1" />
              Edit
            </>
          ) : (
            <>
              <Eye className="h-3 w-3 mr-1" />
              View
            </>
          )}
        </Badge>
      </div>
    </div>
  );

  return (
    <Card className="h-full flex flex-col border-gray-200 shadow-sm">
      <CardHeader className="flex flex-row items-center justify-between space-y-0 flex-shrink-0 pb-4 border-b border-gray-100">
        <div className="flex-1">
          <CardTitle className="flex items-center gap-2.5 text-xl font-bold text-gray-900 mb-1.5">
            <div className="p-1.5 rounded-lg bg-blue-100">
              <Share2 className="h-4 w-4 text-blue-600" />
            </div>
            Shared Dashboards
          </CardTitle>
          <CardDescription className="text-sm text-gray-600 ml-9">
            View dashboards that teammates shared with you.
          </CardDescription>
        </div>
        <Button 
          variant="ghost" 
          size="sm" 
          onClick={refresh} 
          disabled={loading}
          className="hover:bg-gray-100 text-gray-700"
        >
          {loading ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <>
              <RefreshCcw className="h-4 w-4 mr-1.5" />
              Refresh
            </>
          )}
        </Button>
      </CardHeader>
      <CardContent className="space-y-6 flex-1 overflow-y-auto pt-6">
        {error && (
          <div className="rounded-md border border-destructive/50 bg-destructive/10 p-3 text-sm text-destructive flex items-center justify-between">
            <span>{error}</span>
            <Button variant="destructive" size="sm" onClick={refresh}>
              Retry
            </Button>
          </div>
        )}

        {loading && (
          <div className="space-y-3">
            <Skeleton className="h-20 w-full" />
            <Skeleton className="h-20 w-full" />
          </div>
        )}

        {!loading && pendingInvites.length === 0 && !error && (
          <div className="rounded-xl border-2 border-dashed border-gray-200 bg-gray-50/50 p-8 text-center">
            <div className="inline-flex items-center justify-center w-12 h-12 rounded-full bg-gray-100 mb-3">
              <Users className="h-6 w-6 text-gray-400" />
            </div>
            <p className="text-sm font-medium text-gray-700 mb-1">
              {hasSharedItems
                ? "You're all caught up!"
                : "No shared dashboards yet"}
            </p>
            <p className="text-xs text-gray-500">
              {hasSharedItems
                ? "Invite cards will show here when teammates share dashboards."
                : "Ask a teammate to share a dashboard with you."}
            </p>
          </div>
        )}

        {pendingInvites.length > 0 && (
          <div className="space-y-4">
            <div className="flex items-center gap-2 pb-2">
              <Clock className="h-4 w-4 text-amber-600" />
              <p className="text-xs font-bold text-gray-700 uppercase tracking-wider">
                Pending Invites
              </p>
              <Badge variant="secondary" className="ml-auto text-xs px-2 py-0.5 bg-amber-100 text-amber-700 border-amber-200">
                {pendingInvites.length}
              </Badge>
            </div>
            <div className="space-y-3">
              {pendingInvites.map(renderInviteCard)}
            </div>
          </div>
        )}

        {acceptedInvites.length > 0 && (
          <div className="space-y-4">
            <div className="flex items-center gap-2 pb-2">
              <CheckCircle2 className="h-4 w-4 text-green-600" />
              <p className="text-xs font-bold text-gray-700 uppercase tracking-wider">
                Accepted Dashboards
              </p>
              <Badge variant="secondary" className="ml-auto text-xs px-2 py-0.5 bg-green-100 text-green-700 border-green-200">
                {acceptedInvites.length}
              </Badge>
            </div>
            <div className="space-y-2.5">
              {acceptedInvites.map(renderAcceptedCard)}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
};
