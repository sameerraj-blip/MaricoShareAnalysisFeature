import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Checkbox } from "@/components/ui/checkbox";
import { sharedAnalysesApi, dashboardsApi } from "@/lib/api";
import { useToast } from "@/hooks/use-toast";
import { getUserEmail } from "@/utils/userStorage";
import { X } from "lucide-react";

interface ShareAnalysisDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  sessionId?: string;
  fileName?: string;
}

export const ShareAnalysisDialog = ({
  open,
  onOpenChange,
  sessionId,
  fileName,
}: ShareAnalysisDialogProps) => {
  const [targetEmail, setTargetEmail] = useState("");
  const [note, setNote] = useState("");
  const [selectedDashboardId, setSelectedDashboardId] = useState<string | undefined>(undefined);
  const [dashboardEditable, setDashboardEditable] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const { toast } = useToast();
  const userEmail = getUserEmail();

  // Fetch user dashboards
  const { data: dashboardsData, isLoading: isLoadingDashboards } = useQuery({
    queryKey: ['dashboards', 'list', userEmail],
    queryFn: async () => {
      const res = await dashboardsApi.list();
      // Show all dashboards that are NOT explicitly shared with the user
      // The API already filters by user, so dashboards without isShared=true are owned
      const ownedDashboards = res.dashboards.filter(d => d.isShared !== true);
      
      console.log('[ShareAnalysisDialog] Total dashboards from API:', res.dashboards.length);
      console.log('[ShareAnalysisDialog] Owned dashboards (isShared !== true):', ownedDashboards.length);
      console.log('[ShareAnalysisDialog] User email:', userEmail);
      console.log('[ShareAnalysisDialog] All dashboards:', res.dashboards.map(d => ({ 
        id: d.id, 
        name: d.name, 
        isShared: d.isShared, 
        username: d.username,
        sharedBy: d.sharedBy
      })));
      
      return { dashboards: ownedDashboards };
    },
    enabled: open && !!userEmail,
  });

  const resetForm = () => {
    setTargetEmail("");
    setNote("");
    setSelectedDashboardId(undefined);
    setDashboardEditable(false);
  };

  const handleClose = (next: boolean) => {
    if (!isSubmitting) {
      if (!next) {
        resetForm();
      }
      onOpenChange(next);
    }
  };

  const handleShare = async () => {
    if (!sessionId || !targetEmail.trim()) return;
    setIsSubmitting(true);
    try {
      await sharedAnalysesApi.share({
        sessionId,
        targetEmail: targetEmail.trim(),
        note: note.trim() || undefined,
        dashboardId: selectedDashboardId || undefined,
        isEditable: selectedDashboardId ? dashboardEditable : undefined,
      });
      const dashboardText = selectedDashboardId 
        ? ` and dashboard "${(dashboardsData?.dashboards || []).find(d => d.id === selectedDashboardId)?.name || 'selected dashboard'}"`
        : "";
      toast({
        title: "Invite sent",
        description: `${fileName ?? "Analysis"}${dashboardText} was shared with ${targetEmail.trim()}.`,
      });
      resetForm();
      onOpenChange(false);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to share analysis.";
      toast({
        title: "Share failed",
        description: message,
        variant: "destructive",
      });
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Share analysis</DialogTitle>
          <DialogDescription>
            Invite a teammate to co-own <span className="font-semibold">{fileName}</span>. Once they accept, you’ll both work
            inside the same live analysis.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-2">
          <div className="space-y-2">
            <Label htmlFor="share-email">Recipient email</Label>
            <Input
              id="share-email"
              type="email"
              placeholder="teammate@example.com"
              value={targetEmail}
              onChange={(event) => setTargetEmail(event.target.value)}
              disabled={isSubmitting}
              required
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="share-note">Message (optional)</Label>
            <Textarea
              id="share-note"
              placeholder="Add context for your teammate..."
              value={note}
              onChange={(event) => setNote(event.target.value)}
              disabled={isSubmitting}
              rows={3}
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="share-dashboard">Share dashboard (optional)</Label>
            <div className="flex gap-2">
              <Select
                value={selectedDashboardId || ""}
                onValueChange={(value) => {
                  setSelectedDashboardId(value);
                }}
                disabled={isSubmitting}
              >
                <SelectTrigger id="share-dashboard" className="flex-1">
                  <SelectValue placeholder="Select a dashboard to share (optional)" />
                </SelectTrigger>
                <SelectContent>
                  {isLoadingDashboards ? (
                    <div className="px-2 py-1.5 text-sm text-muted-foreground">Loading...</div>
                  ) : (dashboardsData?.dashboards || []).length === 0 ? (
                    <div className="px-2 py-1.5 text-sm text-muted-foreground">No dashboards available</div>
                  ) : (
                    (dashboardsData?.dashboards || []).map((dashboard) => (
                      <SelectItem key={dashboard.id} value={dashboard.id}>
                        {dashboard.name}
                      </SelectItem>
                    ))
                  )}
                </SelectContent>
              </Select>
              {selectedDashboardId && (
                <Button
                  type="button"
                  variant="outline"
                  size="icon"
                  onClick={() => {
                    setSelectedDashboardId(undefined);
                    setDashboardEditable(false);
                  }}
                  disabled={isSubmitting}
                  className="flex-shrink-0"
                >
                  <X className="h-4 w-4" />
                </Button>
              )}
            </div>
          </div>
          {selectedDashboardId && (
            <div className="flex items-center space-x-2">
              <Checkbox
                id="dashboard-editable"
                checked={dashboardEditable}
                onCheckedChange={(checked) => setDashboardEditable(checked === true)}
                disabled={isSubmitting}
              />
              <Label
                htmlFor="dashboard-editable"
                className="text-sm font-normal cursor-pointer"
              >
                Allow editing of dashboard
              </Label>
            </div>
          )}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => handleClose(false)} disabled={isSubmitting}>
            Cancel
          </Button>
          <Button onClick={handleShare} disabled={!targetEmail.trim() || isSubmitting}>
            {isSubmitting ? "Sending..." : "Send invite"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

