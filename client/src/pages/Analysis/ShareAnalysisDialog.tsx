import { useState, KeyboardEvent } from "react";
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
import { Checkbox } from "@/components/ui/checkbox";
import { Badge } from "@/components/ui/badge";
import { X } from "lucide-react";
import { sharedAnalysesApi, dashboardsApi } from "@/lib/api";
import { useToast } from "@/hooks/use-toast";
import { getUserEmail } from "@/utils/userStorage";
import { ScrollArea } from "@/components/ui/scroll-area";
import { cn } from "@/lib/utils";
import { logger } from "@/lib/logger";

// Utility function to validate email format
const isValidEmail = (email: string): boolean => {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailRegex.test(email);
};

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
  const [emails, setEmails] = useState<string[]>([]);
  const [emailInput, setEmailInput] = useState("");
  const [note, setNote] = useState("");
  // Map of dashboardId -> { editable: boolean }
  const [selectedDashboards, setSelectedDashboards] = useState<Record<string, { editable: boolean }>>({});
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
      
      logger.log('[ShareAnalysisDialog] Total dashboards from API:', res.dashboards.length);
      logger.log('[ShareAnalysisDialog] Owned dashboards (isShared !== true):', ownedDashboards.length);
      logger.log('[ShareAnalysisDialog] User email:', userEmail);
      logger.log('[ShareAnalysisDialog] All dashboards:', res.dashboards.map(d => ({ 
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
    setEmails([]);
    setEmailInput("");
    setNote("");
    setSelectedDashboards({});
  };

  const addEmail = (email: string) => {
    const trimmedEmail = email.trim();
    if (!trimmedEmail) return;
    
    if (!isValidEmail(trimmedEmail)) {
      toast({
        title: "Invalid email format",
        description: `"${trimmedEmail}" is not a valid email address.`,
        variant: "destructive",
      });
      return;
    }
    
    if (emails.includes(trimmedEmail)) {
      toast({
        title: "Duplicate email",
        description: `"${trimmedEmail}" has already been added.`,
        variant: "destructive",
      });
      return;
    }
    
    setEmails([...emails, trimmedEmail]);
    setEmailInput("");
  };

  const removeEmail = (emailToRemove: string) => {
    setEmails(emails.filter(email => email !== emailToRemove));
  };

  const handleEmailInputKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter" || e.key === ",") {
      e.preventDefault();
      if (emailInput.trim()) {
        addEmail(emailInput);
      }
    } else if (e.key === "Backspace" && !emailInput && emails.length > 0) {
      // Remove last email when backspace is pressed on empty input
      removeEmail(emails[emails.length - 1]);
    }
  };

  const handleEmailInputBlur = () => {
    if (emailInput.trim()) {
      addEmail(emailInput);
    }
  };

  const handleDashboardToggle = (dashboardId: string, checked: boolean) => {
    if (checked) {
      setSelectedDashboards(prev => ({
        ...prev,
        [dashboardId]: { editable: false }
      }));
    } else {
      setSelectedDashboards(prev => {
        const updated = { ...prev };
        delete updated[dashboardId];
        return updated;
      });
    }
  };

  const handleDashboardEditableToggle = (dashboardId: string, editable: boolean) => {
    setSelectedDashboards(prev => ({
      ...prev,
      [dashboardId]: { editable }
    }));
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
    if (!sessionId) return;
    
    // Add any remaining email in the input field
    if (emailInput.trim()) {
      addEmail(emailInput);
    }
    
    if (emails.length === 0) {
      toast({
        title: "No emails provided",
        description: "Please enter at least one email address.",
        variant: "destructive",
      });
      return;
    }
    
    setIsSubmitting(true);
    try {
      const dashboardIds = Object.keys(selectedDashboards);
      const dashboardPermissions = dashboardIds.length > 0 
        ? Object.fromEntries(
            dashboardIds.map(id => [id, selectedDashboards[id].editable ? 'edit' : 'view'])
          )
        : undefined;
      
      // Share with each email address
      const sharePromises = emails.map(email =>
        sharedAnalysesApi.share({
          sessionId,
          targetEmail: email,
          note: note.trim() || undefined,
          dashboardIds: dashboardIds.length > 0 ? dashboardIds : undefined,
          dashboardPermissions,
        })
      );
      
      await Promise.all(sharePromises);
      
      const dashboardCount = dashboardIds.length;
      const dashboardText = dashboardCount > 0 
        ? ` and ${dashboardCount} dashboard${dashboardCount > 1 ? 's' : ''}`
        : "";
      const emailText = emails.length === 1 
        ? emails[0] 
        : `${emails.length} recipients`;
      
      toast({
        title: "Invites sent",
        description: `${fileName ?? "Analysis"}${dashboardText} was shared with ${emailText}.`,
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
            <Label htmlFor="share-email">Recipient emails</Label>
            <div className="min-h-[42px] w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-within:ring-2 focus-within:ring-ring focus-within:ring-offset-2">
              <div className="flex flex-wrap gap-2">
                {emails.map((email) => (
                  <Badge
                    key={email}
                    variant="secondary"
                    className="flex items-center gap-1.5 pr-1 py-1"
                  >
                    <span className="text-xs">{email}</span>
                    <button
                      type="button"
                      onClick={() => removeEmail(email)}
                      disabled={isSubmitting}
                      className="rounded-full hover:bg-secondary-foreground/20 p-0.5 focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-1 disabled:opacity-50 disabled:cursor-not-allowed"
                      aria-label={`Remove ${email}`}
                    >
                      <X className="h-3 w-3" />
                    </button>
                  </Badge>
                ))}
                <Input
                  id="share-email"
                  type="text"
                  placeholder={emails.length === 0 ? "Enter email addresses..." : ""}
                  value={emailInput}
                  onChange={(e) => setEmailInput(e.target.value)}
                  onKeyDown={handleEmailInputKeyDown}
                  onBlur={handleEmailInputBlur}
                  disabled={isSubmitting}
                  className="flex-1 min-w-[200px] border-0 p-0 focus-visible:ring-0 focus-visible:ring-offset-0 h-7"
                />
              </div>
            </div>
            <p className="text-xs text-muted-foreground">
              Press Enter or comma to add emails. Click × to remove.
            </p>
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
            <Label>Share dashboards (optional)</Label>
            {isLoadingDashboards ? (
              <div className="px-2 py-1.5 text-sm text-muted-foreground">Loading dashboards...</div>
            ) : (dashboardsData?.dashboards || []).length === 0 ? (
              <div className="px-2 py-1.5 text-sm text-muted-foreground">No dashboards available</div>
            ) : (
              <ScrollArea className="h-48 w-full rounded-md border p-4">
                <div className="space-y-3">
                  {(dashboardsData?.dashboards || []).map((dashboard) => {
                    const isSelected = selectedDashboards[dashboard.id] !== undefined;
                    const isEditable = selectedDashboards[dashboard.id]?.editable || false;
                    return (
                      <div key={dashboard.id} className="space-y-2">
                        <div className="flex items-center space-x-2">
                          <Checkbox
                            id={`dashboard-${dashboard.id}`}
                            checked={isSelected}
                            onCheckedChange={(checked) => handleDashboardToggle(dashboard.id, checked === true)}
                            disabled={isSubmitting}
                          />
                          <Label
                            htmlFor={`dashboard-${dashboard.id}`}
                            className="text-sm font-medium leading-none cursor-pointer flex-1"
                          >
                            {dashboard.name}
                          </Label>
                        </div>
                        {isSelected && (
                          <div className="flex items-center space-x-2 ml-6">
                            <Checkbox
                              id={`dashboard-editable-${dashboard.id}`}
                              checked={isEditable}
                              onCheckedChange={(checked) => handleDashboardEditableToggle(dashboard.id, checked === true)}
                              disabled={isSubmitting}
                            />
                            <Label
                              htmlFor={`dashboard-editable-${dashboard.id}`}
                              className="text-sm font-normal cursor-pointer"
                            >
                              Allow editing
                            </Label>
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              </ScrollArea>
            )}
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => handleClose(false)} disabled={isSubmitting}>
            Cancel
          </Button>
          <Button onClick={handleShare} disabled={emails.length === 0 || isSubmitting}>
            {isSubmitting ? "Sending..." : "Send invite"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

