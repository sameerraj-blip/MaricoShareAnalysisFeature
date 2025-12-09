import { useState } from "react";
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
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { sharedDashboardsApi } from "@/lib/api";
import { useToast } from "@/hooks/use-toast";

interface ShareDashboardDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  dashboardId?: string;
  dashboardName?: string;
}

export const ShareDashboardDialog = ({
  open,
  onOpenChange,
  dashboardId,
  dashboardName,
}: ShareDashboardDialogProps) => {
  const [targetEmail, setTargetEmail] = useState("");
  const [note, setNote] = useState("");
  const [permission, setPermission] = useState<"view" | "edit">("view");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const { toast } = useToast();

  const resetForm = () => {
    setTargetEmail("");
    setNote("");
    setPermission("view");
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
    if (!dashboardId || !targetEmail.trim()) return;
    setIsSubmitting(true);
    try {
      await sharedDashboardsApi.share({
        dashboardId,
        targetEmail: targetEmail.trim(),
        permission,
        note: note.trim() || undefined,
      });
      toast({
        title: "Invite sent",
        description: `${dashboardName ?? "Dashboard"} was shared with ${targetEmail.trim()} with ${permission} permission.`,
      });
      resetForm();
      onOpenChange(false);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to share dashboard.";
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
          <DialogTitle>Share dashboard</DialogTitle>
          <DialogDescription>
            Invite a teammate to access <span className="font-semibold">{dashboardName}</span>. Choose whether they can view or edit the dashboard.
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
          <div className="space-y-3">
            <Label>Permission</Label>
            <RadioGroup value={permission} onValueChange={(value) => setPermission(value as "view" | "edit")}>
              <div className="flex items-center space-x-2">
                <RadioGroupItem value="view" id="view" />
                <Label htmlFor="view" className="font-normal cursor-pointer">
                  View - Can only view the dashboard
                </Label>
              </div>
              <div className="flex items-center space-x-2">
                <RadioGroupItem value="edit" id="edit" />
                <Label htmlFor="edit" className="font-normal cursor-pointer">
                  Edit - Can view and edit the dashboard
                </Label>
              </div>
            </RadioGroup>
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
