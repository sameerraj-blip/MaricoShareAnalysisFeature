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

// Utility function to parse multiple emails from space-separated input
const parseEmails = (input: string): string[] => {
  return input
    .split(/\s+/)
    .map(email => email.trim())
    .filter(email => email.length > 0);
};

// Utility function to validate email format
const isValidEmail = (email: string): boolean => {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailRegex.test(email);
};

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
    
    // Parse multiple emails from input
    const emails = parseEmails(targetEmail);
    
    // Validate all emails
    const invalidEmails = emails.filter(email => !isValidEmail(email));
    if (invalidEmails.length > 0) {
      toast({
        title: "Invalid email format",
        description: `Please check these emails: ${invalidEmails.join(', ')}`,
        variant: "destructive",
      });
      return;
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
      // Share with each email address
      const sharePromises = emails.map(email =>
        sharedDashboardsApi.share({
          dashboardId,
          targetEmail: email,
          permission,
          note: note.trim() || undefined,
        })
      );
      
      await Promise.all(sharePromises);
      
      const emailText = emails.length === 1 
        ? emails[0] 
        : `${emails.length} recipients`;
      
      toast({
        title: "Invites sent",
        description: `${dashboardName ?? "Dashboard"} was shared with ${emailText} with ${permission} permission.`,
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
              type="text"
              placeholder="teammate@example.com teammate2@example.com"
              value={targetEmail}
              onChange={(event) => setTargetEmail(event.target.value)}
              disabled={isSubmitting}
              required
            />
            <p className="text-xs text-muted-foreground">
              Enter multiple emails separated by spaces
            </p>
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
