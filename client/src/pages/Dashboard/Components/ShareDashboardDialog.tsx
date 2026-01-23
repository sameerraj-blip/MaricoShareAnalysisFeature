import { useState, KeyboardEvent } from "react";
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
import { Badge } from "@/components/ui/badge";
import { X } from "lucide-react";
import { sharedDashboardsApi } from "@/lib/api";
import { useToast } from "@/hooks/use-toast";

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
  const [emails, setEmails] = useState<string[]>([]);
  const [emailInput, setEmailInput] = useState("");
  const [note, setNote] = useState("");
  const [permission, setPermission] = useState<"view" | "edit">("view");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const { toast } = useToast();

  const resetForm = () => {
    setEmails([]);
    setEmailInput("");
    setNote("");
    setPermission("view");
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

  const handleClose = (next: boolean) => {
    if (!isSubmitting) {
      if (!next) {
        resetForm();
      }
      onOpenChange(next);
    }
  };

  const handleShare = async () => {
    if (!dashboardId) return;
    
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
          <Button onClick={handleShare} disabled={emails.length === 0 || isSubmitting}>
            {isSubmitting ? "Sending..." : "Send invite"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};
