import { useState } from 'react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { Info } from 'lucide-react';

interface ContextModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (context: string) => Promise<void>;
  isLoading?: boolean;
}

export function ContextModal({ isOpen, onClose, onSave, isLoading = false }: ContextModalProps) {
  const [context, setContext] = useState('');

  const handleSave = async () => {
    if (context.trim()) {
      await onSave(context.trim());
    } else {
      // If empty, just close without saving
      onClose();
    }
  };

  const handleCancel = () => {
    setContext('');
    onClose();
  };

  return (
    <Dialog open={isOpen} onOpenChange={(open) => !open && handleCancel()}>
      <DialogContent className="sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Info className="h-5 w-5" />
            Add Context for Your Data
          </DialogTitle>
          <DialogDescription>
            Provide any additional context about your data that will help the AI better understand and analyze it. 
            This context will be included with every message you send. You can skip this step if you prefer.
          </DialogDescription>
        </DialogHeader>
        
        <div className="space-y-4 py-4">
          <Textarea
            placeholder="E.g., This data represents sales figures for Q4 2023. Focus on identifying trends and anomalies..."
            value={context}
            onChange={(e) => setContext(e.target.value)}
            className="min-h-[120px] resize-none"
            disabled={isLoading}
          />
          <p className="text-sm text-muted-foreground">
            This context will be permanently associated with this session and sent to the AI with each of your messages.
          </p>
        </div>

        <DialogFooter>
          <Button
            variant="outline"
            onClick={handleCancel}
            disabled={isLoading}
          >
            Skip
          </Button>
          <Button
            onClick={handleSave}
            disabled={isLoading}
          >
            {isLoading ? 'Saving...' : 'Save Context'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
