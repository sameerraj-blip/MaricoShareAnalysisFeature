import React, { useState, useRef, useEffect } from 'react';
import { ArrowLeft, BarChart3, Calendar, Download, Loader2, Edit2, Check, X, Share2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';

interface DashboardHeaderProps {
  name: string;
  lastOpenedAt?: Date;
  updatedAt: Date; // Use updatedAt as fallback
  sheetCount: number;
  isExporting: boolean;
  onBack: () => void;
  onExport: () => void;
  onRename?: (newName: string) => Promise<void>;
  onShare?: () => void;
}

export const DashboardHeader: React.FC<DashboardHeaderProps> = ({
  name,
  lastOpenedAt,
  updatedAt,
  sheetCount,
  isExporting,
  onBack,
  onExport,
  onRename,
  onShare,
}) => {
  const [isEditing, setIsEditing] = useState(false);
  const [editName, setEditName] = useState(name);
  const [isRenaming, setIsRenaming] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    setEditName(name);
  }, [name]);

  useEffect(() => {
    if (isEditing && inputRef.current) {
      inputRef.current.focus();
      inputRef.current.select();
    }
  }, [isEditing]);

  const handleSave = async () => {
    if (!onRename || !editName.trim() || editName.trim() === name) {
      setIsEditing(false);
      setEditName(name);
      return;
    }

    setIsRenaming(true);
    try {
      await onRename(editName.trim());
      setIsEditing(false);
    } catch (error: any) {
      // Error will be shown via toast from the mutation
      setEditName(name); // Reset on error
    } finally {
      setIsRenaming(false);
    }
  };

  const handleCancel = () => {
    setIsEditing(false);
    setEditName(name);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSave();
    } else if (e.key === 'Escape') {
      handleCancel();
    }
  };

  return (
    <div className="flex flex-wrap items-center gap-4">
      <Button variant="ghost" size="icon" onClick={onBack} aria-label="Back to dashboards">
        <ArrowLeft className="h-4 w-4" />
      </Button>

      <div className="flex-1 min-w-[200px]">
        {isEditing ? (
          <div className="flex items-center gap-2">
            <Input
              ref={inputRef}
              value={editName}
              onChange={(e) => setEditName(e.target.value)}
              onKeyDown={handleKeyDown}
              disabled={isRenaming}
              className="text-2xl font-semibold h-9"
            />
            <Button
              variant="ghost"
              size="icon"
              onClick={handleSave}
              disabled={isRenaming || !editName.trim()}
              className="h-8 w-8"
            >
              <Check className="h-4 w-4" />
            </Button>
            <Button
              variant="ghost"
              size="icon"
              onClick={handleCancel}
              disabled={isRenaming}
              className="h-8 w-8"
            >
              <X className="h-4 w-4" />
            </Button>
          </div>
        ) : (
          <div className="flex items-center gap-2 group">
            <h1 className="text-2xl font-semibold text-foreground">{name}</h1>
            {onRename && (
              <Button
                variant="ghost"
                size="icon"
                onClick={() => setIsEditing(true)}
                className="h-6 w-6 opacity-0 group-hover:opacity-100 transition-opacity"
                aria-label="Rename dashboard"
              >
                <Edit2 className="h-3 w-3" />
              </Button>
            )}
          </div>
        )}
        <div className="flex flex-wrap items-center gap-4 text-sm text-muted-foreground mt-1">
          <span className="inline-flex items-center gap-1">
            <Calendar className="h-4 w-4" />
            Last updated {lastOpenedAt 
              ? `${lastOpenedAt.toLocaleDateString()} ${lastOpenedAt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`
              : `${updatedAt.toLocaleDateString()} ${updatedAt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`}
          </span>
          <span className="inline-flex items-center gap-1">
            <BarChart3 className="h-4 w-4" />
            {sheetCount} view{sheetCount === 1 ? '' : 's'}
          </span>
        </div>
      </div>

      <div className="flex items-center gap-2 ml-auto">
        {onShare && (
          <Button
            variant="outline"
            onClick={onShare}
            className="text-emerald-600 hover:text-emerald-700 hover:bg-emerald-50"
          >
            <Share2 className="h-4 w-4 mr-2" />
            Share
          </Button>
        )}
        <Button onClick={onExport} disabled={isExporting}>
          {isExporting ? (
            <>
              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              Exporting…
            </>
          ) : (
            <>
              <Download className="h-4 w-4 mr-2" />
              Export PPT
            </>
          )}
        </Button>
      </div>
    </div>
  );
};

