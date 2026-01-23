import React, { useRef, useState, useEffect, lazy, Suspense } from 'react';
import { ChartSpec } from '@/shared/schema';
import { InsightRecommendationTile } from './InsightRecommendationTile';
import { EditInsightModal } from './EditInsightModal';
import { Button } from '@/components/ui/button';
import { Trash2, GripVertical } from 'lucide-react';
import Draggable, { DraggableData, DraggableEvent } from 'react-draggable';
import { useToast } from '@/hooks/use-toast';
import { useDashboardContext } from '../context/DashboardContext';
import { Skeleton } from '@/components/ui/skeleton';

// Lazy load ChartRenderer to reduce initial bundle size
const ChartRenderer = lazy(() => import('@/pages/Home/Components/ChartRenderer').then(module => ({ default: module.ChartRenderer })));

interface ChartContainerProps {
  chart: ChartSpec;
  index: number;
  dashboardId: string;
  sheetId?: string;
  onDelete: () => void;
  onUpdate?: () => void;
}

export function ChartContainer({ chart, index, dashboardId, sheetId, onDelete, onUpdate }: ChartContainerProps) {
  const [position, setPosition] = useState<{ x: number; y: number }>({ x: 0, y: 0 });
  const [isDragging, setIsDragging] = useState(false);
  const [isEditingInsight, setIsEditingInsight] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const nodeRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const { toast } = useToast();
  const { updateChartInsightOrRecommendation } = useDashboardContext();

  // Load saved position from localStorage, or use default stacked position
  useEffect(() => {
    const storageKey = `dashboard-container-pos:${dashboardId}:${index}`;
    const saved = localStorage.getItem(storageKey);
    if (saved) {
      try {
        const parsed = JSON.parse(saved);
        if (typeof parsed.x === 'number' && typeof parsed.y === 'number') {
          setPosition({ x: parsed.x, y: parsed.y });
          return;
        }
      } catch {}
    }
    // Default position: stack vertically with gap
    const defaultY = index * 650; // 600px height + 50px gap
    setPosition({ x: 0, y: defaultY });
  }, [dashboardId, index]);

  // Calculate bounds - container should only move within the dashboard view area
  const getBounds = () => {
    // Find the parent container that holds all chart containers (the div with position: relative)
    const parentContainer = nodeRef.current?.parentElement;
    if (parentContainer) {
      return parentContainer as HTMLElement;
    }
    // Fallback to dashboard view container
    const dashboardView = nodeRef.current?.closest('.dashboard-view-container');
    if (dashboardView) {
      return dashboardView as HTMLElement;
    }
    return 'parent' as const;
  };

  const onDragStart = () => {
    setIsDragging(true);
  };

  const onDrag = (_e: DraggableEvent, data: DraggableData) => {
    setPosition({ x: data.x, y: data.y });
  };

  const onDragStop = (_e: DraggableEvent, data: DraggableData) => {
    setIsDragging(false);
    setPosition({ x: data.x, y: data.y });
    
    // Save position to localStorage
    const storageKey = `dashboard-container-pos:${dashboardId}:${index}`;
    localStorage.setItem(storageKey, JSON.stringify({ x: data.x, y: data.y }));
  };

  const handleSaveInsight = async (text: string) => {
    setIsSaving(true);
    try {
      await updateChartInsightOrRecommendation(
        dashboardId,
        index,
        { keyInsight: text },
        sheetId
      );
      setIsEditingInsight(false);
      toast({
        title: 'Success',
        description: 'Key insight updated successfully.',
      });
      // Refetch dashboards to get the updated data
      if (onUpdate) {
        await onUpdate();
      }
    } catch (error: any) {
      toast({
        title: 'Error',
        description: error?.message || 'Failed to update insight',
        variant: 'destructive',
      });
    } finally {
      setIsSaving(false);
    }
  };


  return (
    <Draggable
      handle=".container-drag-handle"
      bounds={getBounds() as any}
      position={position}
      onStart={onDragStart}
      onDrag={onDrag}
      onStop={onDragStop}
      nodeRef={nodeRef}
    >
      <div
        ref={nodeRef}
        className="chart-container-wrapper"
        style={{
          position: 'absolute',
          zIndex: isDragging ? 50 : 'auto',
          width: 'calc(100% - 48px)', // Account for padding
          maxWidth: '1200px',
          cursor: isDragging ? 'grabbing' : 'default',
        }}
      >
        <div
          ref={containerRef}
          className="rounded-lg border-2 border-border bg-white shadow-lg hover:shadow-xl transition-shadow"
          style={{
            display: 'flex',
            flexDirection: 'column',
            height: '600px', // Fixed height for consistent layout
            minHeight: '600px',
            width: '100%',
            position: 'relative',
          }}
        >
          {/* Drag Handle */}
          <div
            className="container-drag-handle absolute left-0 top-0 w-full h-10 flex items-center gap-2 pl-3 text-muted-foreground cursor-grab active:cursor-grabbing bg-gradient-to-b from-muted/70 to-transparent z-20 rounded-t-lg"
            aria-label="Drag container"
          >
            <GripVertical className="h-4 w-4" />
            <span className="text-xs font-medium">Drag Container</span>
            <div className="ml-auto pr-3">
              <Button
                variant="ghost"
                size="icon"
                className="h-7 w-7 hover:bg-destructive/10 hover:text-destructive"
                onClick={(e) => {
                  e.stopPropagation();
                  onDelete();
                }}
              >
                <Trash2 className="h-4 w-4" />
              </Button>
            </div>
          </div>

          {/* Content area - accounts for drag handle */}
          <div
            className="flex flex-col"
            style={{
              marginTop: '40px', // Space for drag handle
              height: 'calc(100% - 40px)',
              flex: 1,
            }}
          >
            {/* Chart Section - 55% of content area */}
            <div
              className="flex-shrink-0 border-b border-border"
              style={{
                flexBasis: '55%',
                minHeight: '55%',
                padding: '16px',
                overflow: 'hidden',
              }}
              data-chart-index={index}
            >
              <div className="h-full w-full">
                <Suspense fallback={<Skeleton className="h-full w-full" />}>
                  <ChartRenderer
                    chart={chart}
                    index={index}
                    isSingleChart={false}
                    showAddButton={false}
                    useChartOnlyModal
                    fillParent
                    enableFilters
                  />
                </Suspense>
              </div>
            </div>

            {/* Key Insights Section - 25% of content area */}
            {chart.keyInsight && (
              <div
                className="flex-shrink-0 border-b border-border"
                style={{
                  flexBasis: '25%',
                  minHeight: '25%',
                  padding: '16px',
                  overflow: 'hidden',
                }}
              >
                <InsightRecommendationTile 
                  variant="insight" 
                  text={chart.keyInsight} 
                  onEdit={() => setIsEditingInsight(true)}
                />
              </div>
            )}

            {/* If no insights, expand chart to fill remaining space */}
            {!chart.keyInsight && (
              <div
                className="flex-shrink-0"
                style={{
                  flexBasis: '45%',
                  minHeight: '45%',
                }}
              />
            )}
          </div>
        </div>
      </div>
      {chart.keyInsight && (
        <EditInsightModal
          isOpen={isEditingInsight}
          onClose={() => setIsEditingInsight(false)}
          onSave={handleSaveInsight}
          title="Key Insight"
          initialText={chart.keyInsight}
          isLoading={isSaving}
        />
      )}
    </Draggable>
  );
}

