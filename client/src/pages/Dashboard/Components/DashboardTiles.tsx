import React, { useCallback, useEffect, useMemo, useState, lazy, Suspense } from 'react';
import { DashboardTile } from '@/pages/Dashboard/types';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Trash2, Edit2, Loader2 } from 'lucide-react';
import { Responsive, WidthProvider, Layout, Layouts } from 'react-grid-layout';
import 'react-grid-layout/css/styles.css';
import 'react-resizable/css/styles.css';
import { EditInsightModal } from './EditInsightModal';
import { useToast } from '@/hooks/use-toast';
import { useDashboardContext } from '../context/DashboardContext';
import { Skeleton } from '@/components/ui/skeleton';

// Lazy load ChartRenderer to reduce initial bundle size
const ChartRenderer = lazy(() => import('@/pages/Home/Components/ChartRenderer').then(module => ({ default: module.ChartRenderer })));

const ResponsiveGridLayout = WidthProvider(Responsive);

import { ActiveChartFilters } from '@/lib/chartFilters';

interface DashboardTilesProps {
  dashboardId: string;
  tiles: DashboardTile[];
  onDeleteChart: (chartIndex: number) => void;
  filtersByTile: Record<string, ActiveChartFilters>;
  onTileFiltersChange: (tileId: string, filters: ActiveChartFilters) => void;
  sheetId?: string;
  onUpdate?: () => void;
  canEdit?: boolean; // Whether the user can edit this dashboard
}

const COLS = { lg: 12, md: 10, sm: 6, xs: 4, xxs: 2 } as const;
const ROW_HEIGHT = 32;
const GRID_MARGIN: [number, number] = [24, 24];
const STORAGE_PREFIX = 'dashboard-grid-layout:';
const HIDDEN_TILE_PREFIX = 'dashboard-hidden-tiles:';

type TileConfig = {
  w: number;
  h: number;
  minW: number;
  minH: number;
};

const TILE_CONFIG: Record<DashboardTile['kind'], TileConfig> = {
  chart: { w: 6, h: 12, minW: 3, minH: 4 },
  insight: { w: 4, h: 7, minW: 2, minH: 2 },
  action: { w: 4, h: 7, minW: 2, minH: 2 }, // Kept for backward compatibility but no longer used
};

const ResponsiveLayoutKeys = Object.keys(COLS) as Array<keyof typeof COLS>;

const placeTilesForCols = (tiles: DashboardTile[], cols: number): Layout[] => {
  if (cols <= 0) return [];
  const columnHeights = Array(cols).fill(0);

  return tiles.map((tile) => {
    const config = TILE_CONFIG[tile.kind];
    const w = Math.min(config.w, cols);
    const minW = Math.min(config.minW, cols);
    const h = config.h;
    const minH = config.minH;

    let bestX = 0;
    let bestY = Number.MAX_SAFE_INTEGER;

    for (let x = 0; x <= cols - w; x++) {
      const slice = columnHeights.slice(x, x + w);
      const height = Math.max(...slice);
      if (height < bestY) {
        bestY = height;
        bestX = x;
      }
    }

    for (let i = bestX; i < bestX + w; i++) {
      columnHeights[i] = bestY + h;
    }

    return {
      i: tile.id,
      x: bestX,
      y: bestY,
      w,
      h,
      minW,
      minH,
    };
  });
};

const generateLayouts = (tiles: DashboardTile[]): Layouts => {
  const baseLayouts: Layouts = {};

  ResponsiveLayoutKeys.forEach((key) => {
    baseLayouts[key] = placeTilesForCols(tiles, COLS[key]);
  });

  return baseLayouts;
};

const loadStoredLayouts = (dashboardId: string): Layouts | null => {
  if (typeof window === 'undefined') return null;
  try {
    const raw = localStorage.getItem(`${STORAGE_PREFIX}${dashboardId}`);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === 'object') {
      return parsed as Layouts;
    }
  } catch (error) {
    console.warn('Failed to parse stored dashboard layout', error);
  }
  return null;
};

const persistLayouts = (dashboardId: string, layouts: Layouts) => {
  if (typeof window === 'undefined') return;
  try {
    localStorage.setItem(`${STORAGE_PREFIX}${dashboardId}`, JSON.stringify(layouts));
  } catch (error) {
    console.warn('Failed to persist dashboard layout', error);
  }
};

const ensureLayoutsForTiles = (layouts: Layouts, tiles: DashboardTile[], fallback: Layouts): Layouts => {
  const tileIds = new Set(tiles.map((tile) => tile.id));
  const next: Layouts = {};

  ResponsiveLayoutKeys.forEach((key) => {
    const current = layouts[key] ? [...layouts[key]] : [];
    const base = fallback[key] ?? [];

    const filtered = current.filter((item) => tileIds.has(item.i));

    const missingTiles = tiles.filter((tile) => !filtered.some((item) => item.i === tile.id));
    missingTiles.forEach((tile) => {
      const fallbackItem = base.find((item) => item.i === tile.id);
      if (fallbackItem) {
        filtered.push({ ...fallbackItem });
      } else {
        const config = TILE_CONFIG[tile.kind];
        filtered.push({
          i: tile.id,
          x: 0,
          y: filtered.length > 0 ? Math.max(...filtered.map((item) => item.y + item.h)) : 0,
          w: Math.min(config.w, COLS[key]),
          h: config.h,
          minW: Math.min(config.minW, COLS[key]),
          minH: config.minH,
        });
      }
    });

    next[key] = filtered;
  });

  return next;
};

const loadHiddenTiles = (dashboardId: string): Set<string> => {
  if (typeof window === 'undefined') return new Set();
  try {
    const raw = localStorage.getItem(`${HIDDEN_TILE_PREFIX}${dashboardId}`);
    if (!raw) return new Set();
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      return new Set(parsed.filter((id) => typeof id === 'string'));
    }
  } catch (error) {
    console.warn('Failed to parse hidden tile ids', error);
  }
  return new Set();
};

const persistHiddenTiles = (dashboardId: string, hidden: Set<string>) => {
  if (typeof window === 'undefined') return;
  try {
    localStorage.setItem(`${HIDDEN_TILE_PREFIX}${dashboardId}`, JSON.stringify(Array.from(hidden)));
  } catch (error) {
    console.warn('Failed to persist hidden tile ids', error);
  }
};

export const DashboardTiles: React.FC<DashboardTilesProps> = ({
  dashboardId,
  tiles,
  onDeleteChart,
  filtersByTile,
  onTileFiltersChange,
  sheetId,
  onUpdate,
  canEdit = true, // Default to true for backward compatibility
}) => {
  const [hiddenIds, setHiddenIds] = useState<Set<string>>(() => loadHiddenTiles(dashboardId));
  const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false);
  const [pendingDelete, setPendingDelete] = useState<{ type: 'chart' | 'insight'; index: number; title: string; chartIndex?: number } | null>(null);
  const [editingTile, setEditingTile] = useState<{ type: 'insight'; chartIndex: number; text: string } | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const { toast } = useToast();
  const { updateChartInsightOrRecommendation } = useDashboardContext();

  useEffect(() => {
    setHiddenIds(loadHiddenTiles(dashboardId));
  }, [dashboardId]);

  const visibleTiles = useMemo(
    () => tiles.filter((tile) => !hiddenIds.has(tile.id)),
    [hiddenIds, tiles]
  );

  const fallbackLayouts = useMemo(() => generateLayouts(visibleTiles), [visibleTiles]);
  const [layouts, setLayouts] = useState<Layouts>(() => fallbackLayouts);

  useEffect(() => {
    const stored = loadStoredLayouts(dashboardId);
    if (stored) {
      const merged = ensureLayoutsForTiles(stored, visibleTiles, fallbackLayouts);
      setLayouts(merged);
    } else {
      setLayouts(fallbackLayouts);
    }
  }, [dashboardId, fallbackLayouts, visibleTiles]);

  useEffect(() => {
    setLayouts((prev) => ensureLayoutsForTiles(prev, visibleTiles, fallbackLayouts));
  }, [visibleTiles, fallbackLayouts]);

  const handleLayoutChange = useCallback(
    (_current: Layout[], allLayouts: Layouts) => {
      const sanitized = ensureLayoutsForTiles(allLayouts, visibleTiles, fallbackLayouts);
      setLayouts(sanitized);
      persistLayouts(dashboardId, sanitized);
    },
    [dashboardId, fallbackLayouts, visibleTiles]
  );

  const handleHideTile = useCallback(
    (tileId: string) => {
      setHiddenIds((prev) => {
        const next = new Set(prev);
        next.add(tileId);
        persistHiddenTiles(dashboardId, next);
        return next;
      });
    },
    [dashboardId]
  );

  const handleRestoreTiles = useCallback(() => {
    setHiddenIds(() => {
      const next = new Set<string>();
      persistHiddenTiles(dashboardId, next);
      return next;
    });
  }, [dashboardId]);

  const handleDeleteClick = useCallback((tile: DashboardTile) => {
    if (tile.kind === 'chart') {
      setPendingDelete({ type: 'chart', index: tile.index, title: tile.title || `Chart ${tile.index + 1}` });
      setDeleteConfirmOpen(true);
    } else if (tile.kind === 'insight') {
      // For insights, just remove the insight, not the chart
      if (tile.relatedChartId) {
        const relatedTile = tiles.find(t => t.id === tile.relatedChartId);
        if (relatedTile && relatedTile.kind === 'chart') {
          setPendingDelete({ 
            type: 'insight', 
            index: relatedTile.index,
            chartIndex: relatedTile.index,
            title: tile.title || 'Key Insight'
          });
          setDeleteConfirmOpen(true);
        }
      }
    }
  }, [tiles]);

  const handleConfirmDelete = useCallback(async () => {
    if (!pendingDelete) return;

    if (pendingDelete.type === 'chart') {
      // Delete the entire chart
      onDeleteChart(pendingDelete.index);
      setDeleteConfirmOpen(false);
      setPendingDelete(null);
    } else if (pendingDelete.type === 'insight') {
      // Just remove the insight, not the chart
      if (pendingDelete.chartIndex === undefined) {
        toast({
          title: 'Error',
          description: 'Unable to delete: chart index not found',
          variant: 'destructive',
        });
        setDeleteConfirmOpen(false);
        setPendingDelete(null);
        return;
      }

      setIsSaving(true);
      try {
        await updateChartInsightOrRecommendation(
          dashboardId,
          pendingDelete.chartIndex,
          { keyInsight: '' },
          sheetId
        );
        
        toast({
          title: 'Success',
          description: 'Key insight deleted successfully.',
        });
        
        setDeleteConfirmOpen(false);
        setPendingDelete(null);
        
        // Refetch dashboards to get the updated data
        if (onUpdate) {
          await onUpdate();
        }
      } catch (error: any) {
        toast({
          title: 'Error',
          description: error?.message || 'Failed to delete insight',
          variant: 'destructive',
        });
      } finally {
        setIsSaving(false);
      }
    }
  }, [pendingDelete, onDeleteChart, updateChartInsightOrRecommendation, dashboardId, sheetId, onUpdate, toast]);

  useEffect(() => {
    persistHiddenTiles(dashboardId, hiddenIds);
  }, [dashboardId, hiddenIds]);

  const renderTileContent = (tile: DashboardTile) => {
    switch (tile.kind) {
      case 'chart':
        return (
          <Card className="relative flex h-full flex-col overflow-hidden border border-border/60 bg-background shadow-sm transition-shadow hover:shadow-md dashboard-tile-grab-area group" data-dashboard-tile="chart">
            <CardHeader className="flex w-full items-center justify-between pb-2 pt-3 px-4">
            <div className="flex items-center justify-between w-full">
                <CardTitle className="text-base text-foreground">
                  {tile.title || `Chart ${tile.index + 1}`}
                </CardTitle>
                {canEdit && (
                  <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-7 w-7 text-muted-foreground hover:text-destructive"
                      aria-label="Remove chart from dashboard"
                      onClick={() => handleDeleteClick(tile)}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>
                )}
              </div>
            </CardHeader>
            <CardContent className="flex min-h-0 flex-1 flex-col gap-3 pt-0 px-4">
              <div className="flex-1 min-h-[120px] min-w-0" data-dashboard-chart-node>
                <Suspense fallback={<Skeleton className="h-full w-full" />}>
                  <ChartRenderer
                    chart={tile.chart}
                    index={tile.index}
                    isSingleChart={false}
                    showAddButton={false}
                    useChartOnlyModal
                    fillParent
                    enableFilters
                    filters={filtersByTile[tile.id]}
                    onFiltersChange={(next) => onTileFiltersChange(tile.id, next)}
                  />
                </Suspense>
              </div>
            </CardContent>
          </Card>
        );
      case 'insight': {
        const chartIndex = tile.relatedChartId ? parseInt(tile.relatedChartId.replace('chart-', ''), 10) : -1;
        return (
          <Card className="relative flex h-full flex-col overflow-hidden border border-primary/20 bg-primary/5 shadow-sm transition-shadow hover:shadow-md dashboard-tile-grab-area group" data-dashboard-tile="insight">
            <CardHeader className="flex w-full items-center justify-between pb-2 pt-3 px-4">
              <div className="flex items-center justify-between w-full">
                {tile.title && (
                  <CardTitle className="text-sm font-semibold text-primary flex-1 min-w-0">{tile.title}</CardTitle>
                )}
                {canEdit && (
                  <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity flex-shrink-0">
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-7 w-7 text-primary hover:text-primary/80"
                      aria-label="Edit insight"
                      onClick={(e) => {
                        e.stopPropagation();
                        if (chartIndex >= 0) {
                          setEditingTile({ type: 'insight', chartIndex, text: tile.narrative });
                        }
                      }}
                    >
                      <Edit2 className="h-3.5 w-3.5" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-7 w-7 text-muted-foreground hover:text-destructive"
                      aria-label="Remove insight tile"
                      onClick={() => handleDeleteClick(tile)}
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </Button>
                  </div>
                )}
              </div>
            </CardHeader>
            <CardContent className="flex-1 overflow-auto pt-0 px-4 pb-4">
              <p className="text-sm text-foreground/90 leading-relaxed">{tile.narrative}</p>
            </CardContent>
          </Card>
        );
      }
      default:
        return null;
    }
  };

  const hasHiddenTiles = hiddenIds.size > 0;

  return (
    <div className="space-y-4">
      <ResponsiveGridLayout
        className="dashboard-grid"
        layouts={layouts}
        cols={COLS}
        rowHeight={ROW_HEIGHT}
        margin={GRID_MARGIN}
        isResizable={canEdit}
        isDraggable={canEdit}
        resizeHandles={canEdit ? ['s', 'e', 'n', 'w', 'se', 'sw', 'ne', 'nw'] : []}
        onLayoutChange={handleLayoutChange}
        draggableHandle={canEdit ? ".dashboard-tile-grab-area" : ""}
        compactType={null}
        preventCollision={false}
        draggableCancel="[data-dashboard-tile='chart'] button, [data-dashboard-tile='insight'] button"
      >
        {visibleTiles.map((tile) => (
          <div key={tile.id} className="h-full w-full">
            {renderTileContent(tile)}
          </div>
        ))}
      </ResponsiveGridLayout>

      {hasHiddenTiles && (
        <div className="flex justify-end">
          <Button variant="ghost" size="sm" onClick={handleRestoreTiles}>
            Restore hidden tiles
          </Button>
        </div>
      )}

      {/* Delete Confirmation Modal */}
      <Dialog open={deleteConfirmOpen} onOpenChange={setDeleteConfirmOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Confirm Deletion</DialogTitle>
            <DialogDescription>
              {pendingDelete?.type === 'chart' && (
                <>Are you sure you want to delete the chart "{pendingDelete.title}"? This will also remove its associated insights. This action cannot be undone.</>
              )}
              {pendingDelete?.type === 'insight' && (
                <>Are you sure you want to delete the key insight? This will remove only the insight, and the chart will remain. This action cannot be undone.</>
              )}
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => {
              setDeleteConfirmOpen(false);
              setPendingDelete(null);
            }}>
              Cancel
            </Button>
            <Button variant="destructive" onClick={handleConfirmDelete} disabled={isSaving}>
              {isSaving ? (
                <>
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  Deleting...
                </>
              ) : (
                'Delete'
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Edit Insight Modal */}
      {editingTile && (
        <EditInsightModal
          isOpen={!!editingTile}
          onClose={() => setEditingTile(null)}
          onSave={async (text: string) => {
            if (editingTile.chartIndex < 0) return;
            setIsSaving(true);
            try {
              await updateChartInsightOrRecommendation(
                dashboardId,
                editingTile.chartIndex,
                { keyInsight: text },
                sheetId
              );
              setEditingTile(null);
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
          }}
          title="Key Insight"
          initialText={editingTile.text}
          isLoading={isSaving}
        />
      )}
    </div>
  );
};

