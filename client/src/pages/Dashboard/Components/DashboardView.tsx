import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { DashboardData } from '../modules/useDashboardState';
import { useToast } from '@/hooks/use-toast';
import * as htmlToImage from 'html-to-image';
import PptxGenJS from 'pptxgenjs';
import { DashboardSection, DashboardTile } from '../types';
import { DashboardHeader } from './DashboardHeader';
import { DashboardTiles } from './DashboardTiles';
import { ShareDashboardDialog } from './ShareDashboardDialog';
import { ActiveChartFilters, hasActiveFilters } from '@/lib/chartFilters';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Checkbox } from '@/components/ui/checkbox';
import { Label } from '@/components/ui/label';
import { ChevronLeft, ChevronRight, FileText, Edit2, Check, X, Trash2, Download, Loader2, Plus } from 'lucide-react';
import { cn } from '@/lib/utils';
import { useDashboardContext } from '../context/DashboardContext';
import { getUserEmail } from '@/utils/userStorage';

interface DashboardViewProps {
  dashboard: DashboardData;
  onBack: () => void;
  onDeleteChart: (chartIndex: number, sheetId?: string) => void;
  isRefreshing?: boolean;
  onRefresh?: () => Promise<any>;
  permission?: "view" | "edit"; // Optional permission, defaults to checking ownership
}

const PPT_LAYOUT = 'LAYOUT_16x9';

export function DashboardView({ dashboard, onBack, onDeleteChart, isRefreshing = false, onRefresh, permission }: DashboardViewProps) {
  const [isExporting, setIsExporting] = useState(false);
  const [activeSheetId, setActiveSheetId] = useState<string | null>(null);
  const [isSheetSidebarOpen, setIsSheetSidebarOpen] = useState(true);
  const [tileFilters, setTileFilters] = useState<Record<string, ActiveChartFilters>>({});
  const [editingSheetId, setEditingSheetId] = useState<string | null>(null);
  const [editSheetName, setEditSheetName] = useState('');
  const [deleteSheetDialogOpen, setDeleteSheetDialogOpen] = useState(false);
  const [sheetToDelete, setSheetToDelete] = useState<{ id: string; name: string } | null>(null);
  const [exportDialogOpen, setExportDialogOpen] = useState(false);
  const [selectedSheetIds, setSelectedSheetIds] = useState<Set<string>>(new Set());
  const [addSheetDialogOpen, setAddSheetDialogOpen] = useState(false);
  const [newSheetName, setNewSheetName] = useState('');
  const [shareDialogOpen, setShareDialogOpen] = useState(false);
  const { toast } = useToast();
  const { renameDashboard, renameSheet, addSheet, removeSheet, refetch: refetchDashboards } = useDashboardContext();

  // Determine permission: if not provided, check if user owns the dashboard or has edit permission on shared dashboard
  const canEdit = useMemo(() => {
    if (permission !== undefined) {
      return permission === "edit";
    }
    // If it's a shared dashboard, use the shared permission
    if (dashboard.isShared && dashboard.sharedPermission) {
      return dashboard.sharedPermission === "edit";
    }
    // Check if user is a collaborator with edit permission
    const userEmail = getUserEmail()?.toLowerCase();
    if (dashboard.collaborators && userEmail) {
      const collaborator = dashboard.collaborators.find(
        (c) => c.userId.toLowerCase() === userEmail
      );
      if (collaborator && collaborator.permission === "edit") {
        return true;
      }
    }
    // Check ownership by comparing username with current user email
    const dashboardUsername = dashboard.username?.toLowerCase();
    return userEmail === dashboardUsername;
  }, [permission, dashboard]);

  // Get sheets or create default from charts (backward compatibility)
  const sheets = useMemo(() => {
    if (dashboard.sheets && dashboard.sheets.length > 0) {
      return dashboard.sheets.sort((a, b) => (a.order || 0) - (b.order || 0));
    }
    // Backward compatibility: create default sheet from charts
    return [{
      id: 'default',
      name: 'Overview',
      charts: dashboard.charts,
      order: 0,
    }];
  }, [dashboard.sheets, dashboard.charts]);

  // Set active sheet on mount
  useEffect(() => {
    if (!activeSheetId && sheets.length > 0) {
      setActiveSheetId(sheets[0].id);
    }
  }, [activeSheetId, sheets]);

  const activeSheet = sheets.find(s => s.id === activeSheetId) || sheets[0];
  
  // Ensure activeSheetId is always set when we have sheets
  const currentSheetId = activeSheetId || (sheets.length > 0 ? sheets[0].id : null);

  const sections = useMemo<DashboardSection[]>(() => {
    if (!activeSheet) return [];
    
    const baseTiles: DashboardTile[] = activeSheet.charts.flatMap((chart, index) => {
      const chartId = `chart-${index}`;
      const tiles: DashboardTile[] = [
        {
          kind: 'chart',
          id: chartId,
          title: chart.title || `Chart ${index + 1}`,
          chart,
          index,
          metadata: {
            lastUpdated: dashboard.updatedAt,
          },
        },
      ];

      if (chart.keyInsight) {
        tiles.push({
          kind: 'insight',
          id: `insight-${index}`,
          title: 'Key Insight',
          narrative: chart.keyInsight,
          relatedChartId: chartId,
        });
      }

      return tiles;
    });

    // Always return a section, even if there are no tiles (empty sheet)
    return [
      {
        id: activeSheet.id,
        title: activeSheet.name,
        description: `Charts and insights for ${activeSheet.name}`,
        tiles: baseTiles,
      },
    ];
  }, [activeSheet, dashboard.updatedAt]);

  const chartTiles = useMemo(
    () => sections.flatMap((section) => section.tiles).filter((tile): tile is DashboardTile & { kind: 'chart' } => tile.kind === 'chart'),
    [sections]
  );

  const insightMap = useMemo(() => {
    const map = new Map<string, DashboardTile>();
    sections.forEach((section) => {
      section.tiles.forEach((tile) => {
        if (tile.kind === 'insight' && tile.relatedChartId) {
          map.set(`${tile.kind}-${tile.relatedChartId}`, tile);
        }
      });
    });
    return map;
  }, [sections]);

  const activeSection = sections.find((section) => section.id === activeSheetId) ?? sections[0];

  useEffect(() => {
    const validIds = new Set(
      sections.flatMap((section) => section.tiles.map((tile) => tile.id))
    );
    setTileFilters((prev) => {
      let changed = false;
      const next: Record<string, ActiveChartFilters> = {};
      Object.entries(prev).forEach(([tileId, filters]) => {
        if (validIds.has(tileId)) {
          next[tileId] = filters;
        } else {
          changed = true;
        }
      });
      return changed ? next : prev;
    });
  }, [sections]);

  useEffect(() => {
    setTileFilters({});
  }, [dashboard.id, activeSheetId]);

  const handleTileFiltersChange = useCallback((tileId: string, filters: ActiveChartFilters) => {
    setTileFilters((prev) => {
      const next = { ...prev };
      if (hasActiveFilters(filters)) {
        next[tileId] = filters;
      } else {
        delete next[tileId];
      }
      return next;
    });
  }, []);


  // Handle adding a new sheet
  const handleAddSheet = async () => {
    if (!newSheetName.trim()) return;
    
    try {
      const updated = await addSheet(dashboard.id, newSheetName.trim());
      const newSheet = updated.sheets?.find(s => s.name === newSheetName.trim());
      
      if (newSheet) {
        setActiveSheetId(newSheet.id);
      }
      
      toast({
        title: 'View Created',
        description: `View "${newSheetName.trim()}" has been created.`,
      });
      
      setAddSheetDialogOpen(false);
      setNewSheetName('');
      
      // Refetch to get updated dashboard
    if (onRefresh) {
      await onRefresh();
    }
      await refetchDashboards();
    } catch (error: any) {
      toast({
        title: 'Error',
        description: error?.message || 'Failed to create view',
        variant: 'destructive',
      });
    }
  };

  // Handle export button click - open dialog
  const handleExportClick = () => {
    if (sheets.length === 1) {
      // If only one sheet, export directly
      handleExport([sheets[0].id]);
    } else {
      // Show dialog for sheet selection
      setSelectedSheetIds(new Set(sheets.map(s => s.id))); // Select all by default
      setExportDialogOpen(true);
    }
  };

  // Handle actual export with selected sheets
  const handleExport = async (sheetIdsToExport?: string[]) => {
    if (isExporting) return;

    const sheetsToExport = sheetIdsToExport || Array.from(selectedSheetIds);
    
    if (sheetsToExport.length === 0) {
      toast({ title: 'No views selected', description: 'Please select at least one view to export.' });
      return;
    }

    // Get all charts from selected sheets
    const allCharts: Array<{ sheet: typeof sheets[0]; chartIndex: number; chart: any }> = [];
    sheetsToExport.forEach(sheetId => {
      const sheet = sheets.find(s => s.id === sheetId);
      if (sheet) {
        sheet.charts.forEach((chart, index) => {
          allCharts.push({ sheet, chartIndex: index, chart });
        });
      }
    });

    if (allCharts.length === 0) {
      toast({ title: 'Nothing to export', description: 'Selected views have no content yet.' });
      setExportDialogOpen(false);
      return;
    }

    setIsExporting(true);
    setExportDialogOpen(false);

    try {
      const pptx = new PptxGenJS();
      pptx.layout = PPT_LAYOUT;

      const originalActiveSheetId = activeSheetId;
      let slideIndex = 0;

      // Process each selected sheet
      for (let sheetIndex = 0; sheetIndex < sheetsToExport.length; sheetIndex++) {
        const sheetId = sheetsToExport[sheetIndex];
        const sheet = sheets.find(s => s.id === sheetId);
        if (!sheet || sheet.charts.length === 0) continue;

        // Switch to this sheet to render its charts
        setActiveSheetId(sheetId);
        
        // Wait for charts to render (small delay to ensure DOM updates)
        await new Promise(resolve => setTimeout(resolve, 500));

        // Get chart nodes for this sheet
        const chartNodes = Array.from(document.querySelectorAll('[data-dashboard-chart-node]')) as HTMLElement[];
        
        if (chartNodes.length === 0) {
          console.warn(`No chart nodes found for sheet: ${sheet.name}`);
          continue;
        }

        // Process each chart in this sheet
        for (let chartIndex = 0; chartIndex < Math.min(sheet.charts.length, chartNodes.length); chartIndex++) {
          const chart = sheet.charts[chartIndex];
          const chartNode = chartNodes[chartIndex];
        const slide = pptx.addSlide();

        let imgData: string | undefined;
        if (chartNode) {
          // Increase quality by using higher pixel ratio for better resolution
          imgData = await htmlToImage.toPng(chartNode, {
            cacheBust: true,
            backgroundColor: '#FFFFFF',
            style: { boxShadow: 'none' },
            pixelRatio: 5, // Triple the resolution for crisp, high-quality images
            quality: 1.0, // Maximum quality (0-1 range)
          });
        }

        // Layout matching Keynote style: title at top, chart left, text right
        const slideWidth = 10; // Standard slide width in inches
        const leftPad = 0.3;
        const topPad = 0.3; // Start higher for title
        const titleHeight = 0.5;
        
        // Chart dimensions - left side
        const imgW = 5.5; // Chart width
        const imgH = 4.0; // Chart height
        const chartTopY = topPad + titleHeight + 0.2; // Below title

        // Add chart title at the top (centered or left-aligned)
        slide.addText(chart.title || `Chart ${chartIndex + 1}`, {
          x: leftPad,
          y: topPad,
          w: slideWidth - (leftPad * 2),
          h: titleHeight,
          fontSize: 18,
          bold: true,
          color: '1F2937',
          align: 'left',
          valign: 'middle',
        });

        // Add chart image on the left
        if (imgData) {
          slide.addImage({ data: imgData, x: leftPad, y: chartTopY, w: imgW, h: imgH });
        }

        // Text sections on the right - aligned with chart top
        const rightX = leftPad + imgW + 0.4; // Gap between chart and text
        const colW = slideWidth - rightX - leftPad; // Remaining width
        let currentY = chartTopY; // Start at same Y as chart

        // Key Insight section
        if (chart.keyInsight) {
          slide.addText('Key Insight', {
            x: rightX,
            y: currentY,
            w: colW,
            fontSize: 13,
            bold: true,
            color: '0B63F6',
            valign: 'top',
          });
          currentY += 0.4;
          
          slide.addText(chart.keyInsight, {
            x: rightX,
            y: currentY,
            w: colW,
            h: 1.8, // Height for insight text
            fontSize: 11,
            color: '111827',
            wrap: true,
            valign: 'top',
          });
          currentY += 2.0; // Space for insight text + gap
        }


          slideIndex++;
        }
      }

      // Restore original active sheet
      setActiveSheetId(originalActiveSheetId);

      const fileName = sheetsToExport.length === sheets.length 
        ? `${dashboard.name || 'dashboard'}.pptx`
        : `${dashboard.name || 'dashboard'}_${sheetsToExport.length}_sheets.pptx`;
      
      await pptx.writeFile({ fileName });
      toast({ 
        title: 'Export complete', 
        description: `Your PowerPoint with ${slideIndex} slide${slideIndex !== 1 ? 's' : ''} has been downloaded.` 
      });
    } catch (err) {
      console.error(err);
      toast({
        title: 'Export failed',
        description: 'Please try again or contact support.',
        variant: 'destructive',
      });
    } finally {
      setIsExporting(false);
    }
  };

  // Handle sheet selection in export dialog
  const handleSheetToggle = (sheetId: string) => {
    setSelectedSheetIds(prev => {
      const next = new Set(prev);
      if (next.has(sheetId)) {
        next.delete(sheetId);
      } else {
        next.add(sheetId);
      }
      return next;
    });
  };

  const handleSelectAll = () => {
    if (selectedSheetIds.size === sheets.length) {
      setSelectedSheetIds(new Set());
    } else {
      setSelectedSheetIds(new Set(sheets.map(s => s.id)));
    }
  };

  return (
    <div className="bg-muted/30 h-[calc(100vh-72px)] flex flex-col overflow-y-auto">
      <div className="flex-shrink-0 px-4 pt-8 pb-4 lg:px-8">
        <DashboardHeader
          name={dashboard.name}
          lastOpenedAt={dashboard.lastOpenedAt}
          updatedAt={dashboard.updatedAt}
          sheetCount={sheets.length}
          isExporting={isExporting}
          onBack={onBack}
          onExport={handleExportClick}
          onShare={canEdit ? () => setShareDialogOpen(true) : undefined}
          onRename={canEdit ? async (newName) => {
            try {
              await renameDashboard(dashboard.id, newName);
              if (onRefresh) {
                await onRefresh();
              }
              await refetchDashboards();
            } catch (error: any) {
              toast({
                title: 'Error',
                description: error?.message || 'Failed to rename dashboard',
                variant: 'destructive',
              });
              throw error;
            }
          } : undefined}
        />

      </div>

      <div className="flex-1 min-h-0 flex overflow-hidden">
        {/* Collapsible Sheet Sidebar */}
        {sheets.length > 0 && (
          <>
            <div
              className={cn(
                "flex-shrink-0 bg-background border-r border-border transition-all duration-300 ease-in-out overflow-hidden",
                isSheetSidebarOpen ? "w-64" : "w-0"
              )}
            >
              <div className="h-full flex flex-col">
                <div className="flex items-center justify-between p-4 border-b">
                  <div className="flex items-center gap-2">
                    <h3 className="font-semibold text-sm text-foreground">Views</h3>
                    {canEdit && (
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-6 w-6"
                        onClick={() => {
                          setNewSheetName('');
                          setAddSheetDialogOpen(true);
                        }}
                      >
                        <Plus className="h-4 w-4" />
                      </Button>
                    )}
                  </div>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-6 w-6"
                    onClick={() => setIsSheetSidebarOpen(false)}
                  >
                    <ChevronLeft className="h-4 w-4" />
                  </Button>
                </div>
                <div className="flex-1 overflow-y-auto p-2">
                  <div className="space-y-1">
                    {sheets.map((sheet) => {
                      const isActive = activeSheetId === sheet.id;
                      const isEditing = editingSheetId === sheet.id;
                      
                      const handleStartEdit = (e: React.MouseEvent) => {
                        e.stopPropagation();
                        setEditingSheetId(sheet.id);
                        setEditSheetName(sheet.name);
                      };

                      const handleSaveSheet = async (e: React.MouseEvent) => {
                        e.stopPropagation();
                        if (!editSheetName.trim() || editSheetName.trim() === sheet.name) {
                          setEditingSheetId(null);
                          return;
                        }
                        try {
                          await renameSheet(dashboard.id, sheet.id, editSheetName.trim());
                          setEditingSheetId(null);
                          if (onRefresh) {
                            await onRefresh();
                          }
                          await refetchDashboards();
                        } catch (error: any) {
                          toast({
                            title: 'Error',
                            description: error?.message || 'Failed to rename view',
                            variant: 'destructive',
                          });
                        }
                      };

                      const handleCancelEdit = (e: React.MouseEvent) => {
                        e.stopPropagation();
                        setEditingSheetId(null);
                        setEditSheetName('');
                      };

                      const handleKeyDown = (e: React.KeyboardEvent) => {
                        if (e.key === 'Enter') {
                          e.preventDefault();
                          handleSaveSheet(e as any);
                        } else if (e.key === 'Escape') {
                          e.preventDefault();
                          handleCancelEdit(e as any);
                        }
                      };

                      const handleDeleteClick = (e: React.MouseEvent) => {
                        e.stopPropagation();
                        setSheetToDelete({ id: sheet.id, name: sheet.name });
                        setDeleteSheetDialogOpen(true);
                      };

                      return (
                        <div
                          key={sheet.id}
                          className={cn(
                            "w-full flex items-center gap-2 px-3 py-2.5 rounded-md transition-colors group",
                            isActive && !isEditing
                              ? "bg-primary text-primary-foreground"
                              : "hover:bg-muted text-foreground"
                          )}
                        >
                          <FileText className={cn("h-4 w-4 flex-shrink-0", isActive && !isEditing ? "text-primary-foreground" : "text-muted-foreground")} />
                          {isEditing ? (
                            <div className="flex-1 flex items-center gap-1">
                              <Input
                                value={editSheetName}
                                onChange={(e) => setEditSheetName(e.target.value)}
                                onKeyDown={handleKeyDown}
                                onClick={(e) => e.stopPropagation()}
                                className="h-7 text-sm"
                                autoFocus
                              />
                              <Button
                                variant="ghost"
                                size="icon"
                                onClick={handleSaveSheet}
                                className="h-6 w-6"
                              >
                                <Check className="h-3 w-3" />
                              </Button>
                              <Button
                                variant="ghost"
                                size="icon"
                                onClick={handleCancelEdit}
                                className="h-6 w-6"
                              >
                                <X className="h-3 w-3" />
                              </Button>
                            </div>
                          ) : (
                            <>
                              <button
                                onClick={() => setActiveSheetId(sheet.id)}
                                className="flex-1 min-w-0 text-left"
                              >
                                <div className={cn("font-medium text-sm truncate", isActive && "text-primary-foreground")}>
                                  {sheet.name}
                                </div>
                                <div className={cn("text-xs truncate", isActive ? "text-primary-foreground/80" : "text-muted-foreground")}>
                                  {sheet.charts.length} chart{sheet.charts.length !== 1 ? 's' : ''}
                                </div>
                              </button>
                              {canEdit && (
                                <div className="flex gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
                                  <Button
                                    variant="ghost"
                                    size="icon"
                                    onClick={handleStartEdit}
                                    className={cn("h-6 w-6 flex-shrink-0", isActive && "text-primary-foreground")}
                                    aria-label="Rename view"
                                  >
                                    <Edit2 className="h-3 w-3" />
                                  </Button>
                                  {sheets.length > 1 && (
                                    <Button
                                      variant="ghost"
                                      size="icon"
                                      onClick={handleDeleteClick}
                                      className={cn("h-6 w-6 flex-shrink-0 hover:text-destructive", isActive && "text-primary-foreground hover:text-destructive")}
                                      aria-label="Delete view"
                                    >
                                      <Trash2 className="h-3 w-3" />
                                    </Button>
                                  )}
                                </div>
                              )}
                            </>
                          )}
                        </div>
                      );
                    })}
                  </div>
                </div>
              </div>
            </div>
            
            {/* Collapsed Sidebar Toggle Button */}
            {!isSheetSidebarOpen && (
              <div className="flex-shrink-0 border-r border-border">
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-full w-8 rounded-none"
                  onClick={() => setIsSheetSidebarOpen(true)}
                >
                  <ChevronRight className="h-4 w-4" />
                </Button>
              </div>
            )}
          </>
        )}

        <div className="flex-1 min-h-0 flex flex-col gap-8 px-4 pb-8 lg:px-8 overflow-hidden">
          <div className="flex-1 min-w-0 overflow-y-auto overflow-x-hidden">
            {activeSection ? (
              <section
                key={activeSection.id}
                id={`section-${activeSection.id}`}
                className="space-y-4"
                data-dashboard-section={activeSection.id}
              >
                <div>
                  <h2 className="text-lg font-semibold text-foreground">{activeSection.title}</h2>
                  {activeSection.description && (
                    <p className="text-sm text-muted-foreground">{activeSection.description}</p>
                  )}
                </div>

                <DashboardTiles
                  dashboardId={dashboard.id}
                  tiles={activeSection.tiles}
                  onDeleteChart={canEdit ? (chartIndex) => {
                    const sheetIdToUse = currentSheetId || (sheets.length > 0 ? sheets[0].id : undefined);
                    console.log('Deleting chart:', { chartIndex, sheetId: sheetIdToUse, activeSheetId, sheets });
                    onDeleteChart(chartIndex, sheetIdToUse || undefined);
                  } : undefined}
                  filtersByTile={tileFilters}
                  onTileFiltersChange={handleTileFiltersChange}
                  sheetId={currentSheetId || undefined}
                  onUpdate={onRefresh}
                  canEdit={canEdit}
                />
              </section>
            ) : (
              <div className="rounded-lg border border-dashed border-border p-12 text-center text-sm text-muted-foreground">
                Select a section to get started.
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Export Sheet Selection Dialog */}
      <Dialog open={exportDialogOpen} onOpenChange={setExportDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Export Dashboard</DialogTitle>
            <DialogDescription>
              Select which views you want to export to PowerPoint. You can select multiple views or export the entire dashboard.
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            <div className="flex items-center space-x-2 pb-2 border-b">
              <Checkbox
                id="select-all"
                checked={selectedSheetIds.size === sheets.length}
                onCheckedChange={handleSelectAll}
              />
              <Label
                htmlFor="select-all"
                className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70 cursor-pointer"
              >
                Select All ({sheets.length} views)
              </Label>
            </div>
            <div className="space-y-3 max-h-[300px] overflow-y-auto">
              {sheets.map((sheet) => (
                <div key={sheet.id} className="flex items-center space-x-2">
                  <Checkbox
                    id={`sheet-${sheet.id}`}
                    checked={selectedSheetIds.has(sheet.id)}
                    onCheckedChange={() => handleSheetToggle(sheet.id)}
                  />
                  <Label
                    htmlFor={`sheet-${sheet.id}`}
                    className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70 cursor-pointer flex-1"
                  >
                    <div className="flex items-center justify-between">
                      <span>{sheet.name}</span>
                      <span className="text-xs text-muted-foreground ml-2">
                        {sheet.charts.length} chart{sheet.charts.length !== 1 ? 's' : ''}
                      </span>
                    </div>
                  </Label>
                </div>
              ))}
            </div>
            {selectedSheetIds.size === 0 && (
              <p className="text-sm text-muted-foreground text-center py-2">
                Please select at least one view to export.
              </p>
            )}
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setExportDialogOpen(false);
                setSelectedSheetIds(new Set());
              }}
              disabled={isExporting}
            >
              Cancel
            </Button>
            <Button
              onClick={() => handleExport()}
              disabled={isExporting || selectedSheetIds.size === 0}
              className="bg-blue-600 hover:bg-blue-700"
            >
              {isExporting ? (
                <>
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  Exporting...
                </>
              ) : (
                <>
                  <Download className="h-4 w-4 mr-2" />
                  Export {selectedSheetIds.size} View{selectedSheetIds.size !== 1 ? 's' : ''}
                </>
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Sheet Confirmation Dialog */}
      <Dialog open={deleteSheetDialogOpen} onOpenChange={setDeleteSheetDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete View</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete the view "{sheetToDelete?.name}"? This will permanently remove all charts in this view. This action cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setDeleteSheetDialogOpen(false);
                setSheetToDelete(null);
              }}
            >
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={async () => {
                if (!sheetToDelete) return;
                
                const wasActiveSheet = activeSheetId === sheetToDelete.id;
                
                try {
                  await removeSheet(dashboard.id, sheetToDelete.id);
                  
                  // If the deleted sheet was active, switch to the first remaining sheet
                  if (wasActiveSheet) {
                    const remainingSheets = sheets.filter(s => s.id !== sheetToDelete.id);
                    if (remainingSheets.length > 0) {
                      setActiveSheetId(remainingSheets[0].id);
                    }
                  }
                  
                  toast({
                    title: 'Sheet Deleted',
                    description: `Sheet "${sheetToDelete.name}" has been deleted.`,
                  });
                  
                  setDeleteSheetDialogOpen(false);
                  setSheetToDelete(null);
                  
                  // Refetch to get updated dashboard
                  if (onRefresh) {
                    await onRefresh();
                  }
                  await refetchDashboards();
                } catch (error: any) {
                  toast({
                    title: 'Error',
                    description: error?.message || 'Failed to delete sheet',
                    variant: 'destructive',
                  });
                }
              }}
            >
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Add Sheet Dialog */}
      <Dialog open={addSheetDialogOpen} onOpenChange={setAddSheetDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add New View</DialogTitle>
            <DialogDescription>
              Create a new view to organize your charts. Enter a name for the view.
            </DialogDescription>
          </DialogHeader>
          <div className="py-4">
            <Label htmlFor="new-sheet-name">View Name</Label>
            <Input
              id="new-sheet-name"
              value={newSheetName}
              onChange={(e) => setNewSheetName(e.target.value)}
              placeholder="Enter view name"
              onKeyDown={(e) => {
                if (e.key === 'Enter' && newSheetName.trim()) {
                  e.preventDefault();
                  handleAddSheet();
                }
              }}
              autoFocus
            />
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setAddSheetDialogOpen(false);
                setNewSheetName('');
              }}
            >
              Cancel
            </Button>
            <Button
              onClick={handleAddSheet}
              disabled={!newSheetName.trim()}
            >
              Create View
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <ShareDashboardDialog
        open={shareDialogOpen}
        onOpenChange={setShareDialogOpen}
        dashboardId={dashboard.id}
        dashboardName={dashboard.name}
      />
    </div>
  );
}
