import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { DashboardData } from '../modules/useDashboardState';
import { useToast } from '@/hooks/use-toast';
import * as htmlToImage from 'html-to-image';
import PptxGenJS from 'pptxgenjs';
import { DashboardSection, DashboardTile } from '../types';
import { DashboardHeader } from './DashboardHeader';
import { DashboardFilters } from './DashboardFilters';
import { DashboardSectionNav } from './DashboardSectionNav';
import { DashboardTiles } from './DashboardTiles';
import { DashboardEmptyState } from './DashboardEmptyState';
import { ActiveChartFilters, hasActiveFilters, summarizeChartFilters } from '@/lib/chartFilters';

interface DashboardViewProps {
  dashboard: DashboardData;
  onBack: () => void;
  onDeleteChart: (chartIndex: number) => void;
  isRefreshing?: boolean;
  onRefresh?: () => Promise<any>;
}

const PPT_LAYOUT = 'LAYOUT_16x9';

export function DashboardView({ dashboard, onBack, onDeleteChart, isRefreshing = false, onRefresh }: DashboardViewProps) {
  const [isExporting, setIsExporting] = useState(false);
  const [activeSectionId, setActiveSectionId] = useState('overview');
  const [tileFilters, setTileFilters] = useState<Record<string, ActiveChartFilters>>({});
  const { toast } = useToast();

  const sections = useMemo<DashboardSection[]>(() => {
    const baseTiles: DashboardTile[] = dashboard.charts.flatMap((chart, index) => {
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

      if (chart.recommendation) {
        tiles.push({
          kind: 'action',
          id: `action-${index}`,
          title: 'Recommended Action',
          recommendation: chart.recommendation,
          relatedChartId: chartId,
        });
      }

      return tiles;
    });

    if (baseTiles.length === 0) {
      return [];
    }

    return [
      {
        id: 'overview',
        title: 'Overview',
        description: 'A curated view of your charts, insights, and recommended actions.',
        tiles: baseTiles,
      },
    ];
  }, [dashboard.charts, dashboard.updatedAt]);

  const chartTiles = useMemo(
    () => sections.flatMap((section) => section.tiles).filter((tile): tile is DashboardTile & { kind: 'chart' } => tile.kind === 'chart'),
    [sections]
  );

  const insightMap = useMemo(() => {
    const map = new Map<string, DashboardTile>();
    sections.forEach((section) => {
      section.tiles.forEach((tile) => {
        if ((tile.kind === 'insight' || tile.kind === 'action') && tile.relatedChartId) {
          map.set(`${tile.kind}-${tile.relatedChartId}`, tile);
        }
      });
    });
    return map;
  }, [sections]);

  const activeSection = sections.find((section) => section.id === activeSectionId) ?? sections[0];

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
  }, [dashboard.id]);

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

  const dashboardFilterSummary = useMemo(() => {
    const summary: string[] = [];
    sections.forEach((section) => {
      section.tiles.forEach((tile) => {
        if (tile.kind !== 'chart') return;
        const filters = tileFilters[tile.id];
        if (!filters || !hasActiveFilters(filters)) return;
        const chipSummaries = summarizeChartFilters(filters);
        if (chipSummaries.length === 0) return;
        summary.push(`${tile.title}: ${chipSummaries.join(' • ')}`);
      });
    });
    return summary;
  }, [sections, tileFilters]);

  const handleResetAllFilters = useCallback(async () => {
    setTileFilters({});
    if (onRefresh) {
      await onRefresh();
    }
  }, [onRefresh]);

  const handleExport = async () => {
    if (isExporting) return;
    if (chartTiles.length === 0) {
      toast({ title: 'Nothing to export', description: 'This dashboard has no content yet.' });
      return;
    }

    setIsExporting(true);

    try {
      const chartNodes = Array.from(document.querySelectorAll('[data-dashboard-chart-node]')) as HTMLElement[];
      if (chartNodes.length === 0) {
        toast({ title: 'No charts found', description: 'Try refreshing the page and exporting again.' });
        return;
      }

      const pptx = new PptxGenJS();
      pptx.layout = PPT_LAYOUT;

      const totalSlides = Math.min(chartTiles.length, chartNodes.length);

      for (let index = 0; index < totalSlides; index++) {
        const chartTile = chartTiles[index];
        const chartNode = chartNodes[index];
        const slide = pptx.addSlide();

        let imgData: string | undefined;
        if (chartNode) {
          imgData = await htmlToImage.toPng(chartNode, {
            cacheBust: true,
            backgroundColor: '#FFFFFF',
            style: { boxShadow: 'none' },
          });
        }

        const leftPad = 0.5;
        const topPad = 0.6;
        const imgW = 7.0;
        const imgH = 4.0;

        if (imgData) {
          slide.addImage({ data: imgData, x: leftPad, y: topPad, w: imgW, h: imgH });
        }

        const rightX = leftPad + imgW + 0.4;
        const colW = 3.2;

        slide.addText(chartTile.title, {
          x: rightX,
          y: topPad,
          w: colW,
          fontSize: 16,
          bold: true,
          color: '1F2937',
        });

        const insight = insightMap.get(`insight-${chartTile.id}`);
        if (insight && insight.kind === 'insight') {
          slide.addText('Key Insight', {
            x: rightX,
            y: topPad + 0.4,
            w: colW,
            fontSize: 12,
            bold: true,
            color: '0B63F6',
          });
          slide.addText(insight.narrative, {
            x: rightX,
            y: topPad + 0.7,
            w: colW,
            h: 2.0,
            fontSize: 11,
            color: '111827',
            wrap: true,
          });
        }

        const recommendation = insightMap.get(`action-${chartTile.id}`);
        if (recommendation && recommendation.kind === 'action') {
          const recY = topPad + 2.9;
          slide.addText('Recommendation', {
            x: rightX,
            y: recY,
            w: colW,
            fontSize: 12,
            bold: true,
            color: '059669',
          });
          slide.addText(recommendation.recommendation, {
            x: rightX,
            y: recY + 0.3,
            w: colW,
            h: 1.8,
            fontSize: 11,
            color: '111827',
            wrap: true,
          });
        }
      }

      await pptx.writeFile({ fileName: `${dashboard.name || 'dashboard'}.pptx` });
      toast({ title: 'Export complete', description: 'Your PowerPoint has been downloaded.' });
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

  if (dashboard.charts.length === 0) {
    return (
      <div className="px-4 py-8">
        <DashboardEmptyState name={dashboard.name} onBack={onBack} />
      </div>
    );
  }

  return (
    <div className="bg-muted/30 h-[calc(100vh-72px)] flex flex-col overflow-y-auto">
      <div className="flex-shrink-0 px-4 pt-8 pb-4 lg:px-8">
        <DashboardHeader
          name={dashboard.name}
          createdAt={dashboard.createdAt}
          chartCount={dashboard.charts.length}
          isExporting={isExporting}
          onBack={onBack}
          onExport={handleExport}
        />

        <div className="mt-6">
          <DashboardFilters
            isLoading={isExporting || isRefreshing}
            onReset={handleResetAllFilters}
            appliedFilters={dashboardFilterSummary}
            hasActiveFilters={dashboardFilterSummary.length > 0}
          />
        </div>
      </div>

      <div className="flex-1 min-h-0 flex flex-col gap-8 px-4 pb-8 lg:px-8 lg:flex-row overflow-hidden">
        <div className="flex-shrink-0">
          <DashboardSectionNav
            sections={sections.map((section) => ({
              id: section.id,
              title: section.title,
              count: section.tiles.length,
            }))}
            activeSectionId={activeSection?.id || 'overview'}
            onSelect={(sectionId) => setActiveSectionId(sectionId)}
          />
        </div>

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
                onDeleteChart={onDeleteChart}
                filtersByTile={tileFilters}
                onTileFiltersChange={handleTileFiltersChange}
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
  );
}
