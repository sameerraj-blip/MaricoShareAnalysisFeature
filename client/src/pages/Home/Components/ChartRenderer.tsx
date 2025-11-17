import React, { useCallback, useMemo, useState } from 'react';
import { format as formatDate } from 'date-fns';
import { ChartSpec } from '@shared/schema';
import { ChartModal } from './ChartModal';
import { ChartOnlyModal } from '@/pages/Dashboard/Components/ChartOnlyModal';
import { DashboardModal } from './DashboardModal/DashboardModal';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Checkbox } from '@/components/ui/checkbox';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Slider } from '@/components/ui/slider';
import { Filter, Plus, X } from 'lucide-react';
import {
  ResponsiveContainer,
  LineChart,
  Line,
  BarChart,
  Bar,
  ScatterChart,
  Scatter,
  ComposedChart,
  PieChart,
  Pie,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  Cell,
} from 'recharts';
import {
  ActiveChartFilters,
  applyChartFilters,
  deriveChartFilterDefinitions,
  hasActiveFilters,
  ChartFilterDefinition,
} from '@/lib/chartFilters';

interface ChartRendererProps {
  chart: ChartSpec;
  index: number;
  isSingleChart?: boolean;
  showAddButton?: boolean;
  useChartOnlyModal?: boolean;
  fillParent?: boolean;
  enableFilters?: boolean;
  filters?: ActiveChartFilters;
  onFiltersChange?: (filters: ActiveChartFilters) => void;
}

const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4'];

type FiltersUpdater = ActiveChartFilters | ((prev: ActiveChartFilters) => ActiveChartFilters);

const formatDateForDisplay = (value?: string) => {
  if (!value) return undefined;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return undefined;
  return formatDate(parsed, 'd MMM yyyy');
};

const formatNumberForDisplay = (value: number) => {
  if (Number.isNaN(value)) return '';
  if (Number.isInteger(value)) return value.toString();
  const abs = Math.abs(value);
  if (abs >= 1000 || abs < 0.01) {
    return value.toPrecision(3);
  }
  return value.toFixed(2);
};

const determineSliderStep = (min: number, max: number) => {
  const range = Math.abs(max - min);
  if (!Number.isFinite(range) || range === 0) return 1;
  if (range <= 0.1) return 0.001;
  if (range <= 1) return 0.01;
  if (range <= 10) return 0.1;
  if (range <= 100) return 1;
  return Math.pow(10, Math.floor(Math.log10(range)) - 1);
};

const parseNumericValue = (value: any): number => {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : NaN;
  }
  if (typeof value === 'string') {
    const cleaned = value.replace(/[,%]/g, '').trim();
    const parsed = Number(cleaned);
    return Number.isFinite(parsed) ? parsed : NaN;
  }
  return NaN;
};

const getNumericValues = (rows: Record<string, any>[], key?: string | null) => {
  if (!key) return [];
  return rows
    .map((row) => parseNumericValue(row?.[key]))
    .filter((val) => Number.isFinite(val)) as number[];
};

const getDynamicDomain = (values: number[], paddingFraction: number = 0.1) => {
  if (!values.length) return undefined;
  const min = Math.min(...values);
  const max = Math.max(...values);
  if (!Number.isFinite(min) || !Number.isFinite(max)) return undefined;
  if (min === max) {
    const pad = Math.max(Math.abs(min) * 0.1, 5);
    return [min - pad, max + pad] as [number, number];
  }
  const range = max - min;
  const padding = Math.max(range * paddingFraction, 2);
  return [min - padding, max + padding] as [number, number];
};

// Smart number formatter for axis labels
const formatAxisLabel = (value: number): string => {
  if (Math.abs(value) < 0.01 && value !== 0) {
    return value.toFixed(4);
  }
  if (Math.abs(value) < 1000 && value % 1 !== 0) {
    return value.toFixed(2);
  }
  const absValue = Math.abs(value);
  if (absValue >= 1e9) {
    return (value / 1e9).toFixed(1) + 'B';
  } else if (absValue >= 1e6) {
    return (value / 1e6).toFixed(1) + 'M';
  } else if (absValue >= 1e3) {
    return (value / 1e3).toFixed(1) + 'K';
  }
  return value.toFixed(0);
};

export function ChartRenderer({
  chart,
  index,
  isSingleChart = false,
  showAddButton = true,
  useChartOnlyModal = false,
  fillParent = false,
  enableFilters = false,
  filters,
  onFiltersChange,
}: ChartRendererProps) {
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isDashboardModalOpen, setIsDashboardModalOpen] = useState(false);
  const [internalFilters, setInternalFilters] = useState<ActiveChartFilters>({});
  const { type, title, data: chartDataSource = [], x, y, xDomain, yDomain, trendLine, xLabel, yLabel } = chart;
  const chartColor = COLORS[index % COLORS.length];

  const originalData = useMemo(() => {
    if (!Array.isArray(chartDataSource)) {
      return [];
    }
    return chartDataSource as Record<string, unknown>[];
  }, [chartDataSource]);

  const filterDefinitions: ChartFilterDefinition[] = useMemo(() => {
    if (!enableFilters) return [];
    const forceCategoricalKeys: string[] = [];
    const forceNumericKeys: string[] = [];
    const forceDateKeys: string[] = [];

    // Check if X-axis is a date column (for time-based charts)
    if (typeof x === 'string') {
      // Check if column name suggests it's a date column
      const xLower = x.toLowerCase();
      const nameSuggestsDate = /\b(date|month|week|year|time|period)\b/i.test(xLower);
      
      // Check if sample values look like dates
      if (originalData.length > 0) {
        const sampleValues = originalData.slice(0, Math.min(5, originalData.length))
          .map(row => String(row[x] || ''));
        const allLookLikeDates = sampleValues.length > 0 && 
          sampleValues.every(v => {
            if (!v || v.length < 4) return false;
            // Check for month-year format like "Apr-22", "May-22"
            if (/^[A-Za-z]{3}[-/]?\d{2,4}$/i.test(v.trim())) return true;
            // Check for standard date formats
            const parsed = new Date(v);
            return !isNaN(parsed.getTime());
          });
        
        if (nameSuggestsDate || allLookLikeDates) {
          forceDateKeys.push(x);
        } else {
          forceCategoricalKeys.push(x);
        }
      } else {
        forceCategoricalKeys.push(x);
      }
    }
    if (typeof y === 'string') {
      forceNumericKeys.push(y);
    }

    return deriveChartFilterDefinitions(originalData, {
      forceCategoricalKeys,
      forceNumericKeys,
      forceDateKeys,
    });
  }, [enableFilters, originalData, x, y]);

  const isControlled = filters !== undefined;

  const updateFilters = useCallback(
    (updater: FiltersUpdater) => {
      if (isControlled) {
        if (!onFiltersChange) return;
        const base = filters ?? {};
        const next = typeof updater === 'function' ? (updater as (prev: ActiveChartFilters) => ActiveChartFilters)(base) : updater;
        onFiltersChange(next);
      } else {
        setInternalFilters((prev) => {
          const next = typeof updater === 'function' ? (updater as (prev: ActiveChartFilters) => ActiveChartFilters)(prev) : updater;
          onFiltersChange?.(next);
          return next;
        });
      }
    },
    [filters, isControlled, onFiltersChange]
  );

  const baseFilters = isControlled ? (filters ?? {}) : internalFilters;

  const effectiveFilters: ActiveChartFilters = useMemo(() => {
    if (!enableFilters) {
      return {};
    }
    if (!filterDefinitions.length) {
      return {};
    }

    const allowedKeys = new Set(filterDefinitions.map((definition) => definition.key));
    const sanitized: ActiveChartFilters = {};

    Object.entries(baseFilters).forEach(([key, selection]) => {
      if (!selection) return;
      if (!allowedKeys.has(key)) return;

      if (selection.type === 'categorical') {
        if (!selection.values || selection.values.length === 0) return;
        sanitized[key] = {
          type: 'categorical',
          values: Array.from(new Set(selection.values)),
        };
        return;
      }

      if (selection.type === 'date') {
        if (!selection.start && !selection.end) return;
        sanitized[key] = {
          type: 'date',
          start: selection.start,
          end: selection.end,
        };
        return;
      }

      if (selection.type === 'numeric') {
        if (selection.min === undefined && selection.max === undefined) return;
        sanitized[key] = {
          type: 'numeric',
          min: selection.min,
          max: selection.max,
        };
      }
    });

    return sanitized;
  }, [baseFilters, enableFilters, filterDefinitions]);

  const filteredData = useMemo(() => {
    if (!enableFilters) return originalData;
    return applyChartFilters(originalData, effectiveFilters);
  }, [enableFilters, effectiveFilters, originalData]);

  const filtersApplied = enableFilters && hasActiveFilters(effectiveFilters);
  const baseChartData = enableFilters ? filteredData : originalData;
  
  const chartData = baseChartData;
  
  const showNoDataState = chartData.length === 0;

  const activeFilterChips = useMemo(() => {
    if (!enableFilters) return [];
    return filterDefinitions
      .map((definition) => {
        const selection = effectiveFilters[definition.key];
        if (!selection) return null;

        if (selection.type === 'categorical') {
          if (!selection.values || selection.values.length === 0) return null;
          return {
            key: definition.key,
            label: `${definition.label}: ${selection.values.join(', ')}`,
          };
        }

        if (selection.type === 'date') {
          const segments: string[] = [];
          if (selection.start) {
            const formatted = formatDateForDisplay(selection.start) ?? selection.start;
            segments.push(`from ${formatted}`);
          }
          if (selection.end) {
            const formatted = formatDateForDisplay(selection.end) ?? selection.end;
            segments.push(`to ${formatted}`);
          }
          if (segments.length === 0) return null;
          return {
            key: definition.key,
            label: `${definition.label}: ${segments.join(' ')}`,
          };
        }

        if (selection.type === 'numeric') {
          const parts: string[] = [];
          if (selection.min !== undefined) {
            parts.push(`≥ ${formatNumberForDisplay(selection.min)}`);
          }
          if (selection.max !== undefined) {
            parts.push(`≤ ${formatNumberForDisplay(selection.max)}`);
          }
          if (parts.length === 0) return null;
          return {
            key: definition.key,
            label: `${definition.label}: ${parts.join(' & ')}`,
          };
        }

        return null;
      })
      .filter(Boolean) as { key: string; label: string }[];
  }, [enableFilters, effectiveFilters, filterDefinitions]);

  const handleResetFilters = useCallback(() => {
    updateFilters({});
  }, [updateFilters]);

  const handleClearFilterKey = useCallback(
    (key: string) => {
      updateFilters((prev) => {
        const next = { ...prev };
        delete next[key];
        return next;
      });
    },
    [updateFilters]
  );

  const handleToggleCategoricalOption = useCallback(
    (key: string, option: string, checked: boolean) => {
      updateFilters((prev) => {
        const next: ActiveChartFilters = { ...prev };
        const existing = next[key];
        if (!checked) {
          if (existing?.type === 'categorical') {
            const remaining = existing.values.filter((value) => value !== option);
            if (remaining.length > 0) {
              next[key] = { type: 'categorical', values: remaining };
            } else {
              delete next[key];
            }
          }
          return next;
        }

        const values = existing?.type === 'categorical' ? new Set(existing.values) : new Set<string>();
        values.add(option);
        next[key] = { type: 'categorical', values: Array.from(values) };
        return next;
      });
    },
    [updateFilters]
  );

  const handleDateChange = useCallback(
    (key: string, boundary: 'start' | 'end', value?: string) => {
      updateFilters((prev) => {
        const next: ActiveChartFilters = { ...prev };
        const existing = next[key];
        const definition = filterDefinitions.find(
          (candidate): candidate is ChartFilterDefinition & { type: 'date' } =>
            candidate.key === key && candidate.type === 'date'
        );

        let start = boundary === 'start' ? value : existing?.type === 'date' ? existing.start : undefined;
        let end = boundary === 'end' ? value : existing?.type === 'date' ? existing.end : undefined;

        if (definition) {
          if (start) {
            if (definition.min && start < definition.min) start = definition.min;
            if (definition.max && start > definition.max) start = definition.max;
          }
          if (end) {
            if (definition.min && end < definition.min) end = definition.min;
            if (definition.max && end > definition.max) end = definition.max;
          }

          if (start && end && start > end) {
            if (boundary === 'start') {
              end = start;
            } else {
              start = end;
            }
          }
        }

        if (!start && !end) {
          delete next[key];
        } else {
          next[key] = { type: 'date', start, end };
        }

        return next;
      });
    },
    [filterDefinitions, updateFilters]
  );

  const handleNumericBoundsChange = useCallback(
    (definition: ChartFilterDefinition, boundary: 'min' | 'max', value?: number) => {
      if (definition.type !== 'numeric') {
        return;
      }

      updateFilters((prev) => {
        const next: ActiveChartFilters = { ...prev };
        const existing = next[definition.key];
        const currentMin = existing?.type === 'numeric' ? existing.min : undefined;
        const currentMax = existing?.type === 'numeric' ? existing.max : undefined;

        let min = boundary === 'min' ? value : currentMin;
        let max = boundary === 'max' ? value : currentMax;

        if (min !== undefined) {
          min = Math.max(definition.min, Math.min(min, definition.max));
        }
        if (max !== undefined) {
          max = Math.max(definition.min, Math.min(max, definition.max));
        }

        if (min !== undefined && max !== undefined && min > max) {
          if (boundary === 'min') {
            max = min;
          } else {
            min = max;
          }
        }

        const tolerance = determineSliderStep(definition.min, definition.max) / 2;
        const isDefaultMin =
          min === undefined || Math.abs(min - definition.min) <= tolerance;
        const isDefaultMax =
          max === undefined || Math.abs(max - definition.max) <= tolerance;

        if (isDefaultMin && isDefaultMax) {
          delete next[definition.key];
        } else {
          next[definition.key] = {
            type: 'numeric',
            min,
            max,
          };
        }

        return next;
      });
    },
    [updateFilters]
  );

  const handleNumericSliderChange = useCallback(
    (definition: ChartFilterDefinition, values: number[]) => {
      if (definition.type !== 'numeric') {
        return;
      }

      const [rawMin, rawMax] = values;
      let min = Math.max(definition.min, Math.min(rawMin, definition.max));
      let max = Math.max(definition.min, Math.min(rawMax, definition.max));

      if (min > max) {
        const midpoint = (min + max) / 2;
        min = midpoint;
        max = midpoint;
      }

      const tolerance = determineSliderStep(definition.min, definition.max) / 2;

      updateFilters((prev) => {
        const next: ActiveChartFilters = { ...prev };
        if (
          Math.abs(min - definition.min) <= tolerance &&
          Math.abs(max - definition.max) <= tolerance
        ) {
          delete next[definition.key];
        } else {
          next[definition.key] = {
            type: 'numeric',
            min,
            max,
          };
        }
        return next;
      });
    },
    [updateFilters]
  );

  const handleCardClick = useCallback(
    (event: React.MouseEvent<HTMLDivElement>) => {
      const target = event.target as HTMLElement;
      if (target.closest('[data-chart-filter-control="true"]')) {
        return;
      }
      setIsModalOpen(true);
    },
    []
  );

  const renderChart = () => {
    if (showNoDataState) {
      return (
        <div className="flex h-full min-h-[200px] w-full items-center justify-center rounded-md border border-dashed border-border/60 bg-muted/30 px-4 text-sm text-muted-foreground">
          {filtersApplied ? 'No data matches the current filters.' : 'No data available for this chart.'}
        </div>
      );
    }

    switch (type) {
      case 'line':
        // For dual-axis charts, use blue for left axis, red for right axis
        const leftAxisColor = chart.y2 ? '#3b82f6' : chartColor; // Blue for left when dual-axis
        const rightAxisColor = '#ef4444'; // Red for right axis
        const leftValues = getNumericValues(chartData as Record<string, any>[], y);
        const leftDomain = yDomain || getDynamicDomain(leftValues);
        const rightValues = chart.y2 ? getNumericValues(chartData as Record<string, any>[], chart.y2 as string) : [];
        const rightDomain = chart.y2 ? getDynamicDomain(rightValues) : undefined;
        
        return (
          <ResponsiveContainer width="100%" height={fillParent ? '100%' : isSingleChart ? 400 : 250}>
            <LineChart data={chartData} margin={{ left: 50, right: chart.y2 ? 50 : 10, top: 10, bottom: 30 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
              <XAxis
                dataKey={x}
                tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 10 }}
                angle={-45}
                textAnchor="end"
                label={{ value: xLabel || x, position: 'insideBottom', offset: -5, style: { textAnchor: 'middle', fill: 'hsl(var(--foreground))', fontSize: 12, fontWeight: 600 } }}
                height={50}
              />
              {chart.y2 ? (
                <>
              <YAxis
                    tick={{ fill: leftAxisColor, fontSize: 10, fontWeight: 500 }}
                width={60}
                tickFormatter={formatAxisLabel}
                    label={{ value: yLabel || y, angle: -90, position: 'left', style: { textAnchor: 'middle', fill: leftAxisColor, fontSize: 12, fontWeight: 600 } }}
                yAxisId="left"
                    stroke={leftAxisColor}
                domain={leftDomain}
              />
                <YAxis
                  orientation="right"
                  yAxisId="right"
                    tick={{ fill: rightAxisColor, fontSize: 10, fontWeight: 500 }}
                    width={60}
                    tickFormatter={formatAxisLabel}
                    label={{ value: chart.y2Label || chart.y2, angle: 90, position: 'right', style: { textAnchor: 'middle', fill: rightAxisColor, fontSize: 12, fontWeight: 600 } }}
                    stroke={rightAxisColor}
                  domain={rightDomain}
                  />
                </>
              ) : (
                <YAxis
                  tick={{ fill: leftAxisColor, fontSize: 10, fontWeight: 500 }}
                  width={60}
                  tickFormatter={formatAxisLabel}
                  label={{ value: yLabel || y, angle: -90, position: 'left', style: { textAnchor: 'middle', fill: leftAxisColor, fontSize: 12, fontWeight: 600 } }}
                  stroke={leftAxisColor}
                domain={leftDomain}
                />
              )}
              <Tooltip />
              {chart.y2 && (
                <Legend
                  wrapperStyle={{ paddingTop: '10px' }}
                  iconType="line"
                  formatter={(value) => value}
                />
              )}
              <Line
                type="monotone"
                dataKey={y}
                name={chart.y2 ? (yLabel || y) : undefined}
                stroke={leftAxisColor}
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4 }}
                {...(chart.y2 ? { yAxisId: 'left' } : {})}
              />
              {chart.y2 && (
                <>
                  {/* Render single y2 if no y2Series */}
                  {!chart.y2Series && (
                    <Line
                      type="monotone"
                      dataKey={chart.y2 as string}
                      name={chart.y2Label || chart.y2}
                      stroke={rightAxisColor}
                      strokeWidth={2}
                      dot={false}
                      activeDot={{ r: 4 }}
                      yAxisId="right"
                    />
                  )}
                  {/* Render multiple y2Series if present */}
                  {chart.y2Series && chart.y2Series.map((series, index) => {
                    // Use different colors for multiple series
                    const colors = ['#ef4444', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899'];
                    const seriesColor = colors[index % colors.length];
                    return (
                      <Line
                        key={series}
                        type="monotone"
                        dataKey={series}
                        name={series}
                        stroke={seriesColor}
                        strokeWidth={2}
                        dot={false}
                        activeDot={{ r: 4 }}
                        yAxisId="right"
                      />
                    );
                  })}
                </>
              )}
            </LineChart>
          </ResponsiveContainer>
        );

      case 'bar':
        return (
          <ResponsiveContainer width="100%" height={fillParent ? '100%' : isSingleChart ? 400 : 250}>
            <BarChart data={chartData} margin={{ left: 50, right: 10, top: 10, bottom: fillParent ? 120 : isSingleChart ? 100 : 80 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
              <XAxis
                dataKey={x}
                tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 10 }}
                angle={-45}
                textAnchor="end"
                interval={0}
                label={{ value: xLabel || x, position: 'bottom', offset: 10, style: { textAnchor: 'middle', fill: 'hsl(var(--foreground))', fontSize: 12, fontWeight: 600 } }}
                height={fillParent ? 100 : isSingleChart ? 90 : 70}
              />
              <YAxis
                tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 10 }}
                width={60}
                tickFormatter={formatAxisLabel}
                label={{ value: yLabel || y, angle: -90, position: 'left', style: { textAnchor: 'middle', fill: 'hsl(var(--foreground))', fontSize: 12, fontWeight: 600 } }}
              />
              <Tooltip />
              <Bar dataKey={y} fill={chartColor} radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        );

      case 'scatter':
        const getTickCount = (domain: [number, number] | undefined): number => {
          if (!domain) return 6;
          const range = domain[1] - domain[0];
          if (range <= 10) return 6;
          if (range <= 50) return 6;
          if (range <= 100) return 6;
          return 6;
        };

        // Calculate trendline if not provided but we have data
        let trendlineData = trendLine;
        if (!trendlineData && chartData.length > 0) {
          // Calculate linear regression from data points
          const validData = chartData.filter((d: any) => {
            const xVal = typeof d[x] === 'number' ? d[x] : Number(d[x]);
            const yVal = typeof d[y] === 'number' ? d[y] : Number(d[y]);
            return !isNaN(xVal) && !isNaN(yVal);
          });

          if (validData.length > 1) {
              const xValues = validData.map((d: any) => (typeof d[x] === 'number' ? d[x] : Number(d[x])));
              const yValues = validData.map((d: any) => (typeof d[y] === 'number' ? d[y] : Number(d[y])));
            
            // Calculate linear regression
            const n = xValues.length;
            const sumX = xValues.reduce((a, b) => a + b, 0);
            const sumY = yValues.reduce((a, b) => a + b, 0);
            const sumXY = xValues.reduce((sum, xi, i) => sum + xi * yValues[i], 0);
            const sumX2 = xValues.reduce((sum, xi) => sum + xi * xi, 0);
            
            const denominator = n * sumX2 - sumX * sumX;
            if (denominator !== 0) {
              const slope = (n * sumXY - sumX * sumY) / denominator;
              const intercept = (sumY - slope * sumX) / n;
              
              // Calculate domain boundaries from data if not provided
              let xMin: number, xMax: number;
              if (xDomain && typeof xDomain[0] === 'number' && typeof xDomain[1] === 'number') {
                xMin = xDomain[0];
                xMax = xDomain[1];
              } else {
                // Calculate from actual data
                xMin = Math.min(...xValues);
                xMax = Math.max(...xValues);
                // Add a small padding (5% on each side)
                const xPadding = (xMax - xMin) * 0.05;
                xMin = xMin - xPadding;
                xMax = xMax + xPadding;
              }
              
              // Calculate Y values for trendline at domain boundaries
              const yAtMin = slope * xMin + intercept;
              const yAtMax = slope * xMax + intercept;
              
              trendlineData = [
                { [x]: xMin, [y]: yAtMin },
                { [x]: xMax, [y]: yAtMax },
              ];
            }
          }
        }

        // Custom tooltip for scatter to show exact X, Y values
        const renderScatterTooltip = ({ active, payload }: { active?: boolean; payload?: any[] }) => {
          // Some environments don't set `active` reliably; rely on payload presence
          if (!payload || payload.length === 0) return null;
          const p = payload[0]?.payload as any;
          if (!p) return null;
          const xVal = p[x];
          const yVal = p[y];
          return (
            <div style={{ background: 'white', border: '1px solid hsl(var(--border))', borderRadius: 6, padding: '6px 8px', boxShadow: '0 2px 6px rgba(0,0,0,0.08)' }}>
              <div style={{ fontSize: 12, color: 'hsl(var(--muted-foreground))', marginBottom: 4 }}>{xLabel || x}</div>
              <div style={{ fontSize: 12, fontWeight: 600, color: 'hsl(var(--foreground))' }}>{String(xVal)}</div>
              <div style={{ fontSize: 12, color: 'hsl(var(--muted-foreground))', marginTop: 6 }}>{yLabel || y}</div>
              <div style={{ fontSize: 12, fontWeight: 600, color: 'hsl(var(--foreground))' }}>{String(yVal)}</div>
            </div>
          );
        };

        // Use ComposedChart to render scatter with trendline
        return (
          <ResponsiveContainer width="100%" height={fillParent ? '100%' : isSingleChart ? 400 : 250}>
            <ComposedChart data={chartData} margin={{ left: 50, right: 10, top: 10, bottom: 30 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
              <XAxis
                dataKey={x}
                type="number"
                tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 10 }}
                domain={xDomain || ['auto', 'auto']}
                tickFormatter={formatAxisLabel}
                tickCount={getTickCount(xDomain)}
                label={{ value: xLabel || x, position: 'insideBottom', offset: -5, style: { textAnchor: 'middle', fill: 'hsl(var(--foreground))', fontSize: 12, fontWeight: 600 } }}
              />
              <YAxis
                dataKey={y}
                type="number"
                tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 10 }}
                domain={yDomain || ['auto', 'auto']}
                tickFormatter={formatAxisLabel}
                tickCount={getTickCount(yDomain)}
                width={60}
                label={{ value: yLabel || y, angle: -90, position: 'left', style: { textAnchor: 'middle', fill: 'hsl(var(--foreground))', fontSize: 12, fontWeight: 600 } }}
              />
              <Tooltip
                cursor={{ strokeDasharray: '3 3' }}
                formatter={(_value: any, _name: any, props: any) => {
                  const p = (props && props.payload) || {};
                  const yVal = p[y];
                  return [String(yVal), yLabel || y];
                }}
                labelFormatter={(_label: any, payload: any[]) => {
                  const p = payload && payload[0] && payload[0].payload;
                  const xVal = p ? p[x] : '';
                  return `${xLabel || x}: ${String(xVal)}`;
                }}
                content={renderScatterTooltip as any}
              />
              <Scatter name={`${y}`} data={chartData} dataKey={y} fill={chartColor} fillOpacity={0.6} isAnimationActive={false} />
              {trendlineData && trendlineData.length === 2 && (
                <Line
                  type="linear"
                  dataKey={y}
                  data={trendlineData}
                  stroke="#3b82f6"
                  strokeWidth={2}
                  strokeDasharray="5 5"
                  dot={false}
                  activeDot={false}
                  legendType="none"
                  connectNulls={false}
                />
              )}
            </ComposedChart>
          </ResponsiveContainer>
        );

      case 'pie':
        return (
          <ResponsiveContainer width="100%" height={fillParent ? '100%' : isSingleChart ? 400 : 250}>
            <PieChart>
              <Pie
                data={chartData}
                dataKey={y}
                nameKey={x}
                cx="50%"
                cy="50%"
                innerRadius={isSingleChart ? 60 : 40}
                outerRadius={isSingleChart ? 120 : 80}
                label={({ percent }) => `${(percent * 100).toFixed(0)}%`}
              >
                {chartData.map((_: unknown, idx: number) => (
                  <Cell key={`cell-${idx}`} fill={COLORS[idx % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip />
              <Legend 
                verticalAlign="bottom" 
                height={36}
                iconType="circle"
                wrapperStyle={{ fontSize: '12px' }}
              />
            </PieChart>
          </ResponsiveContainer>
        );

      case 'area':
        return (
          <ResponsiveContainer width="100%" height={fillParent ? '100%' : isSingleChart ? 400 : 250}>
            <AreaChart data={chartData} margin={{ left: 50, right: 10, top: 10, bottom: 30 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
              <XAxis
                dataKey={x}
                tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 10 }}
                angle={-45}
                textAnchor="end"
                label={{ value: xLabel || x, position: 'insideBottom', offset: -5, style: { textAnchor: 'middle', fill: 'hsl(var(--foreground))', fontSize: 12, fontWeight: 600 } }}
                height={50}
              />
              <YAxis
                tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 10 }}
                width={60}
                tickFormatter={formatAxisLabel}
                label={{ value: yLabel || y, angle: -90, position: 'left', style: { textAnchor: 'middle', fill: 'hsl(var(--foreground))', fontSize: 12, fontWeight: 600 } }}
              />
              <Tooltip />
              <Area
                type="monotone"
                dataKey={y}
                stroke={chartColor}
                fill={chartColor}
                fillOpacity={0.3}
                strokeWidth={2}
              />
            </AreaChart>
          </ResponsiveContainer>
        );

      default:
        return <p className="text-muted-foreground text-center py-8">Unsupported chart type</p>;
    }
  };

  return (
    <>
      <div className="group relative h-full rounded-lg border border-border bg-card p-4 shadow-sm transition-shadow hover:shadow-md">
        <div
          className={`cursor-pointer ${fillParent ? 'flex h-full flex-col' : ''}`}
          onClick={handleCardClick}
        >
          {!fillParent && (
            <div className="mb-2 flex items-start justify-between gap-2">
              <h3 className="line-clamp-2 text-sm font-semibold text-foreground">{title}</h3>
            </div>
          )}

          {enableFilters && activeFilterChips.length > 0 && (
            <div
              className="mb-3 flex flex-wrap gap-2"
              data-chart-filter-control="true"
              onClick={(event) => event.stopPropagation()}
            >
              {activeFilterChips.map((chip) => (
                <Badge
                  key={chip.key}
                  variant="secondary"
                  className="flex items-center gap-2 rounded-full px-2.5 py-1 text-xs"
                >
                  <span className="max-w-[200px] truncate">{chip.label}</span>
                  <button
                    type="button"
                    className="text-muted-foreground/80 transition hover:text-destructive"
                    onClick={(event) => {
                      event.stopPropagation();
                      handleClearFilterKey(chip.key);
                    }}
                    aria-label={`Remove filter ${chip.label}`}
                  >
                    <X className="h-3 w-3" />
                  </button>
                </Badge>
              ))}
            </div>
          )}

          <div className={`w-full ${fillParent ? 'flex-1 min-h-0' : ''}`}>{renderChart()}</div>
        </div>
        {showAddButton && (
          <Button
            variant="outline"
            size="sm"
            className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity shadow-md z-10"
            onClick={(e) => {
              e.stopPropagation();
              setIsDashboardModalOpen(true);
            }}
          >
            <Plus className="h-4 w-4 mr-1" />
            Add to Dashboard
          </Button>
        )}
      </div>
      {useChartOnlyModal ? (
        <ChartOnlyModal
          isOpen={isModalOpen}
          onClose={() => setIsModalOpen(false)}
          chart={chart}
          enableFilters={enableFilters}
          filterDefinitions={filterDefinitions}
          effectiveFilters={effectiveFilters}
          filtersApplied={filtersApplied}
          chartData={chartData}
          onFiltersChange={updateFilters}
          handleClearFilterKey={handleClearFilterKey}
          handleToggleCategoricalOption={handleToggleCategoricalOption}
          handleDateChange={handleDateChange}
          handleNumericSliderChange={handleNumericSliderChange}
          handleNumericBoundsChange={handleNumericBoundsChange}
          handleResetFilters={handleResetFilters}
          formatDateForDisplay={formatDateForDisplay}
          determineSliderStep={determineSliderStep}
        />
      ) : (
      <ChartModal
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        chart={chart}
        enableFilters={enableFilters}
        filterDefinitions={filterDefinitions}
        effectiveFilters={effectiveFilters}
        filtersApplied={filtersApplied}
        chartData={chartData}
        onFiltersChange={updateFilters}
        handleClearFilterKey={handleClearFilterKey}
        handleToggleCategoricalOption={handleToggleCategoricalOption}
        handleDateChange={handleDateChange}
        handleNumericSliderChange={handleNumericSliderChange}
        handleNumericBoundsChange={handleNumericBoundsChange}
        handleResetFilters={handleResetFilters}
        formatDateForDisplay={formatDateForDisplay}
        determineSliderStep={determineSliderStep}
      />
      )}
      <DashboardModal
        isOpen={isDashboardModalOpen}
        onClose={() => setIsDashboardModalOpen(false)}
        chart={chart}
      />
    </>
  );
}

