import React from 'react';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Checkbox } from '@/components/ui/checkbox';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Slider } from '@/components/ui/slider';
import { Filter, X } from 'lucide-react';
import { ChartSpec } from '@shared/schema';
import { ChartFilterDefinition, ActiveChartFilters } from '@/lib/chartFilters';
import { format as formatDate } from 'date-fns';
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

interface ChartModalProps {
  isOpen: boolean;
  onClose: () => void;
  chart: ChartSpec;
  enableFilters?: boolean;
  filterDefinitions?: ChartFilterDefinition[];
  effectiveFilters?: ActiveChartFilters;
  filtersApplied?: boolean;
  chartData?: Record<string, unknown>[];
  onFiltersChange?: (filters: ActiveChartFilters) => void;
  handleClearFilterKey?: (key: string) => void;
  handleToggleCategoricalOption?: (key: string, option: string, checked: boolean) => void;
  handleDateChange?: (key: string, field: 'start' | 'end', value?: string) => void;
  handleNumericSliderChange?: (definition: ChartFilterDefinition, values: number[]) => void;
  handleNumericBoundsChange?: (definition: ChartFilterDefinition, field: 'min' | 'max', value?: number) => void;
  handleResetFilters?: () => void;
  formatDateForDisplay?: (value?: string) => string | undefined;
  determineSliderStep?: (min: number, max: number) => number;
}

const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4'];

// Smart number formatter for axis labels
const formatAxisLabel = (value: number): string => {
  // Handle very small decimals
  if (Math.abs(value) < 0.01 && value !== 0) {
    return value.toFixed(4);
  }
  
  // Handle decimals
  if (Math.abs(value) < 1000 && value % 1 !== 0) {
    return value.toFixed(2);
  }
  
  // Handle large numbers with K, M, B suffixes
  const absValue = Math.abs(value);
  if (absValue >= 1e9) {
    return (value / 1e9).toFixed(1) + 'B';
  } else if (absValue >= 1e6) {
    return (value / 1e6).toFixed(1) + 'M';
  } else if (absValue >= 1e3) {
    return (value / 1e3).toFixed(1) + 'K';
  }
  
  // Handle integers and small numbers
  return value.toFixed(0);
};

const formatDateForDisplayLocal = (value?: string) => {
  if (!value) return undefined;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return undefined;
  return formatDate(parsed, 'd MMM yyyy');
};

const determineSliderStepLocal = (min: number, max: number) => {
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

export function ChartModal({ 
  isOpen, 
  onClose, 
  chart,
  enableFilters = false,
  filterDefinitions = [],
  effectiveFilters = {},
  filtersApplied = false,
  chartData,
  onFiltersChange,
  handleClearFilterKey,
  handleToggleCategoricalOption,
  handleDateChange,
  handleNumericSliderChange,
  handleNumericBoundsChange,
  handleResetFilters,
  formatDateForDisplay = formatDateForDisplayLocal,
  determineSliderStep = determineSliderStepLocal,
}: ChartModalProps) {
  const { type, title, data: chartDataSource = [], x, y, xDomain, yDomain, trendLine, xLabel, yLabel } = chart;
  const chartColor = COLORS[0]; // Use primary color for modal
  
  // Use filtered data if available, otherwise use original data
  const baseData = enableFilters && Array.isArray(chartData) ? chartData : chartDataSource;
  const data = Array.isArray(baseData) ? baseData : [];

  const renderChart = () => {
    switch (type) {
      case 'line':
        // For dual-axis charts, use blue for left axis, red for right axis
        const leftAxisColor = chart.y2 ? '#3b82f6' : chartColor; // Blue for left when dual-axis
        const rightAxisColor = '#ef4444'; // Red for right axis
        const leftValues = getNumericValues(data as Record<string, any>[], y);
        const leftDomain = yDomain || getDynamicDomain(leftValues);
        const rightValues = chart.y2 ? getNumericValues(data as Record<string, any>[], chart.y2 as string) : [];
        const rightDomain = chart.y2 ? getDynamicDomain(rightValues) : undefined;
        
        return (
          <ResponsiveContainer width="100%" height={400}>
            <LineChart data={data} margin={{ left: 60, right: chart.y2 ? 60 : 20, top: 20, bottom: 40 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
              <XAxis
                dataKey={x}
                tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 12, fontFamily: 'var(--font-mono)' }}
                angle={-45}
                textAnchor="end"
                stroke="hsl(var(--muted-foreground))"
                label={{ value: xLabel || x, position: 'insideBottom', offset: -5, style: { textAnchor: 'middle', fill: 'hsl(var(--foreground))', fontSize: 16, fontWeight: 600 } }}
                height={60}
              />
              {chart.y2 ? (
                <>
              <YAxis
                    tick={{ fill: leftAxisColor, fontSize: 14, fontFamily: 'var(--font-mono)', fontWeight: 500 }}
                    stroke={leftAxisColor}
                tickFormatter={formatAxisLabel}
                width={90}
                    label={{ value: yLabel || y, angle: -90, position: 'left', style: { textAnchor: 'middle', fill: leftAxisColor, fontSize: 16, fontWeight: 600 } }}
                yAxisId="left"
                domain={leftDomain}
              />
                <YAxis
                  orientation="right"
                  yAxisId="right"
                    tick={{ fill: rightAxisColor, fontSize: 14, fontFamily: 'var(--font-mono)', fontWeight: 500 }}
                    stroke={rightAxisColor}
                    tickFormatter={formatAxisLabel}
                    width={90}
                    label={{ value: chart.y2Label || chart.y2, angle: 90, position: 'right', style: { textAnchor: 'middle', fill: rightAxisColor, fontSize: 16, fontWeight: 600 } }}
                  domain={rightDomain}
                  />
                </>
              ) : (
                <YAxis
                  tick={{ fill: leftAxisColor, fontSize: 14, fontFamily: 'var(--font-mono)', fontWeight: 500 }}
                  stroke={leftAxisColor}
                  tickFormatter={formatAxisLabel}
                  width={90}
                  label={{ value: yLabel || y, angle: -90, position: 'left', style: { textAnchor: 'middle', fill: leftAxisColor, fontSize: 16, fontWeight: 600 } }}
                domain={leftDomain}
                />
              )}
              <Tooltip
                contentStyle={{
                  backgroundColor: 'hsl(var(--card))',
                  border: '1px solid hsl(var(--border))',
                  borderRadius: '8px',
                  boxShadow: 'var(--shadow-lg)',
                  fontSize: '14px',
                }}
                labelStyle={{ color: 'hsl(var(--foreground))', fontWeight: 600, fontSize: '14px' }}
                itemStyle={{ color: 'hsl(var(--foreground))', fontSize: '14px' }}
              />
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
                strokeWidth={3}
                dot={{ r: 6 }}
                activeDot={{ r: 8 }}
                {...(chart.y2 ? { yAxisId: "left" } : {})}
              />
              {chart.y2 && (
                <>
                  {!chart.y2Series && (
                    <Line
                      type="monotone"
                      dataKey={chart.y2 as string}
                      name={chart.y2Label || chart.y2}
                      stroke={rightAxisColor}
                      strokeWidth={2}
                      dot={false}
                      activeDot={{ r: 5 }}
                      yAxisId="right"
                    />
                  )}
                  {chart.y2Series && chart.y2Series.map((series, index) => {
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
                        activeDot={{ r: 5 }}
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
          <ResponsiveContainer width="100%" height={400}>
            <BarChart data={data} margin={{ left: 60, right: 20, top: 20, bottom: 80 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
              <XAxis
                dataKey={x}
                tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 11, fontFamily: 'var(--font-mono)' }}
                angle={-45}
                textAnchor="end"
                stroke="hsl(var(--muted-foreground))"
                interval={0}
                label={{ value: xLabel || x, position: 'bottom', offset: 5, style: { textAnchor: 'middle', fill: 'hsl(var(--foreground))', fontSize: 14, fontWeight: 600 } }}
                height={70}
              />
              <YAxis
                tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 14, fontFamily: 'var(--font-mono)' }}
                stroke="hsl(var(--muted-foreground))"
                tickFormatter={formatAxisLabel}
                width={90}
                label={{ value: yLabel || y, angle: -90, position: 'left', style: { textAnchor: 'middle', fill: 'hsl(var(--foreground))', fontSize: 16, fontWeight: 600 } }}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'hsl(var(--card))',
                  border: '1px solid hsl(var(--border))',
                  borderRadius: '8px',
                  boxShadow: 'var(--shadow-lg)',
                  fontSize: '14px',
                }}
                labelStyle={{ color: 'hsl(var(--foreground))', fontWeight: 600, fontSize: '14px' }}
                itemStyle={{ color: 'hsl(var(--foreground))', fontSize: '14px' }}
              />
              <Bar dataKey={y} fill={chartColor} radius={[6, 6, 0, 0]} maxBarSize={60} />
            </BarChart>
          </ResponsiveContainer>
        );

      case 'scatter':
        const getTickCount = (domain: [number, number] | undefined): number => {
          if (!domain) return 8;
          const range = domain[1] - domain[0];
          if (range <= 10) return 10;
          if (range <= 50) return 10;
          if (range <= 100) return 10;
          return 8;
        };

        // Calculate trendline if not provided but we have data
        let trendlineData = trendLine;
        if (!trendlineData && data.length > 0) {
          // Calculate linear regression from data points
          const validData = data.filter((d: any) => {
            const xVal = typeof d[x] === 'number' ? d[x] : Number(d[x]);
            const yVal = typeof d[y] === 'number' ? d[y] : Number(d[y]);
            return !isNaN(xVal) && !isNaN(yVal);
          });

          if (validData.length > 1) {
            const xValues = validData.map((d: any) => typeof d[x] === 'number' ? d[x] : Number(d[x]));
            const yValues = validData.map((d: any) => typeof d[y] === 'number' ? d[y] : Number(d[y]));
            
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

        return (
          <ResponsiveContainer width="100%" height={400}>
            <ComposedChart margin={{ left: 60, right: 20, top: 20, bottom: 40 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
              <XAxis
                dataKey={x}
                type="number"
                tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 12, fontFamily: 'var(--font-mono)' }}
                stroke="hsl(var(--muted-foreground))"
                domain={xDomain || ['auto', 'auto']}
                tickFormatter={formatAxisLabel}
                tickCount={getTickCount(xDomain)}
                height={60}
                label={{ value: xLabel || x, position: 'insideBottom', offset: -5, style: { textAnchor: 'middle', fill: 'hsl(var(--foreground))', fontSize: 16, fontWeight: 600 } }}
              />
              <YAxis
                dataKey={y}
                type="number"
                tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 12, fontFamily: 'var(--font-mono)' }}
                stroke="hsl(var(--muted-foreground))"
                domain={yDomain || ['auto', 'auto']}
                tickFormatter={formatAxisLabel}
                tickCount={getTickCount(yDomain)}
                width={90}
                label={{ value: yLabel || y, angle: -90, position: 'left', style: { textAnchor: 'middle', fill: 'hsl(var(--foreground))', fontSize: 16, fontWeight: 600 } }}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'hsl(var(--card))',
                  border: '1px solid hsl(var(--border))',
                  borderRadius: '8px',
                  boxShadow: 'var(--shadow-lg)',
                  fontSize: '14px',
                }}
                labelStyle={{ color: 'hsl(var(--foreground))', fontWeight: 600, fontSize: '14px' }}
                itemStyle={{ color: 'hsl(var(--foreground))', fontSize: '14px' }}
              />
              <Scatter name={`${x} vs ${y}`} data={data} fill={chartColor} fillOpacity={0.8} />
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
          <ResponsiveContainer width="100%" height={400}>
            <PieChart>
              <Pie
                data={data}
                dataKey={y}
                nameKey={x}
                cx="50%"
                cy="50%"
                innerRadius={60}
                outerRadius={120}
                label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                labelLine={{ stroke: 'hsl(var(--foreground))' }}
              >
                {data.map((_, idx) => (
                  <Cell key={`cell-${idx}`} fill={COLORS[idx % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip
                contentStyle={{
                  backgroundColor: 'hsl(var(--card))',
                  border: '1px solid hsl(var(--border))',
                  borderRadius: '8px',
                  boxShadow: 'var(--shadow-lg)',
                  fontSize: '14px',
                }}
                itemStyle={{ color: 'hsl(var(--foreground))', fontSize: '14px' }}
              />
              <Legend 
                verticalAlign="bottom" 
                height={36}
                iconType="circle"
                wrapperStyle={{ 
                  fontSize: '14px',
                  color: 'hsl(var(--foreground))'
                }}
              />
            </PieChart>
          </ResponsiveContainer>
        );

      case 'area':
        return (
          <ResponsiveContainer width="100%" height={400}>
            <AreaChart data={data} margin={{ left: 60, right: 20, top: 20, bottom: 40 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
              <XAxis
                dataKey={x}
                tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 12, fontFamily: 'var(--font-mono)' }}
                angle={-45}
                textAnchor="end"
                stroke="hsl(var(--muted-foreground))"
                label={{ value: xLabel || x, position: 'insideBottom', offset: -5, style: { textAnchor: 'middle', fill: 'hsl(var(--foreground))', fontSize: 16, fontWeight: 600 } }}
                height={60}
              />
              <YAxis
                tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 14, fontFamily: 'var(--font-mono)' }}
                stroke="hsl(var(--muted-foreground))"
                tickFormatter={formatAxisLabel}
                width={90}
                label={{ value: yLabel || y, angle: -90, position: 'left', style: { textAnchor: 'middle', fill: 'hsl(var(--foreground))', fontSize: 16, fontWeight: 600 } }}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'hsl(var(--card))',
                  border: '1px solid hsl(var(--border))',
                  borderRadius: '8px',
                  boxShadow: 'var(--shadow-lg)',
                  fontSize: '14px',
                }}
                labelStyle={{ color: 'hsl(var(--foreground))', fontWeight: 600, fontSize: '14px' }}
                itemStyle={{ color: 'hsl(var(--foreground))', fontSize: '14px' }}
              />
              <Area
                type="monotone"
                dataKey={y}
                stroke={chartColor}
                fill={chartColor}
                fillOpacity={0.3}
                strokeWidth={3}
              />
            </AreaChart>
          </ResponsiveContainer>
        );

      default:
        return <p className="text-muted-foreground text-center py-8">Unsupported chart type</p>;
    }
  };

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-7xl w-full max-h-[90vh] overflow-hidden [&>button]:hidden">
        <DialogHeader className="flex flex-row items-center justify-between space-y-0 pb-4 gap-4">
          <DialogTitle className="text-xl truncate flex-1 min-w-0">
            {title}
          </DialogTitle>
          <div className="flex items-center gap-2 flex-shrink-0">
            {enableFilters && filterDefinitions.length > 0 && (
              <Popover>
                <PopoverTrigger asChild>
                  <Button
                    variant={filtersApplied ? 'default' : 'outline'}
                    size="sm"
                    className="h-8 gap-1 px-3 text-xs"
                  >
                    <Filter className="h-3.5 w-3.5" />
                    Filters
                    {filtersApplied && (
                      <Badge
                        variant="secondary"
                        className="ml-1 h-4 rounded-full px-1 text-[10px] font-medium leading-none"
                      >
                        {Object.keys(effectiveFilters).length}
                      </Badge>
                    )}
                  </Button>
                </PopoverTrigger>
                <PopoverContent
                  align="end"
                  className="w-80 max-h-[80vh] space-y-4 p-4 overflow-y-auto"
                  sideOffset={8}
                >
                  {filterDefinitions.map((definition) => {
                    const selection = effectiveFilters[definition.key];

                    if (definition.type === 'categorical') {
                      return (
                        <div key={definition.key} className="space-y-2">
                          <div className="flex items-center justify-between">
                            <span className="text-sm font-medium text-foreground">{definition.label}</span>
                            {selection && (
                              <Button
                                variant="ghost"
                                size="sm"
                                className="h-6 px-2 text-xs"
                                onClick={() => handleClearFilterKey?.(definition.key)}
                              >
                                Clear
                              </Button>
                            )}
                          </div>
                          <ScrollArea className="h-52 pr-2">
                            <div className="flex flex-col gap-2">
                              {definition.options.map((option) => {
                                const isChecked =
                                  selection?.type === 'categorical' &&
                                  selection.values.includes(option.value);
                                return (
                                  <label
                                    key={option.value}
                                    className="flex items-center justify-between gap-3 rounded-md border border-border/60 bg-muted/30 px-2 py-1.5"
                                  >
                                    <div className="flex items-center gap-2">
                                      <Checkbox
                                        checked={isChecked}
                                        onCheckedChange={(checked) =>
                                          handleToggleCategoricalOption?.(
                                            definition.key,
                                            option.value,
                                            Boolean(checked)
                                          )
                                        }
                                      />
                                      <span className="text-sm text-foreground">{option.label}</span>
                                    </div>
                                    <span className="text-xs text-muted-foreground">{option.count}</span>
                                  </label>
                                );
                              })}
                            </div>
                          </ScrollArea>
                        </div>
                      );
                    }

                    if (definition.type === 'date') {
                      return (
                        <div key={definition.key} className="space-y-2">
                          <div className="flex items-center justify-between">
                            <span className="text-sm font-medium text-foreground">{definition.label}</span>
                            {selection && (
                              <Button
                                variant="ghost"
                                size="sm"
                                className="h-6 px-2 text-xs"
                                onClick={() => handleClearFilterKey?.(definition.key)}
                              >
                                Clear
                              </Button>
                            )}
                          </div>
                          <div className="grid grid-cols-1 gap-3">
                            {definition.min && definition.max && (
                              <p className="text-xs text-muted-foreground">
                                {formatDateForDisplay(definition.min) ?? definition.min} –{' '}
                                {formatDateForDisplay(definition.max) ?? definition.max}
                              </p>
                            )}
                            <div className="flex flex-col gap-1">
                              <Label
                                htmlFor={`filter-${definition.key}-start`}
                                className="text-xs font-medium uppercase tracking-wide text-muted-foreground"
                              >
                                Start
                              </Label>
                              <Input
                                id={`filter-${definition.key}-start`}
                                type="date"
                                value={selection?.type === 'date' && selection.start ? selection.start : ''}
                                min={definition.min}
                                max={
                                  selection?.type === 'date' && selection.end
                                    ? selection.end
                                    : definition.max
                                }
                                onChange={(event) =>
                                  handleDateChange?.(
                                    definition.key,
                                    'start',
                                    event.target.value ? event.target.value : undefined
                                  )
                                }
                              />
                            </div>
                            <div className="flex flex-col gap-1">
                              <Label
                                htmlFor={`filter-${definition.key}-end`}
                                className="text-xs font-medium uppercase tracking-wide text-muted-foreground"
                              >
                                End
                              </Label>
                              <Input
                                id={`filter-${definition.key}-end`}
                                type="date"
                                value={selection?.type === 'date' && selection.end ? selection.end : ''}
                                min={
                                  selection?.type === 'date' && selection.start
                                    ? selection.start
                                    : definition.min
                                }
                                max={definition.max}
                                onChange={(event) =>
                                  handleDateChange?.(
                                    definition.key,
                                    'end',
                                    event.target.value ? event.target.value : undefined
                                  )
                                }
                              />
                            </div>
                          </div>
                        </div>
                      );
                    }

                    if (definition.type === 'numeric') {
                      const numericSelection = selection?.type === 'numeric' ? selection : undefined;
                      const currentMin =
                        numericSelection?.min !== undefined ? numericSelection.min : definition.min;
                      const currentMax =
                        numericSelection?.max !== undefined ? numericSelection.max : definition.max;
                      const step = determineSliderStep(definition.min, definition.max);

                      return (
                        <div key={definition.key} className="space-y-2">
                          <div className="flex items-center justify-between">
                            <span className="text-sm font-medium text-foreground">{definition.label}</span>
                            {numericSelection && (
                              <Button
                                variant="ghost"
                                size="sm"
                                className="h-6 px-2 text-xs"
                                onClick={() => handleClearFilterKey?.(definition.key)}
                              >
                                Clear
                              </Button>
                            )}
                          </div>
                          <div className="space-y-3">
                            <Slider
                              value={[currentMin, currentMax]}
                              min={definition.min}
                              max={definition.max}
                              step={step}
                              onValueChange={(values) => handleNumericSliderChange?.(definition, values)}
                            />
                            <div className="grid grid-cols-2 gap-2">
                              <div className="flex flex-col gap-1">
                                <Label className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                                  Min
                                </Label>
                                <Input
                                  type="number"
                                  value={
                                    numericSelection?.min !== undefined
                                      ? String(numericSelection.min)
                                      : ''
                                  }
                                  placeholder={definition.min.toString()}
                                  onChange={(event) => {
                                    const raw = event.target.value.trim();
                                    if (raw === '') {
                                      handleNumericBoundsChange?.(definition, 'min', undefined);
                                      return;
                                    }
                                    const parsed = Number(raw);
                                    if (Number.isNaN(parsed)) return;
                                    handleNumericBoundsChange?.(definition, 'min', parsed);
                                  }}
                                />
                              </div>
                              <div className="flex flex-col gap-1">
                                <Label className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                                  Max
                                </Label>
                                <Input
                                  type="number"
                                  value={
                                    numericSelection?.max !== undefined
                                      ? String(numericSelection.max)
                                      : ''
                                  }
                                  placeholder={definition.max.toString()}
                                  onChange={(event) => {
                                    const raw = event.target.value.trim();
                                    if (raw === '') {
                                      handleNumericBoundsChange?.(definition, 'max', undefined);
                                      return;
                                    }
                                    const parsed = Number(raw);
                                    if (Number.isNaN(parsed)) return;
                                    handleNumericBoundsChange?.(definition, 'max', parsed);
                                  }}
                                />
                              </div>
                            </div>
                          </div>
                        </div>
                      );
                    }

                    return null;
                  })}

                  <div className="flex items-center justify-between pt-2">
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-8 px-3 text-xs"
                      onClick={() => handleResetFilters?.()}
                      disabled={!filtersApplied}
                    >
                      Reset filters
                    </Button>
                    <span className="text-xs text-muted-foreground">{data.length} rows</span>
                  </div>
                </PopoverContent>
              </Popover>
            )}
            <Button
              variant="ghost"
              size="icon"
              onClick={onClose}
              className="h-8 w-8"
            >
              <X className="h-4 w-4" />
            </Button>
          </div>
        </DialogHeader>
        
        <div className="flex-1 overflow-hidden">
          <div className="flex gap-6 h-[500px]">
            {/* Left side - Chart */}
            <div className="flex-1 min-w-0">
              <div className="h-full w-full">
                {renderChart()}
              </div>
            </div>
            
            {/* Right side - Insights */}
            <div className="w-80 flex-shrink-0 overflow-y-auto">
              <div className="space-y-4">
                {/* Key Insight */}
                {chart.keyInsight && (
                  <div className="bg-blue-50 dark:bg-blue-950/20 rounded-lg p-4 border border-blue-200 dark:border-blue-800">
                    <div className="flex items-start gap-3">
                      <div className="w-3 h-3 bg-blue-500 rounded-full mt-1 flex-shrink-0"></div>
                      <div className="flex-1 min-w-0 overflow-hidden">
                          <h3 className="text-sm font-semibold text-blue-900 dark:text-blue-100 mb-2">Key Insight</h3>
                          <div 
                            className="max-h-32 overflow-y-auto scrollbar-thin scrollbar-thumb-gray-300 scrollbar-track-gray-100 hover:scrollbar-thumb-gray-400"
                            onWheel={(e) => {
                              e.stopPropagation();
                              const element = e.currentTarget;
                              element.scrollTop += e.deltaY;
                            }}
                          >
                          <p className="text-sm text-blue-800 dark:text-blue-200 leading-relaxed break-words">{chart.keyInsight}</p>
                          </div>
                        </div>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}

