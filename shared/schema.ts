import { z } from "zod";

// Chart Specifications
export const chartSpecSchema = z.object({
  type: z.enum(["line", "bar", "scatter", "pie", "area"]),
  title: z.string(),
  x: z.string(),
  y: z.string(),
  // Optional secondary Y series for dual-axis line charts
  y2: z.string().optional(),
  // Optional array of additional Y series for multi-series charts on right axis
  y2Series: z.array(z.string()).optional(),
  xLabel: z.string().optional(),
  yLabel: z.string().optional(),
  y2Label: z.string().optional(),
  aggregate: z.enum(["sum", "mean", "count", "none"]).optional(),
  data: z.array(z.record(z.union([z.string(), z.number()]))).optional(),
  xDomain: z.tuple([z.number(), z.number()]).optional(), // [min, max] for X-axis
  yDomain: z.tuple([z.number(), z.number()]).optional(), // [min, max] for Y-axis
  trendLine: z.array(z.record(z.union([z.string(), z.number()]))).optional(), // Two points defining the trend line: [{ [x]: min, [y]: y1 }, { [x]: max, [y]: y2 }]
  keyInsight: z.string().optional(), // Key insight about the chart
});

export type ChartSpec = z.infer<typeof chartSpecSchema>;

// Insights
export const insightSchema = z.object({
  id: z.number(),
  text: z.string(),
});

export type Insight = z.infer<typeof insightSchema>;

// Thinking Steps (for real-time processing display)
export const thinkingStepSchema = z.object({
  step: z.string(),
  status: z.enum(["pending", "active", "completed", "error"]),
  timestamp: z.number(),
  details: z.string().optional(),
});

export type ThinkingStep = z.infer<typeof thinkingStepSchema>;

// Chat Messages
export const messageSchema = z.object({
  role: z.enum(["user", "assistant"]),
  content: z.string(),
  charts: z.array(chartSpecSchema).optional(),
  insights: z.array(insightSchema).optional(),
  timestamp: z.number(),
  thinkingSteps: z.array(thinkingStepSchema).optional(), // Temporary thinking steps shown during processing
});

export type Message = z.infer<typeof messageSchema>;

// Data Summary
export const dataSummarySchema = z.object({
  rowCount: z.number(),
  columnCount: z.number(),
  columns: z.array(z.object({
    name: z.string(),
    type: z.string(),
    sampleValues: z.array(z.union([z.string(), z.number(), z.null()])),
  })),
  numericColumns: z.array(z.string()),
  dateColumns: z.array(z.string()),
});

export type DataSummary = z.infer<typeof dataSummarySchema>;

// Column Statistics Schema
export const columnStatisticsSchema = z.object({
  count: z.number(),
  min: z.number(),
  max: z.number(),
  sum: z.number(),
  mean: z.number(),
  median: z.number(),
  standardDeviation: z.number(),
  q1: z.number(),
  q3: z.number(),
  range: z.number(),
  variance: z.number(),
});

export type ColumnStatistics = z.infer<typeof columnStatisticsSchema>;

// Analysis Metadata Schema
export const analysisMetadataSchema = z.object({
  totalProcessingTime: z.number(),
  aiModelUsed: z.string(),
  fileSize: z.number(),
  analysisVersion: z.string(),
});

export type AnalysisMetadata = z.infer<typeof analysisMetadataSchema>;

// Complete Analysis Data Schema
export const completeAnalysisDataSchema = z.object({
  id: z.string(),
  fileName: z.string(),
  uploadedAt: z.number(),
  createdAt: z.number(),
  lastUpdatedAt: z.number(),
  dataSummary: dataSummarySchema,
  rawData: z.array(z.record(z.union([z.string(), z.number(), z.null()]))),
  sampleRows: z.array(z.record(z.union([z.string(), z.number(), z.null()]))),
  columnStatistics: z.record(z.string(), columnStatisticsSchema),
  charts: z.array(chartSpecSchema),
  insights: z.array(insightSchema),
  messages: z.array(messageSchema),
  
  // ✅ New nested chat storage format
  chatThread: z.array(z.object({
    charts: z.array(z.object({
      chart: chartSpecSchema,
      keyInsight: z.string().optional(),
    })),
    messageInsight: z.string().optional(),
  })).optional(),

  blobInfo: z.object({
    blobUrl: z.string(),
    blobName: z.string(),
  }).optional(),

  analysisMetadata: analysisMetadataSchema,
  sessionId: z.string(),
});


export type CompleteAnalysisData = z.infer<typeof completeAnalysisDataSchema>;

// Analysis Session Summary Schema
export const analysisSessionSummarySchema = z.object({
  id: z.string(),
  fileName: z.string(),
  uploadedAt: z.number(),
  createdAt: z.number(),
  lastUpdatedAt: z.number(),
  dataSummary: dataSummarySchema,
  chartsCount: z.number(),
  insightsCount: z.number(),
  messagesCount: z.number(),
  blobInfo: z.object({
    blobUrl: z.string(),
    blobName: z.string(),
  }).optional(),
  analysisMetadata: analysisMetadataSchema,
  sessionId: z.string(),
});

export type AnalysisSessionSummary = z.infer<typeof analysisSessionSummarySchema>;

// API Response Types
export const uploadResponseSchema = z.object({
  sessionId: z.string(),
  summary: dataSummarySchema,
  charts: z.array(chartSpecSchema),
  insights: z.array(insightSchema),
  sampleRows: z.array(z.record(z.union([z.string(), z.number(), z.null()]))).optional(),
  chatId: z.string().optional(), // CosmosDB chat document ID
  blobInfo: z.object({
    blobUrl: z.string(),
    blobName: z.string(),
  }).optional(), // Azure Blob Storage info
});

export type UploadResponse = z.infer<typeof uploadResponseSchema>;

export const chatResponseSchema = z.object({
  answer: z.string(),
  charts: z.array(chartSpecSchema).optional(),
  insights: z.array(insightSchema).optional(),
  suggestions: z.array(z.string()).optional(),
});

export type ChatResponse = z.infer<typeof chatResponseSchema>;

// Data Retrieval Response Schemas
export const userAnalysisSessionsResponseSchema = z.object({
  sessions: z.array(analysisSessionSummarySchema),
  totalCount: z.number(),
});

export type UserAnalysisSessionsResponse = z.infer<typeof userAnalysisSessionsResponseSchema>;

export const columnStatisticsResponseSchema = z.object({
  chatId: z.string(),
  fileName: z.string(),
  columnStatistics: z.record(z.string(), columnStatisticsSchema),
  numericColumns: z.array(z.string()),
  totalNumericColumns: z.number(),
});

export type ColumnStatisticsResponse = z.infer<typeof columnStatisticsResponseSchema>;

export const rawDataResponseSchema = z.object({
  chatId: z.string(),
  fileName: z.string(),
  data: z.array(z.record(z.union([z.string(), z.number(), z.null()]))),
  pagination: z.object({
    page: z.number(),
    limit: z.number(),
    totalRows: z.number(),
    totalPages: z.number(),
    hasNextPage: z.boolean(),
    hasPrevPage: z.boolean(),
  }),
});

export type RawDataResponse = z.infer<typeof rawDataResponseSchema>;

// Session Storage (backend only)
export interface SessionData {
  data: Record<string, any>[];
  summary: DataSummary;
  fileName: string;
  uploadedAt: number;
}

// Dashboards
export const dashboardSheetSchema = z.object({
  id: z.string(),
  name: z.string().min(1),
  charts: z.array(chartSpecSchema),
  order: z.number().optional(),
});

export type DashboardSheet = z.infer<typeof dashboardSheetSchema>;

export const dashboardSchema = z.object({
  id: z.string(),
  username: z.string(),
  name: z.string(),
  createdAt: z.number(),
  updatedAt: z.number(),
  lastOpenedAt: z.number().optional(), // Track when dashboard was last accessed
  charts: z.array(chartSpecSchema), // Keep for backward compatibility
  sheets: z.array(dashboardSheetSchema).optional(), // New: multiple sheets
});

export type Dashboard = z.infer<typeof dashboardSchema>;

export const createDashboardRequestSchema = z.object({
  name: z.string().min(1),
  charts: z.array(chartSpecSchema).optional(),
});

export const addChartToDashboardRequestSchema = z.object({
  chart: chartSpecSchema,
  sheetId: z.string().optional(), // Optional: specify which sheet to add to
});

export const removeChartFromDashboardRequestSchema = z.object({
  index: z.number().optional(),
  title: z.string().optional(),
  type: z.enum(["line", "bar", "scatter", "pie", "area"]).optional(),
  sheetId: z.string().optional(), // Optional: specify which sheet to remove from
}).refine((data) => data.index !== undefined || data.title !== undefined || data.type !== undefined, {
  message: "Provide index or title/type to remove a chart",
});
