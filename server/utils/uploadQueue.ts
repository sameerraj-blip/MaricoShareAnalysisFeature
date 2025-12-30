/**
 * Upload Processing Queue
 * Handles async processing of large file uploads to prevent blocking
 */

interface UploadJob {
  jobId: string;
  sessionId: string;
  username: string;
  fileName: string;
  fileBuffer: Buffer;
  mimeType: string;
  blobInfo?: { blobUrl: string; blobName: string };
  status: 'pending' | 'uploading' | 'parsing' | 'analyzing' | 'saving' | 'completed' | 'failed';
  progress: number; // 0-100
  error?: string;
  result?: any;
  createdAt: number;
  startedAt?: number;
  completedAt?: number;
}

class UploadQueue {
  private jobs: Map<string, UploadJob> = new Map();
  private processing: Set<string> = new Set();
  private readonly MAX_CONCURRENT = 3; // Process max 3 files concurrently
  private readonly MAX_QUEUE_SIZE = 50;

  /**
   * Add a new upload job to the queue
   */
  async enqueue(
    sessionId: string,
    username: string,
    fileName: string,
    fileBuffer: Buffer,
    mimeType: string,
    blobInfo?: { blobUrl: string; blobName: string }
  ): Promise<string> {
    if (this.jobs.size >= this.MAX_QUEUE_SIZE) {
      throw new Error('Upload queue is full. Please try again later.');
    }

    const jobId = `job_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    
    const job: UploadJob = {
      jobId,
      sessionId,
      username,
      fileName,
      fileBuffer,
      mimeType,
      blobInfo,
      status: 'pending',
      progress: 0,
      createdAt: Date.now(),
    };

    this.jobs.set(jobId, job);
    
    // Start processing if not at max capacity
    this.processNext();
    
    return jobId;
  }

  /**
   * Get job status
   */
  getJob(jobId: string): UploadJob | null {
    return this.jobs.get(jobId) || null;
  }

  /**
   * Process next job in queue
   */
  private async processNext(): Promise<void> {
    if (this.processing.size >= this.MAX_CONCURRENT) {
      return; // Already at max capacity
    }

    // Find next pending job
    const pendingJob = Array.from(this.jobs.values()).find(
      job => job.status === 'pending' && !this.processing.has(job.jobId)
    );

    if (!pendingJob) {
      return; // No pending jobs
    }

    this.processing.add(pendingJob.jobId);
    this.processJob(pendingJob).finally(() => {
      this.processing.delete(pendingJob.jobId);
      // Process next job
      this.processNext();
    });
  }

  /**
   * Process a single job
   */
  private async processJob(job: UploadJob): Promise<void> {
    try {
      job.startedAt = Date.now();
      job.status = 'uploading';
      job.progress = 5;

      // Import processing functions dynamically to avoid circular dependencies
      const { parseFile, createDataSummary, convertDashToZeroForNumericColumns } = await import('../lib/fileParser.js');
      const { analyzeUpload } = await import('../lib/dataAnalyzer.js');
      const { generateAISuggestions } = await import('../lib/suggestionGenerator.js');
      const { createChatDocument, generateColumnStatistics } = await import('../models/chat.model.js');
      const { chunkData, clearVectorStore } = await import('../lib/ragService.js');
      const queryCache = (await import('../lib/cache.js')).default;

      // Step 1: Parse file
      job.status = 'parsing';
      job.progress = 15;
      let data = await parseFile(job.fileBuffer, job.fileName);
      
      if (data.length === 0) {
        throw new Error('No data found in file');
      }

      // Step 2: Create data summary
      job.progress = 25;
      const summary = createDataSummary(data);
      data = convertDashToZeroForNumericColumns(data, summary.numericColumns);

      // Step 3: Analyze with AI (this is the slowest part)
      job.status = 'analyzing';
      job.progress = 40;
      const { charts, insights } = await analyzeUpload(data, summary, job.fileName);

      // Step 4: Generate suggestions
      job.progress = 60;
      let suggestions: string[] = [];
      try {
        suggestions = await generateAISuggestions([], summary);
      } catch (suggestionError) {
        console.error('Failed to generate AI suggestions:', suggestionError);
      }

      // Step 5: Sanitize charts (with memory optimization for large datasets)
      job.progress = 70;
      const MAX_CHART_DATA_POINTS = 50000; // Limit chart data to prevent memory issues
      
      const sanitizedCharts = charts.map((chart) => {
        const convertValueForSchema = (value: any): string | number | null => {
          if (value === null || value === undefined) return null;
          if (value instanceof Date) return value.toISOString();
          if (typeof value === 'number') return isNaN(value) || !isFinite(value) ? null : value;
          if (typeof value === 'string') return value;
          return String(value);
        };
        
        let chartData = chart.data || [];
        
        // Limit data size for memory efficiency
        if (chartData.length > MAX_CHART_DATA_POINTS) {
          console.log(`⚠️ Chart "${chart.title}" has ${chartData.length} data points, limiting to ${MAX_CHART_DATA_POINTS} for memory efficiency`);
          // For line/area charts, sample evenly; for others, take first N
          if (chart.type === 'line' || chart.type === 'area') {
            const step = Math.ceil(chartData.length / MAX_CHART_DATA_POINTS);
            chartData = chartData.filter((_: any, idx: number) => idx % step === 0).slice(0, MAX_CHART_DATA_POINTS);
          } else {
            chartData = chartData.slice(0, MAX_CHART_DATA_POINTS);
          }
        }
        
        // Process in batches to avoid memory spikes
        const BATCH_SIZE = 10000;
        const sanitizedData: Record<string, any>[] = [];
        
        for (let i = 0; i < chartData.length; i += BATCH_SIZE) {
          const batch = chartData.slice(i, i + BATCH_SIZE);
          const sanitizedBatch = batch.map(row => {
            const sanitizedRow: Record<string, any> = {};
            for (const [key, value] of Object.entries(row)) {
              sanitizedRow[key] = convertValueForSchema(value);
            }
            return sanitizedRow;
          }).filter(row => {
            return !Object.values(row).some(value => typeof value === 'number' && isNaN(value));
          });
          
          sanitizedData.push(...sanitizedBatch);
        }
        
        return {
          ...chart,
          data: sanitizedData
        };
      });

      // Step 6: Initialize RAG
      job.progress = 75;
      try {
        clearVectorStore(job.sessionId);
        chunkData(data, summary, job.sessionId);
      } catch (ragError) {
        // Silently continue without RAG
      }

      // Step 7: Generate column statistics
      job.progress = 80;
      const columnStatistics = generateColumnStatistics(data, summary.numericColumns);
      
      // Step 8: Prepare sample rows
      const sampleRows = data.slice(0, 50).map(row => {
        const serializedRow: Record<string, any> = {};
        for (const [key, value] of Object.entries(row)) {
          if (value instanceof Date) {
            serializedRow[key] = value.toISOString();
          } else {
            serializedRow[key] = value;
          }
        }
        return serializedRow;
      });

      // Step 9: Save to database
      job.status = 'saving';
      job.progress = 90;
      queryCache.invalidateSession(job.sessionId);
      
      const processingTime = Date.now() - (job.startedAt || Date.now());
      
      let chatDocument;
      try {
        chatDocument = await createChatDocument(
          job.username,
          job.fileName,
          job.sessionId,
          summary,
          sanitizedCharts,
          data,
          sampleRows,
          columnStatistics,
          job.blobInfo,
          {
            totalProcessingTime: processingTime,
            aiModelUsed: 'gpt-4o',
            fileSize: job.fileBuffer.length,
            analysisVersion: '1.0.0'
          },
          insights
        );
      } catch (cosmosError) {
        console.error("Failed to create chat document:", cosmosError);
      }

      // Step 10: Complete
      job.status = 'completed';
      job.progress = 100;
      job.completedAt = Date.now();
      job.result = {
        sessionId: job.sessionId,
        summary,
        charts: sanitizedCharts,
        insights,
        sampleRows,
        suggestions: suggestions.length > 0 ? suggestions : undefined,
        chatId: chatDocument?.id,
        blobInfo: job.blobInfo,
      };

      // Clean up file buffer from memory after processing
      // Note: We can't explicitly free the buffer, but removing the reference helps GC
      // The buffer will be garbage collected when the job is cleaned up
      delete (job as any).fileBuffer;

    } catch (error) {
      job.status = 'failed';
      job.error = error instanceof Error ? error.message : 'Unknown error occurred';
      job.completedAt = Date.now();
      console.error(`Upload job ${job.jobId} failed:`, error);
    }
  }

  /**
   * Clean up old completed/failed jobs (older than 1 hour)
   */
  cleanup(): void {
    const oneHourAgo = Date.now() - 60 * 60 * 1000;
    for (const [jobId, job] of this.jobs.entries()) {
      if (
        (job.status === 'completed' || job.status === 'failed') &&
        job.completedAt &&
        job.completedAt < oneHourAgo
      ) {
        this.jobs.delete(jobId);
      }
    }
  }

  /**
   * Get queue statistics
   */
  getStats() {
    const jobs = Array.from(this.jobs.values());
    return {
      total: jobs.length,
      pending: jobs.filter(j => j.status === 'pending').length,
      processing: jobs.filter(j => ['uploading', 'parsing', 'analyzing', 'saving'].includes(j.status)).length,
      completed: jobs.filter(j => j.status === 'completed').length,
      failed: jobs.filter(j => j.status === 'failed').length,
      active: this.processing.size,
    };
  }
}

// Singleton instance
export const uploadQueue = new UploadQueue();

// Cleanup old jobs every 30 minutes
setInterval(() => {
  uploadQueue.cleanup();
}, 30 * 60 * 1000);

