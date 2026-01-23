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
    const JOB_TIMEOUT = 30 * 60 * 1000; // 30 minutes timeout for large files
    const timeoutId = setTimeout(() => {
      if (job.status !== 'completed' && job.status !== 'failed') {
        job.status = 'failed';
        job.error = 'Processing timeout: File processing took too long. Please try with a smaller file or contact support.';
        job.completedAt = Date.now();
        console.error(`⏱️ Upload job ${job.jobId} timed out after ${JOB_TIMEOUT / 1000 / 60} minutes`);
      }
    }, JOB_TIMEOUT);
    
    try {
      job.startedAt = Date.now();
      job.status = 'uploading';
      job.progress = 5;

      // Import processing functions dynamically to avoid circular dependencies
      const { parseFile, createDataSummary, convertDashToZeroForNumericColumns } = await import('../lib/fileParser.js');
      const { processLargeFile, shouldUseLargeFileProcessing, getDataForAnalysis } = await import('../lib/largeFileProcessor.js');
      const { analyzeUpload } = await import('../lib/dataAnalyzer.js');
      const { generateAISuggestions } = await import('../lib/suggestionGenerator.js');
      const { createChatDocument, generateColumnStatistics, getChatBySessionIdEfficient, updateChatDocument, addMessagesBySessionId } = await import('../models/chat.model.js');
      const { saveChartsToBlob } = await import('../lib/blobStorage.js');
      const queryCache = (await import('../lib/cache.js')).default;

      // Check if file should use large file processing or chunking
      const useLargeFileProcessing = shouldUseLargeFileProcessing(job.fileBuffer.length);
      let useChunking = job.fileBuffer.length >= 10 * 1024 * 1024; // 10MB threshold for chunking
      
      let data: Record<string, any>[];
      let summary: ReturnType<typeof createDataSummary>;
      let storagePath: string | undefined;
      let chunkIndexBlob: { blobName: string; totalChunks: number; totalRows: number } | undefined;

      // Try chunking first for files >= 10MB (faster upload and query)
      if (useChunking) {
        try {
          const { chunkFile } = await import('../lib/chunkingService.js');
          console.log(`📦 File is ${(job.fileBuffer.length / 1024 / 1024).toFixed(2)}MB. Using chunking for faster processing...`);
          
          job.status = 'parsing';
          job.progress = 5;
          
          // Parse file first to get summary (needed for chunking)
          const tempData = await parseFile(job.fileBuffer, job.fileName);
          if (tempData.length === 0) {
            throw new Error('No data found in file');
          }
          
          summary = createDataSummary(tempData);
          const tempDataProcessed = convertDashToZeroForNumericColumns(tempData, summary.numericColumns);
          summary = createDataSummary(tempDataProcessed);
          
          job.progress = 10;
          
          // Chunk the file
          const chunkIndex = await chunkFile(
            job.fileBuffer,
            job.sessionId,
            job.fileName,
            summary,
            (progress) => {
              job.progress = 10 + Math.floor(progress.progress * 0.3); // Use 30% of progress for chunking
              if (progress.message) {
                console.log(`  ${progress.message}`);
              }
            }
          );
          
          chunkIndexBlob = {
            blobName: `chunks/${job.sessionId}/index.json`,
            totalChunks: chunkIndex.totalChunks,
            totalRows: chunkIndex.totalRows,
          };
          
          // OPTIMIZATION: For very large files, load only a sample of chunks for AI analysis
          // This dramatically speeds up processing while maintaining statistical accuracy
          const { loadChunkData } = await import('../lib/chunkingService.js');
          const MAX_ROWS_FOR_AI = 100000; // Load max 100K rows for AI analysis
          const shouldSampleChunks = chunkIndex.totalRows > MAX_ROWS_FOR_AI;
          
          if (shouldSampleChunks) {
            // Load chunks proportionally to get ~100K rows
            const targetChunks = Math.ceil((MAX_ROWS_FOR_AI / chunkIndex.totalRows) * chunkIndex.totalChunks);
            const chunksToLoad = Math.min(targetChunks, chunkIndex.totalChunks);
            const step = Math.floor(chunkIndex.totalChunks / chunksToLoad);
            const sampledChunks = chunkIndex.chunks.filter((_, idx) => idx % step === 0).slice(0, chunksToLoad);
            console.log(`📦 Loading ${sampledChunks.length} of ${chunkIndex.totalChunks} chunks (sampled from ${chunkIndex.totalRows} rows) for faster AI analysis...`);
            data = await loadChunkData(sampledChunks);
            console.log(`✅ Loaded ${data.length} rows (sampled) from ${chunkIndex.totalChunks} chunks (${chunkIndex.totalRows} rows total) for AI analysis`);
          } else {
            console.log(`📦 Loading ALL ${chunkIndex.totalChunks} chunks for full data analysis (${chunkIndex.totalRows} rows total)...`);
            data = await loadChunkData(chunkIndex.chunks); // Load ALL chunks for smaller files
            console.log(`✅ File chunked into ${chunkIndex.totalChunks} chunks (${chunkIndex.totalRows} rows total), loaded ${data.length} rows for AI analysis`);
          }
          job.progress = 40;
        } catch (chunkError) {
          console.warn('⚠️ Chunking failed, falling back to standard processing:', chunkError);
          // Fall through to standard processing
          useChunking = false;
        }
      }

      if (!useChunking && useLargeFileProcessing) {
        // Use streaming and columnar storage for large files
        console.log(`📦 Large file detected (${(job.fileBuffer.length / 1024 / 1024).toFixed(2)}MB). Using streaming pipeline...`);
        
        job.status = 'parsing';
        job.progress = 10;
        
        try {
          const result = await processLargeFile(
            job.fileBuffer,
            job.sessionId,
            job.fileName,
            (progress) => {
              job.progress = progress.progress;
              if (progress.message) {
                console.log(`  ${progress.message}`);
              }
            }
          );
          
          summary = result.summary;
          storagePath = result.storagePath;
          
          // Load ALL data for AI analysis (no sampling - full data integrity)
          console.log(`📊 Loading ALL ${result.rowCount} rows for full data analysis...`);
          data = await getDataForAnalysis(job.sessionId, undefined, undefined); // undefined = no limit, load all
          
          console.log(`✅ Large file processed: ${result.rowCount} rows, using ALL ${data.length} rows for analysis`);
        } catch (largeFileError) {
          const errorMsg = largeFileError instanceof Error ? largeFileError.message : String(largeFileError);
          throw new Error(`Failed to process large file: ${errorMsg}`);
        }
      } else {
        // Use traditional processing for smaller files
        // Step 1: Parse file
        job.status = 'parsing';
        job.progress = 15;
        try {
          data = await parseFile(job.fileBuffer, job.fileName);
        } catch (parseError) {
          const errorMsg = parseError instanceof Error ? parseError.message : String(parseError);
          if (errorMsg.includes('memory') || errorMsg.includes('heap') || errorMsg.includes('too large')) {
            throw new Error('File is too large to parse. Please try with a smaller file (under 100MB) or reduce the number of rows.');
          }
          throw new Error(`Failed to parse file: ${errorMsg}`);
        }
        
        if (data.length === 0) {
          throw new Error('No data found in file');
        }

        // Step 2: Create data summary
        job.progress = 25;
        try {
          summary = createDataSummary(data);
          data = convertDashToZeroForNumericColumns(data, summary.numericColumns);
        } catch (summaryError) {
          const errorMsg = summaryError instanceof Error ? summaryError.message : String(summaryError);
          if (errorMsg.includes('memory') || errorMsg.includes('heap')) {
            throw new Error('File is too large to analyze. Please try with a smaller file or reduce the number of columns.');
          }
          throw new Error(`Failed to create data summary: ${errorMsg}`);
        }
      }

      // Step 3: Analyze with AI (this is the slowest part)
      job.status = 'analyzing';
      job.progress = 40;
      let charts, insights;
      try {
        // OPTIMIZATION: For large files, use aggressive optimizations to speed up upload
        // 1. Skip chart insights generation (saves 4-6 AI calls)
        // 2. Use sampled data for AI analysis instead of full dataset (much faster)
        const shouldSkipChartInsights = useChunking || useLargeFileProcessing || data.length > 50000;
        const shouldUseSampledDataForAI = data.length > 100000; // For very large files, sample data for AI
        
        let dataForAI = data;
        if (shouldUseSampledDataForAI) {
          // Sample data for AI analysis - 50K rows is statistically sufficient
          const MAX_SAMPLE_FOR_AI = 50000;
          const step = Math.floor(data.length / MAX_SAMPLE_FOR_AI);
          const sampled: Record<string, any>[] = [];
          for (let i = 0; i < data.length && sampled.length < MAX_SAMPLE_FOR_AI; i += step) {
            sampled.push(data[i]);
          }
          dataForAI = sampled;
          console.log(`⚡ Performance optimization: Using ${sampled.length} sampled rows (from ${data.length} total) for AI analysis to speed up processing`);
        }
        
        if (shouldSkipChartInsights) {
          console.log(`⚡ Performance optimization: Skipping chart insights generation for large file (${data.length} rows). Insights will be generated on-demand.`);
        }
        
        const result = await analyzeUpload(dataForAI, summary, job.fileName, shouldSkipChartInsights);
        charts = result.charts;
        insights = result.insights;
      } catch (analyzeError) {
        const errorMsg = analyzeError instanceof Error ? analyzeError.message : String(analyzeError);
        if (errorMsg.includes('timeout') || errorMsg.includes('TIMEOUT')) {
          throw new Error('AI analysis timed out. The file is too large or complex. Please try with a smaller file or fewer columns.');
        }
        if (errorMsg.includes('rate limit') || errorMsg.includes('quota')) {
          throw new Error('AI service rate limit reached. Please try again in a few minutes.');
        }
        throw new Error(`AI analysis failed: ${errorMsg}`);
      }

      // Step 4: Generate suggestions (skip for large files to speed up upload)
      job.progress = 60;
      let suggestions: string[] = [];
      const shouldSkipSuggestions = useChunking || useLargeFileProcessing || data.length > 50000;
      if (!shouldSkipSuggestions) {
        try {
          suggestions = await generateAISuggestions([], summary);
        } catch (suggestionError) {
          console.error('Failed to generate AI suggestions:', suggestionError);
        }
      } else {
        console.log(`⚡ Performance optimization: Skipping AI suggestions generation for large file. Suggestions can be generated on-demand.`);
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

      // RAG initialization removed

      // Step 7: Generate column statistics
      job.progress = 80;
      const columnStatistics = generateColumnStatistics(data, summary.numericColumns);
      
      // Step 7.5: Compute detailed data summary statistics (for Data Summary modal)
      let dataSummaryStatistics: any = undefined;
      try {
        const { getDataSummary } = await import('../lib/dataOps/pythonService.js');
        
        // Sample data if too large (same logic as in endpoint)
        let dataForSummary = data;
        const MAX_ROWS_FOR_SUMMARY = 50000;
        if (data.length > MAX_ROWS_FOR_SUMMARY) {
          console.log(`📊 Computing data summary: sampling ${MAX_ROWS_FOR_SUMMARY} rows from ${data.length} total rows`);
          const step = Math.floor(data.length / MAX_ROWS_FOR_SUMMARY);
          const sampledData: Record<string, any>[] = [];
          for (let i = 0; i < data.length && sampledData.length < MAX_ROWS_FOR_SUMMARY; i += step) {
            sampledData.push(data[i]);
          }
          dataForSummary = sampledData;
        }
        
        console.log(`📊 Computing detailed data summary statistics...`);
        const summaryResponse = await getDataSummary(dataForSummary);
        
        // Calculate quality score
        const fullDataRowCount = summary.rowCount;
        const totalCells = summaryResponse.summary.reduce((sum, col) => sum + fullDataRowCount, 0);
        const totalNulls = summaryResponse.summary.reduce((sum, col) => {
          const nullPercentage = col.total_values > 0 ? col.null_values / col.total_values : 0;
          return sum + Math.round(nullPercentage * fullDataRowCount);
        }, 0);
        const nullPercentage = totalCells > 0 ? (totalNulls / totalCells) * 100 : 0;
        const qualityScore = Math.max(0, Math.round(100 - nullPercentage));
        
        // Scale summary statistics to full dataset size
        const scaledSummary = summaryResponse.summary.map(col => {
          const nullPercentage = col.total_values > 0 ? col.null_values / col.total_values : 0;
          const scaledNulls = Math.round(nullPercentage * fullDataRowCount);
          return {
            ...col,
            total_values: fullDataRowCount,
            null_values: scaledNulls,
            non_null_values: fullDataRowCount - scaledNulls,
          };
        });
        
        dataSummaryStatistics = {
          summary: scaledSummary,
          qualityScore,
          computedAt: Date.now(),
        };
        
        console.log(`✅ Data summary statistics computed successfully (quality score: ${qualityScore})`);
      } catch (summaryError) {
        console.error('⚠️ Failed to compute data summary statistics during upload:', summaryError);
        // Don't fail the upload - this is optional
      }
      
      // Step 8: Prepare sample rows
      // For large files, sampleRows are already provided from columnar storage
      // For small files, slice from in-memory data
      let sampleRows: Record<string, any>[];
      if (useLargeFileProcessing) {
        // Get fresh sample from columnar storage
        sampleRows = await getDataForAnalysis(job.sessionId, undefined, 50);
      } else {
        sampleRows = data.slice(0, 50).map(row => {
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
      }

      // Step 9: Save to database
      job.status = 'saving';
      job.progress = 90;
      queryCache.invalidateSession(job.sessionId);
      
      const processingTime = Date.now() - (job.startedAt || Date.now());
      
      let chatDocument;
      try {
        // Check if a placeholder session already exists (created during upload)
        const existingSession = await getChatBySessionIdEfficient(job.sessionId);
        
        if (existingSession) {
          // Update existing placeholder session with full data
          console.log(`🔄 Updating existing placeholder session: ${job.sessionId}`);
          
          // Handle chart storage (same logic as createChatDocument)
          let chartsToStore = sanitizedCharts;
          let chartReferences = existingSession.chartReferences || [];
          
          if (sanitizedCharts && sanitizedCharts.length > 0) {
            const shouldStoreChartsInBlob = sanitizedCharts.some(chart => {
              const chartSize = JSON.stringify(chart).length;
              const hasLargeData = chart.data && Array.isArray(chart.data) && chart.data.length > 1000;
              return chartSize > 100000 || hasLargeData;
            });
            
            if (shouldStoreChartsInBlob) {
              console.log(`📊 Charts have large data arrays. Storing in blob storage...`);
              try {
                chartReferences = await saveChartsToBlob(job.sessionId, sanitizedCharts, job.username);
                chartsToStore = sanitizedCharts.map(chart => ({
                  ...chart,
                  data: undefined, // Remove data array - stored in blob
                })) as any;
                console.log(`✅ Saved ${chartReferences.length} charts to blob storage`);
              } catch (blobError) {
                console.error('⚠️ Failed to save charts to blob, storing in CosmosDB:', blobError);
                chartsToStore = sanitizedCharts; // Fallback
              }
            }
          }
          
          // Estimate rawData size and decide if we should store it
          // For large files processed with columnar storage, never store raw data
          const estimatedSize = useLargeFileProcessing ? Infinity : JSON.stringify(data).length;
          const MAX_DOCUMENT_SIZE = 3 * 1024 * 1024; // 3MB safety margin
          const shouldStoreRawData = !useLargeFileProcessing && estimatedSize < MAX_DOCUMENT_SIZE && data.length < 10000;
          
          if (useLargeFileProcessing) {
            console.log(`📊 Large file: Data stored in columnar format at ${storagePath}. Only sampleRows stored in CosmosDB.`);
          } else if (!shouldStoreRawData) {
            console.log(`⚠️ Large dataset detected (${data.length} rows, ~${(estimatedSize / 1024 / 1024).toFixed(2)}MB). Storing only sampleRows in CosmosDB.`);
          }
          
          chatDocument = {
            ...existingSession,
            dataSummary: summary,
            charts: chartsToStore,
            chartReferences: chartReferences.length > 0 ? chartReferences : undefined,
            rawData: shouldStoreRawData ? data : [],
            sampleRows,
            // Store columnar storage path for large files
            columnarStoragePath: useLargeFileProcessing ? storagePath : undefined,
            // Store chunk index for chunked files
            chunkIndexBlob: chunkIndexBlob,
            columnStatistics,
            dataSummaryStatistics, // Store pre-computed data summary statistics
            insights,
            analysisMetadata: {
              totalProcessingTime: processingTime,
              aiModelUsed: 'gpt-4o',
              fileSize: job.fileBuffer.length,
              analysisVersion: '1.0.0'
            },
            // Update blobInfo if it wasn't set in placeholder
            blobInfo: job.blobInfo || existingSession.blobInfo,
          };
          chatDocument = await updateChatDocument(chatDocument);
          console.log(`✅ Updated session with processed data: ${chatDocument.id}`);
          
          // Create initial assistant message with insights and charts
          // Only add if messages array is empty (placeholder session)
          if (!chatDocument.messages || chatDocument.messages.length === 0) {
            // Use charts with data (not the stripped versions)
            const chartsWithData = sanitizedCharts.filter(chart => chart.data && chart.data.length > 0);
            
            const initialMessage = {
              role: 'assistant' as const,
              content: `Hi! 👋 I've just finished analyzing your data. Here's what I found:\n\n📊 Your dataset has ${summary.rowCount} rows and ${summary.columnCount} columns\n🔢 ${summary.numericColumns.length} numeric columns to work with\n📅 ${summary.dateColumns.length} date columns for time-based analysis\n\nI've created ${sanitizedCharts.length} visualizations and ${insights.length} key insights to get you started. Feel free to ask me anything about your data - I'm here to help! What would you like to explore first?`,
              charts: chartsWithData.slice(0, 5), // Include first 5 charts in message (full data)
              insights: insights,
              timestamp: Date.now(),
            };
            
            try {
              await addMessagesBySessionId(job.sessionId, [initialMessage]);
              console.log(`✅ Added initial assistant message with ${insights.length} insights and ${chartsWithData.length} charts`);
            } catch (messageError) {
              console.error('⚠️ Failed to add initial message (non-critical):', messageError);
              // Non-critical - session is still updated with data
            }
          }
        } else {
          // No placeholder exists, create new session (backward compatibility)
          console.log(`📝 No placeholder found, creating new session: ${job.sessionId}`);
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
            insights,
            dataSummaryStatistics // Pass pre-computed data summary statistics
          );
        }
      } catch (cosmosError) {
        const errorMsg = cosmosError instanceof Error ? cosmosError.message : String(cosmosError);
        console.error("Failed to save chat document:", cosmosError);
        
        // Provide more helpful error messages for common issues
        if (errorMsg.includes('RequestEntityTooLarge') || errorMsg.includes('413') || errorMsg.includes('too large')) {
          throw new Error('File or analysis results are too large to save. Please try with a smaller file or fewer columns.');
        } else if (errorMsg.includes('timeout') || errorMsg.includes('TIMEOUT') || errorMsg.includes('ETIMEDOUT')) {
          throw new Error('Database connection timeout. The file may be too large. Please try with a smaller file.');
        } else if (errorMsg.includes('ECONNREFUSED') || errorMsg.includes('connection')) {
          throw new Error('Database connection failed. Please check your connection and try again.');
        }
        // If it's not a critical error, continue - the document might still be partially saved
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
      
      // Clear timeout on successful completion
      clearTimeout(timeoutId);

    } catch (error) {
      job.status = 'failed';
      job.error = error instanceof Error ? error.message : 'Unknown error occurred';
      job.completedAt = Date.now();
      console.error(`Upload job ${job.jobId} failed:`, error);
      
      // Clear timeout on error
      clearTimeout(timeoutId);
      
      // If it's a memory or timeout error, provide more helpful message
      const errorMessage = error instanceof Error ? error.message : String(error);
      if (errorMessage.includes('timeout') || errorMessage.includes('TIMEOUT')) {
        job.error = 'Processing timeout: The file is too large or processing took too long. Please try with a smaller file or split your data into multiple files.';
      } else if (errorMessage.includes('memory') || errorMessage.includes('Memory') || errorMessage.includes('heap')) {
        job.error = 'Memory error: The file is too large to process. Please try with a smaller file or reduce the number of rows/columns.';
      }
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

