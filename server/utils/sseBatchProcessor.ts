/**
 * Batch processor for SSE queries
 * Groups queries by type and executes them in batches
 */

interface BatchedQuery {
  userEmail: string;
  type: 'sharedAnalyses' | 'sharedDashboards';
  resolve: (data: any) => void;
  reject: (error: any) => void;
}

class SSEBatchProcessor {
  private batchQueue: BatchedQuery[] = [];
  private batchTimeout: NodeJS.Timeout | null = null;
  private readonly BATCH_SIZE = 10;
  private readonly BATCH_DELAY = 100; // 100ms

  async queueQuery(
    userEmail: string,
    type: 'sharedAnalyses' | 'sharedDashboards'
  ): Promise<any> {
    return new Promise((resolve, reject) => {
      this.batchQueue.push({ userEmail, type, resolve, reject });
      
      if (this.batchQueue.length >= this.BATCH_SIZE) {
        this.processBatch();
      } else if (!this.batchTimeout) {
        this.batchTimeout = setTimeout(() => {
          this.processBatch();
        }, this.BATCH_DELAY);
      }
    });
  }

  private async processBatch(): Promise<void> {
    if (this.batchTimeout) {
      clearTimeout(this.batchTimeout);
      this.batchTimeout = null;
    }

    const batch = this.batchQueue.splice(0, this.BATCH_SIZE);
    if (batch.length === 0) return;

    // Group by type
    const analysesBatch = batch.filter(q => q.type === 'sharedAnalyses');
    const dashboardsBatch = batch.filter(q => q.type === 'sharedDashboards');

    // Process in parallel
    await Promise.all([
      this.processAnalysesBatch(analysesBatch),
      this.processDashboardsBatch(dashboardsBatch),
    ]);
  }

  private async processAnalysesBatch(batch: BatchedQuery[]): Promise<void> {
    const { listSharedAnalysesForUser } = await import('../models/sharedAnalysis.model.js');
    
    await Promise.all(
      batch.map(async (query) => {
        try {
          const data = await listSharedAnalysesForUser(query.userEmail);
          query.resolve(data);
        } catch (error) {
          query.reject(error);
        }
      })
    );
  }

  private async processDashboardsBatch(batch: BatchedQuery[]): Promise<void> {
    const { listSharedDashboardsForUser } = await import('../models/sharedDashboard.model.js');
    
    await Promise.all(
      batch.map(async (query) => {
        try {
          const data = await listSharedDashboardsForUser(query.userEmail);
          query.resolve(data);
        } catch (error) {
          query.reject(error);
        }
      })
    );
  }
}

export const sseBatchProcessor = new SSEBatchProcessor();

