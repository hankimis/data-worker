import { Queue, Worker, Job } from 'bullmq';
import { getRedisConnection } from './connection.js';
import { config } from '../config/index.js';
import { createChildLogger } from '../utils/logger.js';
import { getBrightDataService, BrightDataInput } from '../services/brightdata.js';
import { getDatabaseService } from '../services/database.js';
import { getGoogleSheetsService } from '../services/google-sheets.js';
import { updateJobStats, addActivity } from '../services/heartbeat.js';

const logger = createChildLogger('queue-processor');

// Queue names
export const QUEUE_NAMES = {
  COLLECTION: 'data-collection',
} as const;

// Sheet info for updating collection status
export interface SheetInfo {
  spreadsheetId: string;
  sheetName: string;
  rows: number[];
  itemsPerProfile: number;
}

// Job types
export interface CollectionJobData {
  type: 'profile' | 'hashtag' | 'keyword';
  platform: 'instagram' | 'tiktok';
  targets: string[];
  projectId?: number;
  jobId?: number;
  sheetInfo?: SheetInfo;
}

// Queues
let collectionQueue: Queue<CollectionJobData> | null = null;

// Workers
let collectionWorker: Worker<CollectionJobData> | null = null;

/**
 * Initialize queues
 */
export function initializeQueues(): void {
  const connection = getRedisConnection();
  collectionQueue = new Queue(QUEUE_NAMES.COLLECTION, { connection });
  logger.info('Queues initialized');
}

/**
 * Get collection queue
 */
export function getCollectionQueue(): Queue<CollectionJobData> {
  if (!collectionQueue) {
    throw new Error('Queues not initialized. Call initializeQueues() first.');
  }
  return collectionQueue;
}

/**
 * Process collection job
 */
async function processCollectionJob(job: Job<CollectionJobData>): Promise<void> {
  const { type, platform, targets, projectId, jobId, sheetInfo } = job.data;

  logger.info(
    { jobId: job.id, type, platform, targetCount: targets.length, hasSheetInfo: !!sheetInfo },
    'Processing collection job'
  );

  // Activity 로그
  addActivity('info', 'job', `수집 작업 시작: ${platform} ${type}`, {
    jobId: job.id,
    platform,
    type,
    targetCount: targets.length,
    targets: targets.slice(0, 5), // 처음 5개만 표시
  });

  const brightData = getBrightDataService();
  const database = getDatabaseService();
  const sheetsService = sheetInfo ? getGoogleSheetsService() : null;

  // Heartbeat 상태 업데이트
  updateJobStats({ active: 1 });

  try {
    // Update job status if we have a DB job ID
    if (jobId) {
      await database.updateJobStatus(jobId, 'running');
    }

    // Build inputs based on type
    let inputs: BrightDataInput[];
    if (type === 'profile') {
      inputs = brightData.buildProfileInputs(targets, platform);
    } else if (type === 'hashtag') {
      inputs = brightData.buildHashtagInputs(targets, platform);
    } else {
      inputs = targets.map((keyword) => ({ keyword }));
    }

    // Trigger collection
    const snapshot = await brightData.triggerCollection(platform, inputs);
    await job.updateProgress(10);

    logger.info({ snapshotId: snapshot.snapshot_id }, 'Collection triggered, waiting for results');
    addActivity('info', 'api', `BrightData 수집 요청 완료`, {
      snapshotId: snapshot.snapshot_id,
      platform,
      inputCount: inputs.length,
    });

    // Wait for results
    const results = await brightData.waitForSnapshot(snapshot.snapshot_id, {
      maxWaitMs: 600000, // 10 minutes
      pollIntervalMs: 15000, // 15 seconds
    });

    await job.updateProgress(70);

    // Store results in database
    const { inserted, updated } = await database.upsertContent(platform, results, projectId);
    addActivity('success', 'collection', `콘텐츠 저장 완료: ${inserted}개 신규, ${updated}개 업데이트`, {
      platform,
      inserted,
      updated,
      total: results.length,
    });

    await job.updateProgress(90);

    // Update sheet if sheetInfo provided
    if (sheetInfo && sheetsService) {
      await sheetsService.initialize();

      // 각 타겟별 수집된 릴스 수 계산
      const reelsCounts: Array<{ row: number; reelsCount: number }> = [];
      const uncollectableRows: number[] = [];

      for (let i = 0; i < targets.length; i++) {
        const target = targets[i];
        const row = sheetInfo.rows[i];

        // 해당 타겟의 결과 개수 (author_id 또는 username으로 매칭)
        const targetResults = results.filter(
          (r) =>
            r.author?.username?.toLowerCase() === target.toLowerCase() ||
            r.author?.id?.toLowerCase() === target.toLowerCase()
        );

        if (targetResults.length > 0) {
          reelsCounts.push({ row, reelsCount: targetResults.length });
        } else {
          // 결과가 없으면 수집불가로 표시
          uncollectableRows.push(row);
        }
      }

      // 수집 완료 업데이트
      if (reelsCounts.length > 0) {
        await sheetsService.markReelsCollected(
          sheetInfo.spreadsheetId,
          sheetInfo.sheetName,
          reelsCounts
        );
        logger.info({ count: reelsCounts.length }, 'Updated sheet with reels counts');
      }

      // 수집불가 업데이트
      if (uncollectableRows.length > 0) {
        await sheetsService.markAsUncollectable(
          sheetInfo.spreadsheetId,
          sheetInfo.sheetName,
          uncollectableRows
        );
        logger.info({ count: uncollectableRows.length }, 'Marked uncollectable profiles');
      }
    }

    await job.updateProgress(100);

    logger.info(
      { jobId: job.id, inserted, updated, total: results.length },
      'Collection job completed'
    );
    addActivity('success', 'job', `수집 작업 완료: ${platform} ${type}`, {
      jobId: job.id,
      platform,
      type,
      inserted,
      updated,
      total: results.length,
    });

    // Update DB job status
    if (jobId) {
      await database.updateJobStatus(jobId, 'completed');
    }

    // Heartbeat 상태 업데이트
    updateJobStats({ active: 0, completed: 1, lastJobAt: new Date().toISOString() });
  } catch (error) {
    logger.error({ error, jobId: job.id }, 'Collection job failed');
    addActivity('error', 'job', `수집 작업 실패: ${platform} ${type}`, {
      jobId: job.id,
      platform,
      type,
      error: String(error),
    });

    // 실패 시 시트 상태 초기화
    if (sheetInfo && sheetsService) {
      try {
        await sheetsService.initialize();
        await sheetsService.clearCollectingStatus(
          sheetInfo.spreadsheetId,
          sheetInfo.sheetName,
          sheetInfo.rows
        );
        logger.info('Cleared collecting status after failure');
      } catch (clearError) {
        logger.error({ error: clearError }, 'Failed to clear collecting status');
      }
    }

    if (jobId) {
      await database.updateJobStatus(jobId, 'failed', String(error));
    }

    // Heartbeat 상태 업데이트
    updateJobStats({ active: 0, failed: 1 });

    throw error;
  }
}

/**
 * Start workers
 */
export function startWorkers(): void {
  const connection = getRedisConnection();

  collectionWorker = new Worker(QUEUE_NAMES.COLLECTION, processCollectionJob, {
    connection,
    concurrency: config.worker.concurrency,
  });

  collectionWorker.on('completed', (job) => {
    logger.info({ jobId: job.id }, 'Collection job completed');
  });

  collectionWorker.on('failed', (job, error) => {
    logger.error({ jobId: job?.id, error: error.message }, 'Collection job failed');
  });

  collectionWorker.on('active', () => {
    updateJobStats({ active: 1 });
  });

  logger.info('Workers started');
}

/**
 * Stop workers
 */
export async function stopWorkers(): Promise<void> {
  if (collectionWorker) {
    await collectionWorker.close();
    collectionWorker = null;
  }

  logger.info('Workers stopped');
}

/**
 * Close queues
 */
export async function closeQueues(): Promise<void> {
  if (collectionQueue) {
    await collectionQueue.close();
    collectionQueue = null;
  }

  logger.info('Queues closed');
}

/**
 * Get queue stats
 */
export async function getQueueStats(): Promise<{
  waiting: number;
  active: number;
  completed: number;
  failed: number;
}> {
  if (!collectionQueue) {
    return { waiting: 0, active: 0, completed: 0, failed: 0 };
  }

  const [waiting, active, completed, failed] = await Promise.all([
    collectionQueue.getWaitingCount(),
    collectionQueue.getActiveCount(),
    collectionQueue.getCompletedCount(),
    collectionQueue.getFailedCount(),
  ]);

  return { waiting, active, completed, failed };
}
