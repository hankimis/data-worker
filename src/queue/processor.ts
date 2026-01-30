import { Queue, Worker, Job } from 'bullmq';
import { getRedisConnection } from './connection.js';
import { config } from '../config/index.js';
import { createChildLogger } from '../utils/logger.js';
import { getBrightDataService, BrightDataInput } from '../services/brightdata.js';
import { getDatabaseService } from '../services/database.js';

const logger = createChildLogger('queue-processor');

// Queue names
export const QUEUE_NAMES = {
  COLLECTION: 'data-collection',
  SYNC_SHEETS: 'sync-sheets',
} as const;

// Job types
export interface CollectionJobData {
  type: 'profile' | 'hashtag' | 'keyword';
  platform: 'instagram' | 'tiktok';
  targets: string[];
  projectId?: number;
  jobId?: number;
}

export interface SyncSheetsJobData {
  spreadsheetId: string;
  range: string;
}

// Queues
let collectionQueue: Queue<CollectionJobData> | null = null;
let syncSheetsQueue: Queue<SyncSheetsJobData> | null = null;

// Workers
let collectionWorker: Worker<CollectionJobData> | null = null;
let syncSheetsWorker: Worker<SyncSheetsJobData> | null = null;

/**
 * Initialize queues
 */
export function initializeQueues(): void {
  const connection = getRedisConnection();

  collectionQueue = new Queue(QUEUE_NAMES.COLLECTION, { connection });
  syncSheetsQueue = new Queue(QUEUE_NAMES.SYNC_SHEETS, { connection });

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
 * Get sync sheets queue
 */
export function getSyncSheetsQueue(): Queue<SyncSheetsJobData> {
  if (!syncSheetsQueue) {
    throw new Error('Queues not initialized. Call initializeQueues() first.');
  }
  return syncSheetsQueue;
}

/**
 * Process collection job
 */
async function processCollectionJob(job: Job<CollectionJobData>): Promise<void> {
  const { type, platform, targets, projectId, jobId } = job.data;

  logger.info({ jobId: job.id, type, platform, targetCount: targets.length }, 'Processing collection job');

  const brightData = getBrightDataService();
  const database = getDatabaseService();

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
      // keyword search - platform specific
      inputs = targets.map((keyword) => ({ keyword }));
    }

    // Trigger collection
    const snapshot = await brightData.triggerCollection(platform, inputs);

    // Update job progress
    await job.updateProgress(10);

    // Wait for results
    const results = await brightData.waitForSnapshot(snapshot.snapshot_id, {
      maxWaitMs: 600000, // 10 minutes
      pollIntervalMs: 15000, // 15 seconds
    });

    await job.updateProgress(70);

    // Store results in database
    const { inserted, updated } = await database.upsertContent(platform, results, projectId);

    await job.updateProgress(100);

    logger.info(
      { jobId: job.id, inserted, updated, total: results.length },
      'Collection job completed'
    );

    // Update DB job status
    if (jobId) {
      await database.updateJobStatus(jobId, 'completed');
    }
  } catch (error) {
    logger.error({ error, jobId: job.id }, 'Collection job failed');

    if (jobId) {
      await database.updateJobStatus(jobId, 'failed', String(error));
    }

    throw error;
  }
}

/**
 * Process sync sheets job
 */
async function processSyncSheetsJob(job: Job<SyncSheetsJobData>): Promise<void> {
  const { spreadsheetId, range } = job.data;

  logger.info({ jobId: job.id, spreadsheetId, range }, 'Processing sync sheets job');

  const { getGoogleSheetsService } = await import('../services/google-sheets.js');
  const sheetsService = getGoogleSheetsService();

  try {
    await sheetsService.initialize();

    const entries = await sheetsService.fetchIdList(spreadsheetId, range);

    // Group by platform and type
    const grouped = entries.reduce(
      (acc, entry) => {
        const key = `${entry.platform}-${entry.type}`;
        if (!acc[key]) {
          acc[key] = [];
        }
        acc[key].push(entry);
        return acc;
      },
      {} as Record<string, typeof entries>
    );

    // Queue collection jobs for each group
    const collectionQueue = getCollectionQueue();

    for (const [key, groupEntries] of Object.entries(grouped)) {
      const [platform, type] = key.split('-') as ['instagram' | 'tiktok', 'profile' | 'hashtag' | 'keyword'];

      await collectionQueue.add(
        `collect-${key}`,
        {
          type,
          platform,
          targets: groupEntries.map((e) => e.id),
          projectId: groupEntries[0]?.projectId,
        },
        {
          attempts: config.worker.maxRetries,
          backoff: {
            type: 'exponential',
            delay: 5000,
          },
        }
      );
    }

    logger.info({ entriesCount: entries.length, groups: Object.keys(grouped).length }, 'Sync sheets job completed');
  } catch (error) {
    logger.error({ error, jobId: job.id }, 'Sync sheets job failed');
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

  syncSheetsWorker = new Worker(QUEUE_NAMES.SYNC_SHEETS, processSyncSheetsJob, {
    connection,
    concurrency: 1, // Process one at a time
  });

  syncSheetsWorker.on('completed', (job) => {
    logger.info({ jobId: job.id }, 'Sync sheets job completed');
  });

  syncSheetsWorker.on('failed', (job, error) => {
    logger.error({ jobId: job?.id, error: error.message }, 'Sync sheets job failed');
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

  if (syncSheetsWorker) {
    await syncSheetsWorker.close();
    syncSheetsWorker = null;
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

  if (syncSheetsQueue) {
    await syncSheetsQueue.close();
    syncSheetsQueue = null;
  }

  logger.info('Queues closed');
}
