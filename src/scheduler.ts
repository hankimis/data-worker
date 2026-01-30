import { getSyncSheetsQueue, getCollectionQueue } from './queue/processor.js';
import { getDatabaseService } from './services/database.js';
import { config } from './config/index.js';
import { createChildLogger } from './utils/logger.js';

const logger = createChildLogger('scheduler');

interface ScheduledSheet {
  spreadsheetId: string;
  range: string;
  intervalMs: number;
}

// Configure your Google Sheets here
const SCHEDULED_SHEETS: ScheduledSheet[] = [
  // Example: sync a sheet every hour
  // {
  //   spreadsheetId: 'your-spreadsheet-id',
  //   range: 'Sheet1!A1:D100',
  //   intervalMs: 60 * 60 * 1000, // 1 hour
  // },
];

let schedulerIntervals: NodeJS.Timeout[] = [];
let dbPollInterval: NodeJS.Timeout | null = null;

/**
 * Schedule Google Sheets sync jobs
 */
export function startSheetsSyncScheduler(): void {
  const syncQueue = getSyncSheetsQueue();

  for (const sheet of SCHEDULED_SHEETS) {
    logger.info(
      { spreadsheetId: sheet.spreadsheetId, intervalMs: sheet.intervalMs },
      'Scheduling sheets sync'
    );

    // Run immediately
    syncQueue.add('sync-sheets', {
      spreadsheetId: sheet.spreadsheetId,
      range: sheet.range,
    });

    // Then schedule recurring
    const interval = setInterval(() => {
      syncQueue.add('sync-sheets', {
        spreadsheetId: sheet.spreadsheetId,
        range: sheet.range,
      });
    }, sheet.intervalMs);

    schedulerIntervals.push(interval);
  }

  logger.info({ count: SCHEDULED_SHEETS.length }, 'Sheets sync scheduler started');
}

/**
 * Poll database for pending collection jobs
 */
export function startDatabasePollScheduler(): void {
  const pollInterval = config.worker.pollIntervalMs;

  logger.info({ pollIntervalMs: pollInterval }, 'Starting database poll scheduler');

  const pollJobs = async () => {
    try {
      const database = getDatabaseService();
      const collectionQueue = getCollectionQueue();

      const pendingJobs = await database.getPendingJobs();

      if (pendingJobs.length > 0) {
        logger.info({ count: pendingJobs.length }, 'Found pending jobs');

        for (const job of pendingJobs) {
          await collectionQueue.add(
            `db-job-${job.id}`,
            {
              type: job.job_type as 'profile' | 'hashtag' | 'keyword',
              platform: job.platform as 'instagram' | 'tiktok',
              targets: [job.target_identifier],
              projectId: job.project_id ?? undefined,
              jobId: job.id,
            },
            {
              jobId: `db-job-${job.id}`,
              attempts: config.worker.maxRetries,
              backoff: {
                type: 'exponential',
                delay: 5000,
              },
            }
          );
        }
      }
    } catch (error) {
      logger.error({ error }, 'Failed to poll database for jobs');
    }
  };

  // Run immediately
  pollJobs();

  // Then schedule recurring
  dbPollInterval = setInterval(pollJobs, pollInterval);
}

/**
 * Stop all schedulers
 */
export function stopSchedulers(): void {
  for (const interval of schedulerIntervals) {
    clearInterval(interval);
  }
  schedulerIntervals = [];

  if (dbPollInterval) {
    clearInterval(dbPollInterval);
    dbPollInterval = null;
  }

  logger.info('Schedulers stopped');
}
