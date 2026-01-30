import { config, validateConfig } from './config/index.js';
import { logger } from './utils/logger.js';
import { getDatabaseService } from './services/database.js';
import { getRedisConnection, closeRedisConnection } from './queue/connection.js';
import {
  initializeQueues,
  startWorkers,
  stopWorkers,
  closeQueues,
  getCollectionQueue,
} from './queue/processor.js';
import {
  startSheetsSyncScheduler,
  startDatabasePollScheduler,
  stopSchedulers,
} from './scheduler.js';
import {
  startHeartbeat,
  stopHeartbeat,
  sendOfflineNotification,
} from './services/heartbeat.js';

let isShuttingDown = false;

async function gracefulShutdown(signal: string): Promise<void> {
  if (isShuttingDown) {
    logger.warn('Shutdown already in progress...');
    return;
  }

  isShuttingDown = true;
  logger.info({ signal }, 'Graceful shutdown initiated');

  try {
    // Send offline notification to main server
    await sendOfflineNotification();

    // Stop heartbeat
    stopHeartbeat();

    // Stop schedulers
    stopSchedulers();

    // Stop workers
    await stopWorkers();

    // Close queues
    await closeQueues();

    // Close Redis
    await closeRedisConnection();

    // Close database
    const database = getDatabaseService();
    await database.close();

    logger.info('Shutdown complete');
    process.exit(0);
  } catch (error) {
    logger.error({ error }, 'Error during shutdown');
    process.exit(1);
  }
}

async function main(): Promise<void> {
  logger.info('='.repeat(50));
  logger.info('IOV Data Collector Worker Starting...');
  logger.info('='.repeat(50));

  try {
    // Validate configuration
    validateConfig();
    logger.info('Configuration validated');

    // Initialize database
    const database = getDatabaseService();
    await database.initialize();

    // Initialize Redis connection
    getRedisConnection();
    logger.info('Redis connection initialized');

    // Initialize queues
    initializeQueues();

    // Start workers
    startWorkers();

    // Start schedulers
    startSheetsSyncScheduler();
    startDatabasePollScheduler();

    // Start heartbeat to main SaaS server
    startHeartbeat();

    logger.info('='.repeat(50));
    logger.info(`Worker "${config.worker.name}" (${config.worker.id}) is running.`);
    logger.info('Press Ctrl+C to stop.');
    logger.info('='.repeat(50));

    // Handle shutdown signals
    process.on('SIGINT', () => gracefulShutdown('SIGINT'));
    process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

    // Keep process alive
    process.stdin.resume();
  } catch (error) {
    logger.error({ error }, 'Failed to start worker');
    process.exit(1);
  }
}

// CLI commands for manual operations
const args = process.argv.slice(2);

if (args[0] === 'collect') {
  // Manual collection: npm run dev collect instagram profile @username1 @username2
  const platform = args[1] as 'instagram' | 'tiktok';
  const type = args[2] as 'profile' | 'hashtag' | 'keyword';
  const targets = args.slice(3);

  if (!platform || !type || targets.length === 0) {
    console.log('Usage: npm run dev collect <instagram|tiktok> <profile|hashtag|keyword> <targets...>');
    process.exit(1);
  }

  (async () => {
    try {
      validateConfig();
      getRedisConnection();
      initializeQueues();

      const queue = getCollectionQueue();
      await queue.add('manual-collect', { platform, type, targets });

      logger.info({ platform, type, targets }, 'Manual collection job added to queue');

      // Wait a bit for the job to be added
      await new Promise((resolve) => setTimeout(resolve, 1000));
      await closeQueues();
      await closeRedisConnection();

      process.exit(0);
    } catch (error) {
      logger.error({ error }, 'Failed to add manual collection job');
      process.exit(1);
    }
  })();
} else {
  // Default: start worker
  main();
}
