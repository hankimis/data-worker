import axios from 'axios';
import os from 'os';
import { config } from '../config/index.js';
import { createChildLogger } from '../utils/logger.js';

const logger = createChildLogger('heartbeat');

export interface CollectionStats {
  totalProfiles: number;
  collectedProfiles: number;
  collectingProfiles: number;
  uncollectedProfiles: number;
  uncollectableProfiles: number;
  isCollectionPaused: boolean;
  lastCollectionAt: string | null;
  nextCollectionAt: string | null;
}

export interface ActivityEntry {
  id: string;
  timestamp: string;
  type: 'info' | 'success' | 'warning' | 'error';
  category: 'job' | 'collection' | 'system' | 'api';
  message: string;
  details?: Record<string, unknown>;
}

export interface WorkerStatus {
  workerId: string;
  workerName: string;
  status: 'online' | 'busy' | 'idle' | 'paused';
  version: string;
  startedAt: string;
  system: {
    platform: string;
    arch: string;
    hostname: string;
    cpuUsage: number;
    memoryUsage: {
      total: number;
      used: number;
      percentage: number;
    };
    uptime: number;
  };
  queue: {
    activeJobs: number;
    waitingJobs: number;
    completedJobs: number;
    failedJobs: number;
  };
  collection: CollectionStats;
  lastJobAt: string | null;
  recentActivity: ActivityEntry[];
}

// Worker state
let startedAt = new Date().toISOString();
let completedJobs = 0;
let failedJobs = 0;
let activeJobs = 0;
let waitingJobs = 0;
let lastJobAt: string | null = null;
let heartbeatInterval: NodeJS.Timeout | null = null;

// Recent activity log (max 100 entries)
const MAX_ACTIVITY_ENTRIES = 100;
let recentActivity: ActivityEntry[] = [];
let activityIdCounter = 0;

// Trigger callback (to avoid circular dependency with scheduler)
let triggerCallback: (() => Promise<void>) | null = null;

/**
 * Register trigger callback for manual collection
 */
export function registerTriggerCallback(callback: () => Promise<void>): void {
  triggerCallback = callback;
}

// Collection state
let collectionStats: CollectionStats = {
  totalProfiles: 0,
  collectedProfiles: 0,
  collectingProfiles: 0,
  uncollectedProfiles: 0,
  uncollectableProfiles: 0,
  isCollectionPaused: false,
  lastCollectionAt: null,
  nextCollectionAt: null,
};

/**
 * Update job counters
 */
export function updateJobStats(stats: {
  active?: number;
  waiting?: number;
  completed?: number;
  failed?: number;
  lastJobAt?: string;
}): void {
  if (stats.active !== undefined) activeJobs = stats.active;
  if (stats.waiting !== undefined) waitingJobs = stats.waiting;
  if (stats.completed !== undefined) completedJobs += stats.completed;
  if (stats.failed !== undefined) failedJobs += stats.failed;
  if (stats.lastJobAt) lastJobAt = stats.lastJobAt;
}

/**
 * Update collection stats
 */
export function updateCollectionStats(stats: Partial<CollectionStats>): void {
  collectionStats = { ...collectionStats, ...stats };
}

/**
 * Add activity entry to recent activity log
 */
export function addActivity(
  type: ActivityEntry['type'],
  category: ActivityEntry['category'],
  message: string,
  details?: Record<string, unknown>
): void {
  const entry: ActivityEntry = {
    id: `act-${++activityIdCounter}`,
    timestamp: new Date().toISOString(),
    type,
    category,
    message,
    details,
  };

  recentActivity.unshift(entry);

  // Keep only the most recent entries
  if (recentActivity.length > MAX_ACTIVITY_ENTRIES) {
    recentActivity = recentActivity.slice(0, MAX_ACTIVITY_ENTRIES);
  }

  logger.debug({ activity: entry }, 'Activity logged');
}

/**
 * Get recent activity entries
 */
export function getRecentActivity(limit = 50): ActivityEntry[] {
  return recentActivity.slice(0, limit);
}

/**
 * Get collection stats
 */
export function getCollectionStats(): CollectionStats {
  return { ...collectionStats };
}

/**
 * Check if collection is paused
 */
export function isCollectionPaused(): boolean {
  return collectionStats.isCollectionPaused;
}

/**
 * Set collection paused state
 */
export function setCollectionPaused(paused: boolean): void {
  collectionStats.isCollectionPaused = paused;
}

/**
 * Get current worker status
 */
export function getWorkerStatus(): WorkerStatus {
  const totalMem = os.totalmem();
  const freeMem = os.freemem();
  const usedMem = totalMem - freeMem;

  const cpus = os.cpus();
  const cpuUsage = cpus.reduce((acc, cpu) => {
    const total = Object.values(cpu.times).reduce((a, b) => a + b, 0);
    const idle = cpu.times.idle;
    return acc + ((total - idle) / total) * 100;
  }, 0) / cpus.length;

  let status: WorkerStatus['status'] = 'idle';
  if (collectionStats.isCollectionPaused) {
    status = 'paused';
  } else if (activeJobs > 0) {
    status = 'busy';
  }

  return {
    workerId: config.worker.id,
    workerName: config.worker.name,
    status,
    version: '1.0.0',
    startedAt,
    system: {
      platform: os.platform(),
      arch: os.arch(),
      hostname: os.hostname(),
      cpuUsage: Math.round(cpuUsage * 100) / 100,
      memoryUsage: {
        total: totalMem,
        used: usedMem,
        percentage: Math.round((usedMem / totalMem) * 100 * 100) / 100,
      },
      uptime: os.uptime(),
    },
    queue: {
      activeJobs,
      waitingJobs,
      completedJobs,
      failedJobs,
    },
    collection: collectionStats,
    lastJobAt,
    recentActivity: getRecentActivity(50),
  };
}

/**
 * Send heartbeat to main SaaS server
 */
export async function sendHeartbeat(): Promise<boolean> {
  if (!config.heartbeat.saasApiUrl || !config.heartbeat.workerSecret) {
    logger.debug('Heartbeat not configured, skipping');
    return false;
  }

  try {
    const status = getWorkerStatus();

    const response = await axios.post(
      `${config.heartbeat.saasApiUrl}/api/worker/heartbeat`,
      status,
      {
        headers: {
          'Content-Type': 'application/json',
          'X-Worker-Secret': config.heartbeat.workerSecret,
        },
        timeout: 10000,
      }
    );

    if (response.status === 200) {
      // Check for commands from server
      const data = response.data;
      if (data.command) {
        logger.info({ command: data.command }, 'Received command from server');
        handleServerCommand(data.command);
      }

      logger.debug({ workerId: status.workerId }, 'Heartbeat sent successfully');
      return true;
    }

    logger.warn({ status: response.status }, 'Heartbeat returned non-200 status');
    return false;
  } catch (error) {
    if (axios.isAxiosError(error)) {
      logger.warn(
        { error: error.message, code: error.code },
        'Failed to send heartbeat'
      );
    } else {
      logger.warn({ error }, 'Failed to send heartbeat');
    }
    return false;
  }
}

/**
 * Handle command from server
 */
function handleServerCommand(command: string): void {
  switch (command) {
    case 'pause':
      setCollectionPaused(true);
      logger.info('Collection paused by server command');
      break;
    case 'resume':
      setCollectionPaused(false);
      logger.info('Collection resumed by server command');
      break;
    case 'trigger':
      logger.info('Manual collection trigger received');
      if (triggerCallback) {
        triggerCallback().catch((err) => {
          logger.error({ error: err }, 'Failed to execute trigger callback');
        });
      }
      break;
    default:
      logger.warn({ command }, 'Unknown command received');
  }
}

/**
 * Start heartbeat scheduler
 */
export function startHeartbeat(): void {
  if (!config.heartbeat.saasApiUrl) {
    logger.info('Heartbeat URL not configured, heartbeat disabled');
    return;
  }

  logger.info(
    { intervalMs: config.heartbeat.intervalMs, saasUrl: config.heartbeat.saasApiUrl },
    'Starting heartbeat'
  );

  // Send initial heartbeat
  sendHeartbeat();

  // Schedule recurring heartbeats
  heartbeatInterval = setInterval(() => {
    sendHeartbeat();
  }, config.heartbeat.intervalMs);
}

/**
 * Stop heartbeat scheduler
 */
export function stopHeartbeat(): void {
  if (heartbeatInterval) {
    clearInterval(heartbeatInterval);
    heartbeatInterval = null;
    logger.info('Heartbeat stopped');
  }
}

/**
 * Send offline notification before shutdown
 */
export async function sendOfflineNotification(): Promise<void> {
  if (!config.heartbeat.saasApiUrl || !config.heartbeat.workerSecret) {
    return;
  }

  try {
    await axios.post(
      `${config.heartbeat.saasApiUrl}/api/worker/offline`,
      { workerId: config.worker.id },
      {
        headers: {
          'Content-Type': 'application/json',
          'X-Worker-Secret': config.heartbeat.workerSecret,
        },
        timeout: 5000,
      }
    );
    logger.info('Offline notification sent');
  } catch (error) {
    logger.warn({ error }, 'Failed to send offline notification');
  }
}
