import axios from 'axios';
import os from 'os';
import { config } from '../config/index.js';
import { createChildLogger } from '../utils/logger.js';

const logger = createChildLogger('heartbeat');

export interface WorkerStatus {
  workerId: string;
  workerName: string;
  status: 'online' | 'busy' | 'idle';
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
  lastJobAt: string | null;
}

// Worker state
let startedAt = new Date().toISOString();
let completedJobs = 0;
let failedJobs = 0;
let activeJobs = 0;
let waitingJobs = 0;
let lastJobAt: string | null = null;
let heartbeatInterval: NodeJS.Timeout | null = null;

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
 * Get current worker status
 */
export function getWorkerStatus(): WorkerStatus {
  const totalMem = os.totalmem();
  const freeMem = os.freemem();
  const usedMem = totalMem - freeMem;

  // Get CPU usage (simple approximation using load average on Unix, or 0 on Windows)
  const cpus = os.cpus();
  const cpuUsage = cpus.reduce((acc, cpu) => {
    const total = Object.values(cpu.times).reduce((a, b) => a + b, 0);
    const idle = cpu.times.idle;
    return acc + ((total - idle) / total) * 100;
  }, 0) / cpus.length;

  return {
    workerId: config.worker.id,
    workerName: config.worker.name,
    status: activeJobs > 0 ? 'busy' : 'idle',
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
    lastJobAt,
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
