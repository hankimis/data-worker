import dotenv from 'dotenv';

dotenv.config();

export const config = {
  // Redis
  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379', 10),
    password: process.env.REDIS_PASSWORD || undefined,
  },

  // Database
  database: {
    url: process.env.DATABASE_URL || '',
  },

  // BrightData
  brightdata: {
    apiToken: process.env.BRIGHTDATA_API_TOKEN || '',
    datasetIds: {
      instagram: process.env.BRIGHTDATA_DATASET_ID_INSTAGRAM || '',
      tiktok: process.env.BRIGHTDATA_DATASET_ID_TIKTOK || '',
    },
  },

  // Google Sheets
  google: {
    serviceAccountEmail: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || '',
    privateKey: (process.env.GOOGLE_PRIVATE_KEY || '').replace(/\\n/g, '\n'),
  },

  // Worker
  worker: {
    id: process.env.WORKER_ID || `worker-${Date.now()}`,
    name: process.env.WORKER_NAME || 'Data Collector Worker',
    concurrency: parseInt(process.env.WORKER_CONCURRENCY || '5', 10),
    pollIntervalMs: parseInt(process.env.POLL_INTERVAL_MS || '60000', 10),
    maxRetries: parseInt(process.env.MAX_RETRIES || '3', 10),
  },

  // Heartbeat
  heartbeat: {
    saasApiUrl: process.env.SAAS_API_URL || '',
    workerSecret: process.env.SAAS_WORKER_SECRET || '',
    intervalMs: parseInt(process.env.HEARTBEAT_INTERVAL_MS || '30000', 10),
  },

  // Logging
  logLevel: process.env.LOG_LEVEL || 'info',
} as const;

export function validateConfig(): void {
  const required = [
    ['DATABASE_URL', config.database.url],
    ['BRIGHTDATA_API_TOKEN', config.brightdata.apiToken],
  ];

  const missing = required.filter(([, value]) => !value);

  if (missing.length > 0) {
    throw new Error(
      `Missing required environment variables: ${missing.map(([name]) => name).join(', ')}`
    );
  }
}
