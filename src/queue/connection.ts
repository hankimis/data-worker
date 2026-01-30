import { Redis } from 'ioredis';
import { config } from '../config/index.js';
import { createChildLogger } from '../utils/logger.js';

const logger = createChildLogger('redis');

let connection: Redis | null = null;

export function getRedisConnection(): Redis {
  if (!connection) {
    connection = new Redis({
      host: config.redis.host,
      port: config.redis.port,
      password: config.redis.password,
      maxRetriesPerRequest: null,
      enableReadyCheck: false,
    });

    connection.on('connect', () => {
      logger.info('Redis connected');
    });

    connection.on('error', (err) => {
      logger.error({ error: err }, 'Redis error');
    });

    connection.on('close', () => {
      logger.warn('Redis connection closed');
    });
  }

  return connection;
}

export async function closeRedisConnection(): Promise<void> {
  if (connection) {
    await connection.quit();
    connection = null;
    logger.info('Redis connection closed');
  }
}
