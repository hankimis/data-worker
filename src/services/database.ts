import { Pool, PoolClient } from 'pg';
import { config } from '../config/index.js';
import { createChildLogger } from '../utils/logger.js';
import type { BrightDataResult } from './brightdata.js';

const logger = createChildLogger('database');

export interface ContentRecord {
  id: number;
  platform: string;
  content_url: string;
  author_id: string | null;
  author_name: string | null;
  author_profile_url: string | null;
  thumbnail_url: string | null;
  video_url: string | null;
  caption: string | null;
  view_count: number | null;
  like_count: number | null;
  comment_count: number | null;
  share_count: number | null;
  uploaded_at: Date | null;
}

export class DatabaseService {
  private pool: Pool;

  constructor() {
    this.pool = new Pool({
      connectionString: config.database.url,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 5000,
    });

    this.pool.on('error', (err) => {
      logger.error({ error: err }, 'Unexpected pool error');
    });
  }

  async initialize(): Promise<void> {
    try {
      const client = await this.pool.connect();
      await client.query('SELECT 1');
      client.release();
      logger.info('Database connection established');
    } catch (error) {
      logger.error({ error }, 'Failed to connect to database');
      throw error;
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
    logger.info('Database connection closed');
  }

  /**
   * Upsert content from BrightData results
   */
  async upsertContent(
    platform: 'instagram' | 'tiktok',
    results: BrightDataResult[],
    projectId?: number
  ): Promise<{ inserted: number; updated: number }> {
    const client = await this.pool.connect();
    let inserted = 0;
    let updated = 0;

    try {
      await client.query('BEGIN');

      for (const result of results) {
        // Log raw result for debugging (first 3 items only)
        if (inserted + updated < 3) {
          logger.info({
            rawResult: JSON.stringify(result).substring(0, 500),
            hasAuthor: !!result.author,
            hasMetrics: !!result.metrics,
            hasContent: !!result.content,
          }, 'BrightData result sample');
        }

        const contentUrl = result.url;

        // Check if content exists
        const existing = await client.query(
          'SELECT id FROM contents WHERE content_url = $1',
          [contentUrl]
        );

        if (existing.rows.length > 0) {
          // Update existing content - fill missing fields and update metrics
          // COALESCE with NULLIF handles empty strings: if current is '' or NULL, use new value
          await client.query(
            `UPDATE contents SET
              author_id = COALESCE(NULLIF($1, ''), NULLIF(author_id, ''), author_id),
              author_profile_url = COALESCE(NULLIF($2, ''), author_profile_url),
              thumbnail_url = COALESCE(NULLIF($3, ''), thumbnail_url),
              video_url = COALESCE(NULLIF($4, ''), video_url),
              caption = COALESCE(NULLIF($5, ''), caption),
              view_count_collected = GREATEST($6, view_count_collected),
              like_count_collected = GREATEST($7, like_count_collected),
              comment_count_collected = GREATEST($8, comment_count_collected),
              uploaded_at = COALESCE($9, uploaded_at)
            WHERE content_url = $10`,
            [
              result.author?.username || result.author?.id || '',
              result.author?.profile_pic_url || '',
              result.content?.thumbnail_url || '',
              result.content?.video_url || '',
              result.content?.caption || '',
              result.metrics?.views ?? 0,
              result.metrics?.likes ?? 0,
              result.metrics?.comments ?? 0,
              result.posted_at ? new Date(result.posted_at) : null,
              contentUrl,
            ]
          );
          updated++;

          // Link to monitoring project if specified
          if (projectId) {
            await this.linkContentToProject(client, existing.rows[0].id, projectId);
          }
        } else {
          // Insert new content (correct column names matching schema.ts)
          const insertResult = await client.query(
            `INSERT INTO contents (
              platform, content_url, author_id,
              author_profile_url, thumbnail_url, video_url,
              caption, view_count_collected, like_count_collected, comment_count_collected,
              uploaded_at, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
            RETURNING id`,
            [
              platform,
              contentUrl,
              result.author?.username || result.author?.id || 'unknown',
              result.author?.profile_pic_url,
              result.content?.thumbnail_url,
              result.content?.video_url,
              result.content?.caption,
              result.metrics?.views ?? 0,
              result.metrics?.likes ?? 0,
              result.metrics?.comments ?? 0,
              result.posted_at ? new Date(result.posted_at) : null,
            ]
          );
          inserted++;

          // Link to monitoring project if specified
          if (projectId && insertResult.rows[0]) {
            await this.linkContentToProject(client, insertResult.rows[0].id, projectId);
          }
        }
      }

      await client.query('COMMIT');
      logger.info({ inserted, updated }, 'Content upsert completed');

      return { inserted, updated };
    } catch (error) {
      await client.query('ROLLBACK');
      logger.error({ error }, 'Failed to upsert content');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Link content to a monitoring project
   */
  private async linkContentToProject(
    client: PoolClient,
    contentId: number,
    projectId: number
  ): Promise<void> {
    try {
      await client.query(
        `INSERT INTO monitoring_contents (project_id, content_id, captured_at)
         VALUES ($1, $2, NOW())
         ON CONFLICT (project_id, content_id) DO NOTHING`,
        [projectId, contentId]
      );
    } catch (error) {
      // Ignore if monitoring_contents table doesn't exist
      logger.debug({ error, contentId, projectId }, 'Could not link content to project');
    }
  }

  /**
   * Update collection job status
   */
  async updateJobStatus(
    jobId: number,
    status: 'pending' | 'running' | 'completed' | 'failed',
    error?: string
  ): Promise<void> {
    try {
      await this.pool.query(
        `UPDATE collection_jobs SET
          status = $1,
          last_error = $2,
          last_run_at = NOW(),
          updated_at = NOW()
        WHERE id = $3`,
        [status, error || null, jobId]
      );
      logger.debug({ jobId, status }, 'Job status updated');
    } catch (err) {
      logger.error({ error: err, jobId }, 'Failed to update job status');
    }
  }

  /**
   * Get pending collection jobs
   */
  async getPendingJobs(): Promise<
    Array<{
      id: number;
      platform: string;
      job_type: string;
      target_identifier: string;
      project_id: number | null;
    }>
  > {
    const result = await this.pool.query(
      `SELECT cj.id, cj.platform, cj.job_type, cj.target_identifier,
              mt.project_id
       FROM collection_jobs cj
       LEFT JOIN monitoring_targets mt ON mt.collection_job_id = cj.id
       WHERE cj.status = 'pending'
         OR (cj.status = 'completed'
             AND cj.next_run_at IS NOT NULL
             AND cj.next_run_at <= NOW())
       ORDER BY cj.created_at ASC
       LIMIT 50`
    );

    return result.rows;
  }

  /**
   * Record metrics history for tracking growth
   */
  async recordMetricsHistory(
    contentId: number,
    metrics: {
      views: number;
      likes: number;
      comments: number;
      shares?: number;
    }
  ): Promise<void> {
    try {
      await this.pool.query(
        `INSERT INTO content_metrics_history (content_id, views, likes, comments, shares, recorded_at)
         VALUES ($1, $2, $3, $4, $5, NOW())`,
        [contentId, metrics.views, metrics.likes, metrics.comments, metrics.shares || 0]
      );
    } catch (error) {
      // Ignore if table doesn't exist
      logger.debug({ error, contentId }, 'Could not record metrics history');
    }
  }
}

// Singleton instance
let instance: DatabaseService | null = null;

export function getDatabaseService(): DatabaseService {
  if (!instance) {
    instance = new DatabaseService();
  }
  return instance;
}
