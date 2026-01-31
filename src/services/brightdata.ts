import axios, { AxiosInstance } from 'axios';
import { config } from '../config/index.js';
import { createChildLogger } from '../utils/logger.js';

const logger = createChildLogger('brightdata');

export interface BrightDataInput {
  url?: string;
  username?: string;
  hashtag?: string;
  keyword?: string;
}

export interface BrightDataSnapshot {
  snapshot_id: string;
  status: 'running' | 'ready' | 'failed';
}

export interface BrightDataResult {
  id: string;
  url: string;
  timestamp: string;
  author?: {
    id: string;
    name: string;
    username: string;
    profile_url: string;
    profile_pic_url: string;
    followers_count: number;
    following_count: number;
  };
  content?: {
    type: string;
    caption: string;
    thumbnail_url: string;
    video_url?: string;
  };
  metrics?: {
    views: number;
    likes: number;
    comments: number;
    shares: number;
  };
  posted_at?: string;
  // Raw data for platform-specific fields
  raw?: Record<string, unknown>;
}

export class BrightDataService {
  private client: AxiosInstance;

  constructor() {
    this.client = axios.create({
      baseURL: 'https://api.brightdata.com/datasets/v3',
      headers: {
        Authorization: `Bearer ${config.brightdata.apiToken}`,
        'Content-Type': 'application/json',
      },
      timeout: 30000,
    });
  }

  /**
   * Trigger a new data collection snapshot
   */
  async triggerCollection(
    platform: 'instagram' | 'tiktok',
    inputs: BrightDataInput[]
  ): Promise<BrightDataSnapshot> {
    const datasetId = config.brightdata.datasetIds[platform];

    if (!datasetId) {
      throw new Error(`No dataset ID configured for platform: ${platform}`);
    }

    try {
      logger.info({ platform, inputCount: inputs.length }, 'Triggering BrightData collection');

      const response = await this.client.post<BrightDataSnapshot>(
        `/trigger?dataset_id=${datasetId}&include_errors=true`,
        inputs
      );

      logger.info(
        { snapshotId: response.data.snapshot_id, status: response.data.status },
        'Collection triggered'
      );

      return response.data;
    } catch (error) {
      logger.error({ error, platform }, 'Failed to trigger collection');
      throw error;
    }
  }

  /**
   * Check the status of a snapshot
   */
  async checkSnapshotStatus(snapshotId: string): Promise<BrightDataSnapshot> {
    try {
      const response = await this.client.get<BrightDataSnapshot>(
        `/snapshot/${snapshotId}?format=json`
      );

      logger.debug({ snapshotId, status: response.data.status }, 'Snapshot status checked');

      return response.data;
    } catch (error) {
      logger.error({ error, snapshotId }, 'Failed to check snapshot status');
      throw error;
    }
  }

  /**
   * Normalize BrightData raw response to our expected format
   * Instagram API returns different field names than our interface
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private normalizeResult(raw: any): BrightDataResult {
    // Log raw data for first few items to debug
    logger.debug({ rawKeys: Object.keys(raw || {}).slice(0, 20) }, 'Raw result keys');

    // Instagram field mapping (BrightData uses different names)
    const result: BrightDataResult = {
      id: raw.id || raw.pk || raw.shortcode || '',
      url: raw.url || raw.post_url || raw.link || '',
      timestamp: new Date().toISOString(),
      author: raw.owner || raw.author ? {
        id: raw.owner?.id || raw.owner?.pk || raw.author?.id || '',
        name: raw.owner?.full_name || raw.owner?.name || raw.author?.name || '',
        username: raw.owner?.username || raw.author?.username || '',
        profile_url: raw.owner?.profile_url || '',
        profile_pic_url: raw.owner?.profile_pic_url || raw.owner?.profile_pic_url_hd || raw.author?.profile_pic_url || '',
        followers_count: raw.owner?.follower_count || raw.owner?.followers_count || 0,
        following_count: raw.owner?.following_count || 0,
      } : undefined,
      content: {
        type: raw.media_type || raw.type || 'post',
        caption: raw.caption || raw.description || raw.text || '',
        thumbnail_url: raw.display_url || raw.thumbnail_url || raw.image_url || '',
        video_url: raw.video_url || raw.video_versions?.[0]?.url || '',
      },
      metrics: {
        views: raw.play_count || raw.video_play_count || raw.view_count || raw.views || 0,
        likes: raw.like_count || raw.likes || raw.likes_count || 0,
        comments: raw.comment_count || raw.comments || raw.comments_count || 0,
        shares: raw.share_count || raw.shares || 0,
      },
      posted_at: raw.taken_at_timestamp
        ? new Date(raw.taken_at_timestamp * 1000).toISOString()
        : raw.taken_at || raw.posted_at || raw.timestamp || null,
      raw,
    };

    return result;
  }

  /**
   * Download snapshot results when ready
   */
  async downloadSnapshot(snapshotId: string): Promise<BrightDataResult[]> {
    try {
      logger.info({ snapshotId }, 'Downloading snapshot results');

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const response = await this.client.get<any[]>(
        `/snapshot/${snapshotId}?format=json`
      );

      // Log sample of raw data for debugging
      if (response.data.length > 0) {
        logger.info({
          sampleRawData: JSON.stringify(response.data[0]).substring(0, 1000),
          totalResults: response.data.length,
        }, 'Raw snapshot sample');
      }

      // Normalize all results
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const normalizedResults = response.data.map((raw: any) => this.normalizeResult(raw));

      logger.info({ snapshotId, resultCount: normalizedResults.length }, 'Snapshot downloaded and normalized');

      return normalizedResults;
    } catch (error) {
      logger.error({ error, snapshotId }, 'Failed to download snapshot');
      throw error;
    }
  }

  /**
   * Wait for snapshot to complete with polling
   */
  async waitForSnapshot(
    snapshotId: string,
    options: { maxWaitMs?: number; pollIntervalMs?: number } = {}
  ): Promise<BrightDataResult[]> {
    const { maxWaitMs = 600000, pollIntervalMs = 10000 } = options; // Default: 10min max, 10s poll
    const startTime = Date.now();

    logger.info({ snapshotId, maxWaitMs, pollIntervalMs }, 'Waiting for snapshot completion');

    while (Date.now() - startTime < maxWaitMs) {
      const status = await this.checkSnapshotStatus(snapshotId);

      if (status.status === 'ready') {
        return this.downloadSnapshot(snapshotId);
      }

      if (status.status === 'failed') {
        throw new Error(`Snapshot ${snapshotId} failed`);
      }

      // Wait before next poll
      await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
    }

    throw new Error(`Snapshot ${snapshotId} timed out after ${maxWaitMs}ms`);
  }

  /**
   * Build input objects for profile collection
   */
  buildProfileInputs(usernames: string[], platform: 'instagram' | 'tiktok'): BrightDataInput[] {
    return usernames.map((username) => {
      const cleanUsername = username.replace(/^@/, '');

      if (platform === 'instagram') {
        return { url: `https://www.instagram.com/${cleanUsername}/` };
      } else {
        return { url: `https://www.tiktok.com/@${cleanUsername}` };
      }
    });
  }

  /**
   * Build input objects for hashtag collection
   */
  buildHashtagInputs(hashtags: string[], platform: 'instagram' | 'tiktok'): BrightDataInput[] {
    return hashtags.map((hashtag) => {
      const cleanHashtag = hashtag.replace(/^#/, '');

      if (platform === 'instagram') {
        return { url: `https://www.instagram.com/explore/tags/${cleanHashtag}/` };
      } else {
        return { hashtag: cleanHashtag };
      }
    });
  }
}

// Singleton instance
let instance: BrightDataService | null = null;

export function getBrightDataService(): BrightDataService {
  if (!instance) {
    instance = new BrightDataService();
  }
  return instance;
}
