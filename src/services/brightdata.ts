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
   * Download snapshot results when ready
   */
  async downloadSnapshot(snapshotId: string): Promise<BrightDataResult[]> {
    try {
      logger.info({ snapshotId }, 'Downloading snapshot results');

      const response = await this.client.get<BrightDataResult[]>(
        `/snapshot/${snapshotId}?format=json`
      );

      logger.info({ snapshotId, resultCount: response.data.length }, 'Snapshot downloaded');

      return response.data;
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
