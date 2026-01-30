import { getCollectionQueue, CollectionJobData } from './queue/processor.js';
import { getGoogleSheetsService, DEFAULT_SPREADSHEET_ID, DEFAULT_SHEET_NAME } from './services/google-sheets.js';
import { config } from './config/index.js';
import { createChildLogger } from './utils/logger.js';

const logger = createChildLogger('scheduler');

// IOV 시트 수집 설정
interface IOVSheetConfig {
  spreadsheetId: string;
  sheetName: string;
  intervalMs: number;
  batchSize: number;
  itemsPerProfile: number;
}

// 기본 설정: 아이오브 시트, 1시간마다, 25명씩 배치
const IOV_SHEET_CONFIG: IOVSheetConfig = {
  spreadsheetId: DEFAULT_SPREADSHEET_ID,
  sheetName: DEFAULT_SHEET_NAME,
  intervalMs: 60 * 60 * 1000, // 1시간
  batchSize: 25,
  itemsPerProfile: 10,
};

let iovSheetInterval: NodeJS.Timeout | null = null;
let isCollecting = false;

/**
 * IOV 시트에서 미수집 프로필 수집 시작
 */
async function collectFromIOVSheet(): Promise<void> {
  if (isCollecting) {
    logger.info('Collection already in progress, skipping');
    return;
  }

  isCollecting = true;

  try {
    const sheetsService = getGoogleSheetsService();
    await sheetsService.initialize();

    // 미수집 프로필 가져오기
    const uncollectedProfiles = await sheetsService.getUncollectedProfiles(
      IOV_SHEET_CONFIG.spreadsheetId,
      IOV_SHEET_CONFIG.sheetName
    );

    if (uncollectedProfiles.length === 0) {
      logger.info('No uncollected profiles found');
      isCollecting = false;
      return;
    }

    logger.info({ count: uncollectedProfiles.length }, 'Found uncollected profiles');

    // 배치로 분할
    const batches: typeof uncollectedProfiles[] = [];
    for (let i = 0; i < uncollectedProfiles.length; i += IOV_SHEET_CONFIG.batchSize) {
      batches.push(uncollectedProfiles.slice(i, i + IOV_SHEET_CONFIG.batchSize));
    }

    logger.info({ totalBatches: batches.length, batchSize: IOV_SHEET_CONFIG.batchSize }, 'Split into batches');

    const collectionQueue = getCollectionQueue();

    // 각 배치를 큐에 추가
    for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
      const batch = batches[batchIndex];
      const rows = batch.map((p) => p.row);
      const usernames = batch.map((p) => p.username);

      // 수집중 상태 표시
      await sheetsService.markAsCollecting(
        IOV_SHEET_CONFIG.spreadsheetId,
        IOV_SHEET_CONFIG.sheetName,
        rows
      );

      // 플랫폼별로 그룹화
      const instagramProfiles = batch.filter((p) => p.platform === 'instagram');
      const tiktokProfiles = batch.filter((p) => p.platform === 'tiktok');

      // Instagram 수집 작업 추가
      if (instagramProfiles.length > 0) {
        const jobData: CollectionJobData = {
          type: 'profile',
          platform: 'instagram',
          targets: instagramProfiles.map((p) => p.username),
          sheetInfo: {
            spreadsheetId: IOV_SHEET_CONFIG.spreadsheetId,
            sheetName: IOV_SHEET_CONFIG.sheetName,
            rows: instagramProfiles.map((p) => p.row),
            itemsPerProfile: IOV_SHEET_CONFIG.itemsPerProfile,
          },
        };

        await collectionQueue.add(`iov-instagram-batch-${batchIndex}`, jobData, {
          attempts: config.worker.maxRetries,
          backoff: { type: 'exponential', delay: 10000 },
          delay: batchIndex * 60000, // 배치간 1분 간격
        });

        logger.info(
          { batch: batchIndex, platform: 'instagram', count: instagramProfiles.length },
          'Added Instagram batch to queue'
        );
      }

      // TikTok 수집 작업 추가
      if (tiktokProfiles.length > 0) {
        const jobData: CollectionJobData = {
          type: 'profile',
          platform: 'tiktok',
          targets: tiktokProfiles.map((p) => p.username),
          sheetInfo: {
            spreadsheetId: IOV_SHEET_CONFIG.spreadsheetId,
            sheetName: IOV_SHEET_CONFIG.sheetName,
            rows: tiktokProfiles.map((p) => p.row),
            itemsPerProfile: IOV_SHEET_CONFIG.itemsPerProfile,
          },
        };

        await collectionQueue.add(`iov-tiktok-batch-${batchIndex}`, jobData, {
          attempts: config.worker.maxRetries,
          backoff: { type: 'exponential', delay: 10000 },
          delay: batchIndex * 60000,
        });

        logger.info(
          { batch: batchIndex, platform: 'tiktok', count: tiktokProfiles.length },
          'Added TikTok batch to queue'
        );
      }
    }

    logger.info({ totalBatches: batches.length }, 'All batches added to queue');
  } catch (error) {
    logger.error({ error }, 'Failed to collect from IOV sheet');
  } finally {
    isCollecting = false;
  }
}

/**
 * IOV 시트 수집 스케줄러 시작
 */
export function startSheetsSyncScheduler(): void {
  logger.info(
    {
      spreadsheetId: IOV_SHEET_CONFIG.spreadsheetId,
      sheetName: IOV_SHEET_CONFIG.sheetName,
      intervalMs: IOV_SHEET_CONFIG.intervalMs,
    },
    'Starting IOV sheet collection scheduler'
  );

  // 시작 후 1분 뒤에 첫 수집 실행 (서버 초기화 시간 확보)
  setTimeout(() => {
    collectFromIOVSheet();
  }, 60000);

  // 주기적 수집
  iovSheetInterval = setInterval(() => {
    collectFromIOVSheet();
  }, IOV_SHEET_CONFIG.intervalMs);

  logger.info('IOV sheet collection scheduler started');
}

/**
 * 수동으로 수집 트리거 (관리자 API용)
 */
export async function triggerManualCollection(): Promise<{ message: string; count: number }> {
  if (isCollecting) {
    return { message: '이미 수집이 진행 중입니다.', count: 0 };
  }

  await collectFromIOVSheet();

  const sheetsService = getGoogleSheetsService();
  const profiles = await sheetsService.getProfiles(
    IOV_SHEET_CONFIG.spreadsheetId,
    IOV_SHEET_CONFIG.sheetName
  );
  const collectingCount = profiles.filter((p) => p.isCollecting).length;

  return { message: '수집이 시작되었습니다.', count: collectingCount };
}

/**
 * DB 폴링 스케줄러 (기존 collection_jobs 테이블 사용 시)
 */
export function startDatabasePollScheduler(): void {
  // IOV 시트 수집을 주로 사용하므로 DB 폴링은 비활성화
  logger.info('Database poll scheduler disabled (using IOV sheet collection)');
}

/**
 * 모든 스케줄러 중지
 */
export function stopSchedulers(): void {
  if (iovSheetInterval) {
    clearInterval(iovSheetInterval);
    iovSheetInterval = null;
  }

  logger.info('Schedulers stopped');
}
