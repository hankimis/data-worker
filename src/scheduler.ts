import { getCollectionQueue, CollectionJobData } from './queue/processor.js';
import { getGoogleSheetsService, DEFAULT_SPREADSHEET_ID, DEFAULT_SHEET_NAME } from './services/google-sheets.js';
import { updateCollectionStats, isCollectionPaused, setCollectionPaused, addActivity } from './services/heartbeat.js';
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

// 기본 설정: 아이오브 시트, 1시간마다
// TODO: 테스트 완료 후 batchSize를 25로 변경
const IOV_SHEET_CONFIG: IOVSheetConfig = {
  spreadsheetId: DEFAULT_SPREADSHEET_ID,
  sheetName: DEFAULT_SHEET_NAME,
  intervalMs: 60 * 60 * 1000, // 1시간
  batchSize: 1, // 테스트용: 1개씩 수집
  itemsPerProfile: 10,
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let iovSheetInterval: any = null;
let isCollecting = false;
let nextCollectionTime: Date | null = null;

/**
 * 시트 통계 업데이트
 */
async function updateSheetStats(): Promise<void> {
  try {
    const sheetsService = getGoogleSheetsService();
    await sheetsService.initialize();

    const profiles = await sheetsService.getProfiles(
      IOV_SHEET_CONFIG.spreadsheetId,
      IOV_SHEET_CONFIG.sheetName
    );

    const stats = {
      totalProfiles: profiles.length,
      collectedProfiles: profiles.filter((p) => p.isCollected).length,
      collectingProfiles: profiles.filter((p) => p.isCollecting).length,
      uncollectedProfiles: profiles.filter((p) => !p.isCollected && !p.isCollecting && !p.isUncollectable).length,
      uncollectableProfiles: profiles.filter((p) => p.isUncollectable).length,
      nextCollectionAt: nextCollectionTime?.toISOString() || null,
    };

    updateCollectionStats(stats);
    logger.debug({ stats }, 'Updated collection stats');
  } catch (error) {
    logger.error({ error }, 'Failed to update sheet stats');
  }
}

/**
 * IOV 시트에서 미수집 프로필 수집 시작
 */
async function collectFromIOVSheet(): Promise<{ success: boolean; message: string; count: number }> {
  // 일시정지 상태 확인
  if (isCollectionPaused()) {
    logger.info('Collection is paused, skipping');
    return { success: false, message: '수집이 일시정지 상태입니다.', count: 0 };
  }

  if (isCollecting) {
    logger.info('Collection already in progress, skipping');
    return { success: false, message: '이미 수집이 진행 중입니다.', count: 0 };
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
      await updateSheetStats();
      isCollecting = false;
      return { success: true, message: '수집할 프로필이 없습니다.', count: 0 };
    }

    logger.info({ count: uncollectedProfiles.length }, 'Found uncollected profiles');
    addActivity('info', 'collection', `미수집 프로필 ${uncollectedProfiles.length}개 발견`, {
      count: uncollectedProfiles.length,
    });

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

      // 수집중 상태 표시
      await sheetsService.markAsCollecting(
        IOV_SHEET_CONFIG.spreadsheetId,
        IOV_SHEET_CONFIG.sheetName,
        batch.map((p) => p.row)
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
          delay: batchIndex * 60000,
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

    // 통계 업데이트
    updateCollectionStats({
      lastCollectionAt: new Date().toISOString(),
    });
    await updateSheetStats();

    logger.info({ totalBatches: batches.length }, 'All batches added to queue');
    addActivity('success', 'collection', `${batches.length}개 배치 큐에 추가 완료`, {
      totalBatches: batches.length,
      totalProfiles: uncollectedProfiles.length,
    });

    return {
      success: true,
      message: `${uncollectedProfiles.length}개 프로필 수집을 시작합니다.`,
      count: uncollectedProfiles.length,
    };
  } catch (error) {
    logger.error({ error }, 'Failed to collect from IOV sheet');
    return { success: false, message: `수집 실패: ${error}`, count: 0 };
  } finally {
    isCollecting = false;
  }
}

/**
 * IOV 시트 수집 스케줄러 시작
 */
export async function startSheetsSyncScheduler(): Promise<void> {
  logger.info(
    {
      spreadsheetId: IOV_SHEET_CONFIG.spreadsheetId,
      sheetName: IOV_SHEET_CONFIG.sheetName,
      intervalMs: IOV_SHEET_CONFIG.intervalMs,
    },
    'Starting IOV sheet collection scheduler'
  );

  // 초기 통계 업데이트 (await 추가 - heartbeat 전송 전에 stats 로드)
  await updateSheetStats();

  // 다음 수집 시간 설정
  nextCollectionTime = new Date(Date.now() + 60000);
  updateCollectionStats({ nextCollectionAt: nextCollectionTime.toISOString() });

  // 시작 후 1분 뒤에 첫 수집 실행
  setTimeout(() => {
    collectFromIOVSheet();
    scheduleNextCollection();
  }, 60000);

  logger.info('IOV sheet collection scheduler started');
}

/**
 * 다음 수집 스케줄
 */
function scheduleNextCollection(): void {
  if (iovSheetInterval) {
    clearInterval(iovSheetInterval);
  }

  nextCollectionTime = new Date(Date.now() + IOV_SHEET_CONFIG.intervalMs);
  updateCollectionStats({ nextCollectionAt: nextCollectionTime.toISOString() });

  iovSheetInterval = setInterval(() => {
    collectFromIOVSheet();
    nextCollectionTime = new Date(Date.now() + IOV_SHEET_CONFIG.intervalMs);
    updateCollectionStats({ nextCollectionAt: nextCollectionTime.toISOString() });
  }, IOV_SHEET_CONFIG.intervalMs);
}

/**
 * 수동으로 수집 트리거
 */
export async function triggerManualCollection(): Promise<{ success: boolean; message: string; count: number }> {
  return await collectFromIOVSheet();
}

/**
 * 수집 일시정지
 */
export function pauseCollection(): void {
  setCollectionPaused(true);
  logger.info('Collection paused');
}

/**
 * 수집 재개
 */
export function resumeCollection(): void {
  setCollectionPaused(false);
  logger.info('Collection resumed');
}

/**
 * 현재 수집 상태 조회
 */
export function getCollectionStatus(): {
  isPaused: boolean;
  isCollecting: boolean;
  nextCollectionAt: string | null;
} {
  return {
    isPaused: isCollectionPaused(),
    isCollecting,
    nextCollectionAt: nextCollectionTime?.toISOString() || null,
  };
}

/**
 * DB 폴링 스케줄러 (비활성화)
 */
export function startDatabasePollScheduler(): void {
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
