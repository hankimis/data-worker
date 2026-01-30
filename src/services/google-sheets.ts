import { google, sheets_v4 } from 'googleapis';
import { config } from '../config/index.js';
import { createChildLogger } from '../utils/logger.js';

const logger = createChildLogger('google-sheets');

// IOV 기본 시트 ID
export const DEFAULT_SPREADSHEET_ID = '1bxpW3eSpha9BZEe_hdfq9rfPcZwLrIDw4m9PLJvYYRw';
export const DEFAULT_SHEET_NAME = '아이오브';

export interface SheetProfile {
  row: number;
  username: string;
  profileUrl: string;
  followers: number;
  sourceMode: string;
  sourceQuery: string;
  isCollected: boolean;
  isCollecting: boolean;
  isUncollectable: boolean;
  reelsCollected: number;
  platform: 'instagram' | 'tiktok';
}

export class GoogleSheetsService {
  private sheets: sheets_v4.Sheets | null = null;
  private initialized = false;

  private normalizeHeader(v: unknown): string {
    return String(v ?? '').trim().toLowerCase();
  }

  private findHeaderIndex(header: string[], candidates: string[]): number {
    return header.findIndex((h) => candidates.includes(h));
  }

  private colIndexToA1(colIndex: number): string {
    let n = colIndex + 1;
    let s = '';
    while (n > 0) {
      const r = (n - 1) % 26;
      s = String.fromCharCode(65 + r) + s;
      n = Math.floor((n - 1) / 26);
    }
    return s;
  }

  async initialize(): Promise<void> {
    if (this.initialized) return;

    try {
      logger.info('Initializing Google Sheets connection...');

      const auth = new google.auth.JWT({
        email: config.google.serviceAccountEmail,
        key: config.google.privateKey,
        scopes: ['https://www.googleapis.com/auth/spreadsheets'],
      });

      this.sheets = google.sheets({ version: 'v4', auth });
      this.initialized = true;
      logger.info('Google Sheets service initialized');
    } catch (error) {
      logger.error({ error }, 'Failed to initialize Google Sheets');
      throw error;
    }
  }

  private async getHeaderRow(spreadsheetId: string, sheetName: string): Promise<string[]> {
    await this.initialize();
    if (!this.sheets) throw new Error('Sheets API not initialized');

    const resp = await this.sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `${sheetName}!A1:Z1`,
    });
    const row = resp.data.values?.[0] ?? [];
    return row.map((v) => this.normalizeHeader(v));
  }

  private async getOrCreateReelsColumnLetter(spreadsheetId: string, sheetName: string): Promise<string> {
    await this.initialize();
    if (!this.sheets) throw new Error('Sheets API not initialized');

    const header = await this.getHeaderRow(spreadsheetId, sheetName);
    const reelsCandidates = ['reels collected', 'reels_collected', 'reels', '릴스 수집', '릴스'];

    let idx = this.findHeaderIndex(header, reelsCandidates);
    if (idx >= 0) return this.colIndexToA1(idx);

    // 없으면 첫 빈 칸 또는 마지막 사용
    idx = header.findIndex((h) => !h);
    if (idx < 0) idx = Math.min(header.length, 25);
    if (idx > 25) idx = 25;

    const col = this.colIndexToA1(idx);
    await this.sheets.spreadsheets.values.update({
      spreadsheetId,
      range: `${sheetName}!${col}1`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [['Reels Collected']] },
    });
    return col;
  }

  /**
   * 시트에서 프로필 목록 읽기 (IOV 형식)
   */
  async getProfiles(
    spreadsheetId: string = DEFAULT_SPREADSHEET_ID,
    sheetName: string = DEFAULT_SHEET_NAME
  ): Promise<SheetProfile[]> {
    await this.initialize();
    if (!this.sheets) throw new Error('Sheets API not initialized');

    try {
      const response = await this.sheets.spreadsheets.values.get({
        spreadsheetId,
        range: `${sheetName}!A1:Z`,
      });

      const allRows = response.data.values || [];
      const profiles: SheetProfile[] = [];
      if (allRows.length === 0) return profiles;

      const rawHeader = allRows[0] || [];
      const header = rawHeader.map((v) => this.normalizeHeader(v));

      const looksLikeHeader = header.some((h) =>
        ['username', '아이디', 'name', '이름', 'followers', '팔로워', 'reels collected'].includes(h)
      );

      const dataRows = looksLikeHeader ? allRows.slice(1) : allRows;
      const rowOffset = looksLikeHeader ? 2 : 1;

      // 컬럼 인덱스 찾기
      const colUsername = looksLikeHeader
        ? Math.max(0, this.findHeaderIndex(header, ['username', 'user', 'handle', 'id', 'author_id', 'account', '아이디', '계정']))
        : 0;
      const colPlatform = looksLikeHeader ? this.findHeaderIndex(header, ['platform', '플랫폼']) : -1;
      const colProfileUrl = looksLikeHeader ? this.findHeaderIndex(header, ['profile url', 'profile_url', 'profile', 'url']) : 1;
      const colFollowers = looksLikeHeader ? this.findHeaderIndex(header, ['followers', 'follower', '팔로워']) : 3;
      const colSourceMode = looksLikeHeader ? this.findHeaderIndex(header, ['source mode', 'source_mode', '수집 모드']) : 10;
      const colSourceQuery = looksLikeHeader ? this.findHeaderIndex(header, ['source query', 'source_query', '수집 소스', 'query']) : 11;
      const colReels = looksLikeHeader ? this.findHeaderIndex(header, ['reels collected', 'reels_collected', 'reels', '릴스 수집', '릴스']) : 12;

      dataRows.forEach((row, index) => {
        const username = (row[colUsername] || '').toString().trim().replace(/^@/, '');
        if (!username) return;

        const platformRaw = colPlatform >= 0 ? String(row[colPlatform] || '').trim().toLowerCase() : '';
        const platform: 'instagram' | 'tiktok' =
          platformRaw.includes('tiktok') || platformRaw.includes('tt') ? 'tiktok' : 'instagram';

        const reelsCell = colReels >= 0 ? String(row[colReels] || '').trim() : '';
        const isCollecting = reelsCell === '수집중';
        const isUncollectable = reelsCell === '수집불가';
        const reelsCollected = isCollecting || isUncollectable ? 0 : parseInt(reelsCell, 10) || 0;

        profiles.push({
          row: index + rowOffset,
          username,
          profileUrl: colProfileUrl >= 0 ? String(row[colProfileUrl] || '') : '',
          followers: colFollowers >= 0 ? parseInt(String(row[colFollowers] || '0'), 10) || 0 : 0,
          sourceMode: colSourceMode >= 0 ? String(row[colSourceMode] || '') : '',
          sourceQuery: colSourceQuery >= 0 ? String(row[colSourceQuery] || '') : '',
          isCollected: reelsCollected > 0,
          isCollecting,
          isUncollectable,
          reelsCollected,
          platform,
        });
      });

      logger.info({ count: profiles.length, sheetName }, 'Loaded profiles from sheet');
      return profiles;
    } catch (error) {
      logger.error({ error }, 'Failed to read sheet');
      throw error;
    }
  }

  /**
   * 수집되지 않은 프로필만 반환
   */
  async getUncollectedProfiles(
    spreadsheetId: string = DEFAULT_SPREADSHEET_ID,
    sheetName: string = DEFAULT_SHEET_NAME
  ): Promise<SheetProfile[]> {
    const profiles = await this.getProfiles(spreadsheetId, sheetName);
    return profiles.filter((p) => !p.isCollected && !p.isUncollectable && !p.isCollecting);
  }

  /**
   * 수집중 상태 표시
   */
  async markAsCollecting(
    spreadsheetId: string,
    sheetName: string,
    rows: number[]
  ): Promise<void> {
    await this.initialize();
    if (!this.sheets) throw new Error('Sheets API not initialized');

    try {
      const col = await this.getOrCreateReelsColumnLetter(spreadsheetId, sheetName);
      const data = rows.map((row) => ({
        range: `${sheetName}!${col}${row}`,
        values: [['수집중']],
      }));

      await this.sheets.spreadsheets.values.batchUpdate({
        spreadsheetId,
        requestBody: {
          valueInputOption: 'USER_ENTERED',
          data,
        },
      });

      logger.info({ count: rows.length }, 'Marked profiles as collecting');
    } catch (error) {
      logger.error({ error }, 'Failed to mark as collecting');
      throw error;
    }
  }

  /**
   * 수집 완료 표시
   */
  async markReelsCollected(
    spreadsheetId: string,
    sheetName: string,
    results: Array<{ row: number; reelsCount: number }>
  ): Promise<void> {
    await this.initialize();
    if (!this.sheets) throw new Error('Sheets API not initialized');

    try {
      const col = await this.getOrCreateReelsColumnLetter(spreadsheetId, sheetName);
      const data = results.map((result) => ({
        range: `${sheetName}!${col}${result.row}`,
        values: [[result.reelsCount]],
      }));

      await this.sheets.spreadsheets.values.batchUpdate({
        spreadsheetId,
        requestBody: {
          valueInputOption: 'USER_ENTERED',
          data,
        },
      });

      logger.info({ count: results.length }, 'Updated reels collection results');
    } catch (error) {
      logger.error({ error }, 'Failed to update collection results');
      throw error;
    }
  }

  /**
   * 수집불가 표시
   */
  async markAsUncollectable(
    spreadsheetId: string,
    sheetName: string,
    rows: number[]
  ): Promise<void> {
    await this.initialize();
    if (!this.sheets) throw new Error('Sheets API not initialized');

    try {
      const col = await this.getOrCreateReelsColumnLetter(spreadsheetId, sheetName);
      const data = rows.map((row) => ({
        range: `${sheetName}!${col}${row}`,
        values: [['수집불가']],
      }));

      await this.sheets.spreadsheets.values.batchUpdate({
        spreadsheetId,
        requestBody: {
          valueInputOption: 'USER_ENTERED',
          data,
        },
      });

      logger.info({ count: rows.length }, 'Marked profiles as uncollectable');
    } catch (error) {
      logger.error({ error }, 'Failed to mark as uncollectable');
      throw error;
    }
  }

  /**
   * 수집중 상태 취소
   */
  async clearCollectingStatus(
    spreadsheetId: string,
    sheetName: string,
    rows: number[]
  ): Promise<void> {
    await this.initialize();
    if (!this.sheets) throw new Error('Sheets API not initialized');

    try {
      const col = await this.getOrCreateReelsColumnLetter(spreadsheetId, sheetName);
      const data = rows.map((row) => ({
        range: `${sheetName}!${col}${row}`,
        values: [['']],
      }));

      await this.sheets.spreadsheets.values.batchUpdate({
        spreadsheetId,
        requestBody: {
          valueInputOption: 'USER_ENTERED',
          data,
        },
      });

      logger.info({ count: rows.length }, 'Cleared collecting status');
    } catch (error) {
      logger.error({ error }, 'Failed to clear collecting status');
      throw error;
    }
  }
}

// Singleton instance
let instance: GoogleSheetsService | null = null;

export function getGoogleSheetsService(): GoogleSheetsService {
  if (!instance) {
    instance = new GoogleSheetsService();
  }
  return instance;
}
