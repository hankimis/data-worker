import { google, sheets_v4 } from 'googleapis';
import { config } from '../config/index.js';
import { createChildLogger } from '../utils/logger.js';

const logger = createChildLogger('google-sheets');

export interface SheetIdEntry {
  id: string;
  platform: 'instagram' | 'tiktok';
  type: 'profile' | 'hashtag' | 'keyword';
  projectId?: number;
}

export class GoogleSheetsService {
  private sheets: sheets_v4.Sheets;
  private initialized = false;

  constructor() {
    const auth = new google.auth.JWT({
      email: config.google.serviceAccountEmail,
      key: config.google.privateKey,
      scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
    });

    this.sheets = google.sheets({ version: 'v4', auth });
  }

  async initialize(): Promise<void> {
    if (this.initialized) return;

    try {
      // Test connection by making a simple request
      logger.info('Initializing Google Sheets connection...');
      this.initialized = true;
      logger.info('Google Sheets service initialized');
    } catch (error) {
      logger.error({ error }, 'Failed to initialize Google Sheets');
      throw error;
    }
  }

  /**
   * Fetch ID list from a Google Sheet
   * Expected sheet format:
   * | ID | Platform | Type | ProjectID (optional) |
   * | @username | instagram | profile | 1 |
   * | #hashtag | tiktok | hashtag | 2 |
   */
  async fetchIdList(spreadsheetId: string, range: string): Promise<SheetIdEntry[]> {
    try {
      logger.info({ spreadsheetId, range }, 'Fetching ID list from sheet');

      const response = await this.sheets.spreadsheets.values.get({
        spreadsheetId,
        range,
      });

      const rows = response.data.values;

      if (!rows || rows.length === 0) {
        logger.warn('No data found in sheet');
        return [];
      }

      // Skip header row
      const dataRows = rows.slice(1);

      const entries: SheetIdEntry[] = dataRows
        .filter((row) => row[0] && row[1]) // Must have ID and platform
        .map((row) => ({
          id: String(row[0]).trim(),
          platform: (String(row[1]).toLowerCase() as 'instagram' | 'tiktok'),
          type: (String(row[2] || 'profile').toLowerCase() as 'profile' | 'hashtag' | 'keyword'),
          projectId: row[3] ? parseInt(row[3], 10) : undefined,
        }));

      logger.info({ count: entries.length }, 'Fetched ID entries from sheet');
      return entries;
    } catch (error) {
      logger.error({ error, spreadsheetId, range }, 'Failed to fetch sheet data');
      throw error;
    }
  }

  /**
   * Fetch multiple sheets and combine results
   */
  async fetchMultipleSheets(
    sheets: Array<{ spreadsheetId: string; range: string }>
  ): Promise<SheetIdEntry[]> {
    const results = await Promise.all(
      sheets.map((sheet) => this.fetchIdList(sheet.spreadsheetId, sheet.range))
    );

    return results.flat();
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
