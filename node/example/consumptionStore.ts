import sqlite3 from 'sqlite3';

export interface ConsumptionLogRow {
  groupId: string;
  sequence: number;
  workerId: string;
  payloadJson: string;
  consumedAt: string;
}

export class ConsumptionStore {
  private readonly db: sqlite3.Database;

  constructor(private readonly dbPath = 'example/consumption-log.sqlite') {
    this.db = new sqlite3.Database(this.dbPath);
  }

  async init(): Promise<void> {
    await this.exec('PRAGMA journal_mode = WAL;');
    await this.exec('PRAGMA busy_timeout = 5000;');
    await this.exec(`
      CREATE TABLE IF NOT EXISTS consumed_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        worker_id TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        consumed_at TEXT NOT NULL
      );
    `);
    await this.exec(`
      CREATE INDEX IF NOT EXISTS idx_consumed_group_seq
      ON consumed_messages(group_id, sequence);
    `);
    await this.exec(`
      CREATE INDEX IF NOT EXISTS idx_consumed_at
      ON consumed_messages(consumed_at);
    `);
  }

  async logConsumed(row: ConsumptionLogRow): Promise<void> {
    await this.run(
      `INSERT INTO consumed_messages (group_id, sequence, worker_id, payload_json, consumed_at)
       VALUES (?, ?, ?, ?, ?)`,
      [row.groupId, row.sequence, row.workerId, row.payloadJson, row.consumedAt]
    );
  }

  async close(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.db.close((err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  private async exec(sql: string): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.db.exec(sql, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  private async run(sql: string, params: unknown[]): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.db.run(sql, params, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }
}
