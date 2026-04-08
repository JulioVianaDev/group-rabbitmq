import sqlite3 from 'sqlite3';

const DB_PATH = 'example/consumption-log.sqlite';

function run(db: sqlite3.Database, sql: string): Promise<void> {
  return new Promise((resolve, reject) => {
    db.run(sql, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

async function main(): Promise<void> {
  const db = new sqlite3.Database(DB_PATH);
  try {
    await run(
      db,
      `CREATE TABLE IF NOT EXISTS consumed_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        worker_id TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        consumed_at TEXT NOT NULL
      );`
    );
    await run(db, 'DELETE FROM consumed_messages');
    try {
      await run(db, `DELETE FROM sqlite_sequence WHERE name = 'consumed_messages'`);
    } catch {
      // sqlite_sequence may not exist until first AUTOINCREMENT insert
    }
    console.log(`Cleared all rows in consumed_messages (${DB_PATH})`);
  } finally {
    await new Promise<void>((resolve, reject) => {
      db.close((err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }
}

main().catch((err) => {
  console.error('Clean DB failed:', err);
  process.exit(1);
});
