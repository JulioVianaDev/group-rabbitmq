import sqlite3 from 'sqlite3';

type ReportRow = {
  group_id: string;
  sequence: number;
  worker_id: string;
  consumed_at: string;
};

const DB_PATH = 'example/consumption-log.sqlite';

function all<T>(db: sqlite3.Database, sql: string, params: unknown[] = []): Promise<T[]> {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows as T[]);
    });
  });
}

async function run(): Promise<void> {
  const db = new sqlite3.Database(DB_PATH);
  const rows = await all<ReportRow>(
    db,
    `SELECT group_id, sequence, worker_id, consumed_at
     FROM consumed_messages
     ORDER BY group_id, consumed_at`
  );

  if (rows.length === 0) {
    console.log('No consumed messages found yet.');
    db.close();
    return;
  }

  const byGroup = new Map<string, ReportRow[]>();
  for (const row of rows) {
    const arr = byGroup.get(row.group_id) ?? [];
    arr.push(row);
    byGroup.set(row.group_id, arr);
  }

  console.log(`DB: ${DB_PATH}`);
  console.log(`Total consumed rows: ${rows.length}`);

  for (const [groupId, groupRows] of byGroup.entries()) {
    const sequenceList = groupRows.map((r) => r.sequence);
    const expected = [...sequenceList].sort((a, b) => a - b);
    const isOrdered = sequenceList.every((value, idx) => value === expected[idx]);
    const workers = [...new Set(groupRows.map((r) => r.worker_id))].join(', ');

    console.log('');
    console.log(
      `${groupId} | ordered=${isOrdered ? 'YES' : 'NO'} | workers=[${workers}] | seq=[${sequenceList.join(', ')}]`
    );
  }

  db.close();
}

run().catch((err) => {
  console.error('Report failed:', err);
  process.exit(1);
});
