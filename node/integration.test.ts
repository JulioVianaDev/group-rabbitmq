/**
 * test/integration.test.ts
 *
 * Integration tests — requires real RabbitMQ + Redis running.
 *
 * Start with:
 *   docker compose up -d
 *
 * Then run:
 *   npm run test:integration
 */

import { GroupRabbitMQ } from '../src';
import { GroupStateStore } from '../src/store/GroupStateStore';

const AMQP_URL = process.env.AMQP_URL ?? 'amqp://guest:guest@localhost:5672';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';

const TIMEOUT = 15_000;

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function makeMQ<T = unknown>(extra?: object) {
  return new GroupRabbitMQ<T>({
    amqpUrl: AMQP_URL,
    redisUrl: REDIS_URL,
    queuePrefix: `test-${Date.now()}`, // isolated prefix per test run
    requeueDelayMs: 50,               // faster backoff for tests
    maxRequeueAttempts: 5,
    ...extra,
  });
}

// ─── Tests ────────────────────────────────────────────────────────────────────

describe('GroupRabbitMQ — integration', () => {
  describe('ordering within a group', () => {
    it('processes messages in the order they were published', async () => {
      const mq = makeMQ<{ n: number }>();
      await mq.connect();

      const received: number[] = [];

      await mq.consume(async (payload) => {
        received.push(payload.n);
        await sleep(10);
      });

      await mq.subscribeToGroup('grp-order');

      await mq.publish('grp-order', { n: 1 });
      await mq.publish('grp-order', { n: 2 });
      await mq.publish('grp-order', { n: 3 });

      // Wait for all 3 to be processed
      await waitUntil(() => received.length === 3, 8_000);

      expect(received).toEqual([1, 2, 3]);

      await mq.disconnect();
    }, TIMEOUT);
  });

  describe('parallel processing across groups', () => {
    it('processes different groups in parallel', async () => {
      const mq = makeMQ<{ group: string; ts?: number }>();
      await mq.connect();

      const startTimes: Record<string, number> = {};
      const endTimes: Record<string, number> = {};

      await mq.consume(async (payload) => {
        startTimes[payload.group] = Date.now();
        await sleep(200); // simulate work
        endTimes[payload.group] = Date.now();
      }, { maxConcurrentGroups: 10 });

      await mq.subscribeToGroups(['parallel-A', 'parallel-B']);

      await mq.publish('parallel-A', { group: 'A' });
      await mq.publish('parallel-B', { group: 'B' });

      await waitUntil(() => Object.keys(endTimes).length === 2, 5_000);

      // Both started before either finished → they ran in parallel
      const aStart = startTimes['A'];
      const bStart = startTimes['B'];
      const aEnd = endTimes['A'];
      const bEnd = endTimes['B'];

      expect(aStart).toBeLessThan(bEnd);
      expect(bStart).toBeLessThan(aEnd);

      await mq.disconnect();
    }, TIMEOUT);
  });

  describe('maxConcurrentGroups limit', () => {
    it('does not start a 3rd group when limit is 2', async () => {
      const mq = makeMQ<{ group: string }>();
      await mq.connect();

      const processing = new Set<string>();
      const maxSimultaneous = { value: 0 };
      const done: string[] = [];

      await mq.consume(async (payload) => {
        processing.add(payload.group);
        maxSimultaneous.value = Math.max(maxSimultaneous.value, processing.size);
        await sleep(300);
        processing.delete(payload.group);
        done.push(payload.group);
      }, { maxConcurrentGroups: 2 });

      await mq.subscribeToGroups(['limit-A', 'limit-B', 'limit-C']);

      await mq.publish('limit-A', { group: 'limit-A' });
      await mq.publish('limit-B', { group: 'limit-B' });
      await mq.publish('limit-C', { group: 'limit-C' });

      await waitUntil(() => done.length === 3, 10_000);

      expect(maxSimultaneous.value).toBeLessThanOrEqual(2);

      await mq.disconnect();
    }, TIMEOUT);
  });

  describe('same group serialised across messages', () => {
    it('never processes two messages from the same group at the same time', async () => {
      const mq = makeMQ<{ n: number }>();
      await mq.connect();

      let concurrent = 0;
      let maxConcurrent = 0;
      const order: number[] = [];

      await mq.consume(async (payload) => {
        concurrent++;
        maxConcurrent = Math.max(maxConcurrent, concurrent);
        await sleep(50);
        order.push(payload.n);
        concurrent--;
      }, { maxConcurrentGroups: 10 }); // high limit — only serial constraint comes from group

      await mq.subscribeToGroup('serial-grp');

      for (let i = 1; i <= 5; i++) {
        await mq.publish('serial-grp', { n: i });
      }

      await waitUntil(() => order.length === 5, 8_000);

      expect(maxConcurrent).toBe(1);        // never more than 1 at a time
      expect(order).toEqual([1, 2, 3, 4, 5]); // strict order

      await mq.disconnect();
    }, TIMEOUT);
  });

  describe('publishBatch', () => {
    it('correctly assigns sequences across groups', async () => {
      const mq = makeMQ<{ v: string }>();
      await mq.connect();

      const msgs = await mq.publishBatch([
        { groupId: 'batch-1', payload: { v: 'a' } },
        { groupId: 'batch-2', payload: { v: 'b' } },
        { groupId: 'batch-1', payload: { v: 'c' } },
      ]);

      // group batch-1 should have sequences 1 and 2
      const batch1 = msgs.filter((m) => m.groupId === 'batch-1');
      expect(batch1[0].sequence).toBe(1);
      expect(batch1[1].sequence).toBe(2);

      // group batch-2 should have sequence 1 (its own counter)
      const batch2 = msgs.filter((m) => m.groupId === 'batch-2');
      expect(batch2[0].sequence).toBe(1);

      await mq.disconnect();
    }, TIMEOUT);
  });

  describe('GroupStateStore — slot management', () => {
    it('acquires, holds, and releases group slots correctly', async () => {
      const store = new GroupStateStore(REDIS_URL);
      await store.connect();

      const worker = `test-worker-${Date.now()}`;

      // Acquire slot for group-A
      const r1 = await store.tryAcquireGroupSlot(worker, 'slot-A', 2);
      expect(r1).toBe('acquired');

      // Same group — should be "already mine"
      const r2 = await store.tryAcquireGroupSlot(worker, 'slot-A', 2);
      expect(r2).toBe('already_mine');

      // Acquire a second group
      const r3 = await store.tryAcquireGroupSlot(worker, 'slot-B', 2);
      expect(r3).toBe('acquired');

      // At capacity — third group should be rejected
      const r4 = await store.tryAcquireGroupSlot(worker, 'slot-C', 2);
      expect(r4).toBe('at_capacity');

      // Release A — now C should fit
      await store.releaseGroupSlot(worker, 'slot-A');
      const r5 = await store.tryAcquireGroupSlot(worker, 'slot-C', 2);
      expect(r5).toBe('acquired');

      // Another worker should not be able to acquire slot-C (locked by this worker)
      const r6 = await store.tryAcquireGroupSlot('other-worker', 'slot-C', 10);
      expect(r6).toBe('locked_elsewhere');

      await store.clearWorkerState(worker);
      await store.disconnect();
    }, TIMEOUT);
  });
});

// ─── Utility ──────────────────────────────────────────────────────────────────

async function waitUntil(condition: () => boolean, timeoutMs: number): Promise<void> {
  const start = Date.now();
  while (!condition()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitUntil timed out after ${timeoutMs}ms`);
    }
    await sleep(50);
  }
}
