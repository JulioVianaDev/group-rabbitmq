import { GroupRabbitMQ } from '../src';
import { ConsumptionStore } from './consumptionStore';
import Redis from 'ioredis';

const AMQP_URL = process.env.AMQP_URL ?? 'amqp://guest:guest@127.0.0.1:5672';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const MANAGEMENT_URL = process.env.MANAGEMENT_URL ?? 'http://127.0.0.1:15672';
const WORKER_ID = process.env.WORKER_ID ?? `worker-${Math.random().toString(36).slice(2, 8)}`;
const MAX_CONCURRENT_GROUPS = Number(process.env.MAX_CONCURRENT_GROUPS ?? 3);
const REBALANCE_INTERVAL_MS = Number(process.env.REBALANCE_INTERVAL_MS ?? 2000);
const WORKER_HEARTBEAT_TTL_SEC = Number(process.env.WORKER_HEARTBEAT_TTL_SEC ?? 10);

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function hashToInt(input: string): number {
  let hash = 0;
  for (let i = 0; i < input.length; i++) {
    hash = (hash * 31 + input.charCodeAt(i)) >>> 0;
  }
  return hash;
}

function shouldThisWorkerHandle(groupId: string, workers: string[]): boolean {
  if (workers.length <= 1) return true;
  const slot = hashToInt(groupId) % workers.length;
  return workers[slot] === WORKER_ID;
}

async function run(): Promise<void> {
  const consumptionStore = new ConsumptionStore();
  await consumptionStore.init();
  const redis = new Redis(REDIS_URL, {
    maxRetriesPerRequest: null,
    enableReadyCheck: true,
  });
  redis.on('error', (err) => {
    console.warn(`${WORKER_ID} redis warning: ${err.message}`);
  });
  const workersSetKey = 'example:workers:active';
  const workersHeartbeatPrefix = 'example:workers:heartbeat:';
  const subscribedGroups = new Set<string>();

  const mq = new GroupRabbitMQ<{ value: string; group: string; index: number }>({
    amqpUrl: AMQP_URL,
    redisUrl: REDIS_URL,
    queuePrefix: 'example',
    managementUrl: MANAGEMENT_URL,
  });

  await mq.connect();

  await mq.consume(
    async (payload, ctx) => {
      const startedAt = new Date().toISOString();
      console.log(
        `[${startedAt}] ${WORKER_ID} START group=${ctx.groupId} seq=${ctx.sequence} value=${payload.value}`
      );
      await consumptionStore.logConsumed({
        groupId: ctx.groupId,
        sequence: ctx.sequence,
        workerId: WORKER_ID,
        payloadJson: JSON.stringify(payload),
        consumedAt: startedAt,
      });
      await sleep(1000);
      const endedAt = new Date().toISOString();
      console.log(`[${endedAt}] ${WORKER_ID} END   group=${ctx.groupId} seq=${ctx.sequence}`);
    },
    {
      workerId: WORKER_ID,
      maxConcurrentGroups: MAX_CONCURRENT_GROUPS,
    }
  );

  if (!mq.discovery) {
    throw new Error('Group discovery is not configured.');
  }

  const registerWorker = async (): Promise<void> => {
    await redis.sadd(workersSetKey, WORKER_ID);
    await redis.set(`${workersHeartbeatPrefix}${WORKER_ID}`, '1', 'EX', WORKER_HEARTBEAT_TTL_SEC);
  };

  const getActiveWorkers = async (): Promise<string[]> => {
    const allWorkers = await redis.smembers(workersSetKey);
    const workerStates = await Promise.all(
      allWorkers.map(async (id) => ({
        id,
        alive: (await redis.exists(`${workersHeartbeatPrefix}${id}`)) === 1,
      }))
    );
    const activeWorkers = workerStates.filter((w) => w.alive).map((w) => w.id).sort();
    const inactiveWorkers = workerStates.filter((w) => !w.alive).map((w) => w.id);
    if (inactiveWorkers.length > 0) {
      await redis.srem(workersSetKey, ...inactiveWorkers);
    }
    return activeWorkers;
  };

  const rebalanceGroups = async (): Promise<void> => {
    const groups = await mq.discovery!.discoverGroups();
    const activeWorkers = await getActiveWorkers();

    for (const groupId of groups) {
      const iOwnGroup = shouldThisWorkerHandle(groupId, activeWorkers);
      const isSubscribed = subscribedGroups.has(groupId);

      if (iOwnGroup && !isSubscribed) {
        await mq.subscribeToGroup(groupId);
        subscribedGroups.add(groupId);
        console.log(`${WORKER_ID} subscribed group=${groupId} workers=${activeWorkers.join(',')}`);
      } else if (!iOwnGroup && isSubscribed) {
        await mq.unsubscribeFromGroup(groupId);
        subscribedGroups.delete(groupId);
        console.log(`${WORKER_ID} unsubscribed group=${groupId} workers=${activeWorkers.join(',')}`);
      }
    }
  };

  const heartbeatTimer = setInterval(async () => {
    try {
      await registerWorker();
    } catch (err) {
      console.warn(`${WORKER_ID} heartbeat warning: ${(err as Error).message}`);
    }
  }, Math.max(1000, Math.floor((WORKER_HEARTBEAT_TTL_SEC * 1000) / 2)));

  const rebalanceTimer = setInterval(async () => {
    await rebalanceGroups().catch((err) =>
      console.error(`${WORKER_ID} rebalance error:`, err)
    );
  }, REBALANCE_INTERVAL_MS);

  await registerWorker();
  try {
    await rebalanceGroups();
  } catch (err) {
    console.warn(`${WORKER_ID} initial rebalance warning: ${(err as Error).message}`);
  }

  mq.discovery.watch(async () => {
    await rebalanceGroups();
  }, 1000);

  console.log(
    `${WORKER_ID} ready. dynamic rebalance enabled, maxConcurrentGroups=${MAX_CONCURRENT_GROUPS}`
  );

  const shutdown = async () => {
    console.log(`${WORKER_ID} shutting down...`);
    clearInterval(heartbeatTimer);
    clearInterval(rebalanceTimer);
    await redis.del(`${workersHeartbeatPrefix}${WORKER_ID}`);
    await redis.srem(workersSetKey, WORKER_ID);
    await mq.disconnect();
    await redis.quit();
    await consumptionStore.close();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

run().catch((err) => {
  console.error('Worker failed:', err);
  process.exit(1);
});
