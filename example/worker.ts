import { GroupRabbitMQ } from '../src';
import { ConsumptionStore } from './consumptionStore';

const AMQP_URL = process.env.AMQP_URL ?? 'amqp://guest:guest@localhost:5672';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';
const MANAGEMENT_URL = process.env.MANAGEMENT_URL ?? 'http://localhost:15672';
const WORKER_ID = process.env.WORKER_ID ?? `worker-${Math.random().toString(36).slice(2, 8)}`;
const MAX_CONCURRENT_GROUPS = Number(process.env.MAX_CONCURRENT_GROUPS ?? 3);
const WORKER_INDEX = Number(process.env.WORKER_INDEX ?? 0);
const WORKER_COUNT = Number(process.env.WORKER_COUNT ?? 1);

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

function shouldThisWorkerHandle(groupId: string): boolean {
  if (WORKER_COUNT <= 1) return true;
  return hashToInt(groupId) % WORKER_COUNT === WORKER_INDEX;
}

async function run(): Promise<void> {
  const consumptionStore = new ConsumptionStore();
  await consumptionStore.init();

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

  const subscribeIfOwned = async (groupId: string): Promise<void> => {
    if (!shouldThisWorkerHandle(groupId)) return;
    await mq.subscribeToGroup(groupId);
    console.log(
      `${WORKER_ID} subscribed group=${groupId} slot=${WORKER_INDEX}/${WORKER_COUNT}`
    );
  };

  const existingGroups = await mq.discovery.discoverGroups();
  await Promise.all(existingGroups.map(subscribeIfOwned));

  mq.discovery.watch(async (groupId) => {
    await subscribeIfOwned(groupId);
  }, 1000);

  console.log(
    `${WORKER_ID} ready. workerIndex=${WORKER_INDEX}, workerCount=${WORKER_COUNT}, maxConcurrentGroups=${MAX_CONCURRENT_GROUPS}`
  );

  const shutdown = async () => {
    console.log(`${WORKER_ID} shutting down...`);
    await mq.disconnect();
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
