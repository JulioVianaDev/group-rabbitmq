import { GroupRabbitMQ } from '../src';
import { ConsumptionStore } from './consumptionStore';

const AMQP_URL = process.env.AMQP_URL ?? 'amqp://guest:guest@127.0.0.1:5672';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const MANAGEMENT_URL = process.env.MANAGEMENT_URL ?? 'http://127.0.0.1:15672';
const WORKER_ID = process.env.WORKER_ID ?? `worker-${Math.random().toString(36).slice(2, 8)}`;
const MAX_CONCURRENT_GROUPS = Number(process.env.MAX_CONCURRENT_GROUPS ?? 3);

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
      dynamicWorkerBalancing: true,
    }
  );

  console.log(`${WORKER_ID} ready (dynamic worker balancing enabled).`);

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
