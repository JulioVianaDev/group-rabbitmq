import { GroupRabbitMQ } from '../src';

const AMQP_URL = process.env.AMQP_URL ?? 'amqp://guest:guest@127.0.0.1:5672';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

const groups = ['group-1', 'group-2', 'group-3', 'group-4', 'group-5',];

async function run(): Promise<void> {
  const mq = new GroupRabbitMQ<{ value: string; group: string; index: number }>({
    amqpUrl: AMQP_URL,
    redisUrl: REDIS_URL,
    queuePrefix: 'example',
  });

  await mq.connect();

  for (const groupId of groups) {
    for (let i = 1; i <= 3; i++) {
      const payload = {
        value: `${groupId}-message-${i}`,
        group: groupId,
        index: i,
      };
      const message = await mq.publish(groupId, payload);
      console.log(`PUBLISHED group=${groupId} seq=${message.sequence} value=${payload.value}`);
    }
  }

  await mq.disconnect();
  console.log('Producer done: sent 5 groups x 3 messages.');
}

run().catch((err) => {
  console.error('Producer failed:', err);
  process.exit(1);
});
