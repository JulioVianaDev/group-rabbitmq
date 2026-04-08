# group-rabbitmq

A RabbitMQ library that adds **group-based ordering** and **per-worker concurrency limits** on top of standard AMQP queues.

## The problem it solves

Given messages:
```ts
[
  { groupId: "item1", value: "batata" },
  { groupId: "item2", value: "tomate" },
  { groupId: "item1", value: "ariba"  },
]
```

You want:
- `item1/batata` and `item2/tomate` processed **in parallel** (different groups)
- `item1/ariba` processed **after** `item1/batata` (same group → strict order)
- A worker to process **at most N different groups** at the same time

## Install

```bash
npm install group-rabbitmq
# peer deps
npm install amqplib ioredis
```

## Quick start

```ts
import { GroupRabbitMQ } from 'group-rabbitmq';

const mq = new GroupRabbitMQ({
  amqpUrl: 'amqp://localhost',
  redisUrl: 'redis://localhost',
});

await mq.connect();

// Publish
await mq.publish('order-1', { item: 'batata', qty: 3 });
await mq.publish('order-1', { item: 'ariba',  qty: 1 }); // after batata
await mq.publish('order-2', { item: 'tomate', qty: 5 }); // parallel to order-1

// Consume — process at most 2 groups simultaneously on this worker
await mq.consume(async (payload, ctx) => {
  console.log(`[${ctx.groupId}] seq=${ctx.sequence}`, payload);
}, { maxConcurrentGroups: 2 });

await mq.subscribeToGroups(['order-1', 'order-2']);
```

---

## How it works

### RabbitMQ topology

```
Producer
  │  publish(groupId="item1", ...)
  ▼
group.exchange  (direct)
  │  routingKey = groupId
  ├──▶  queue: group.item1   (durable, FIFO)
  ├──▶  queue: group.item2
  └──▶  queue: group.itemN

group.dlx  (fanout)
  └──▶  queue: group.dead    (failed / expired messages)
```

Each `groupId` gets its own dedicated queue. The exchange routes by `routingKey = groupId`, so messages always land in the right group's queue.

### How `prefetch=1` works here

**`prefetch=1` does NOT lock the exchange or any other queue.**

It is scoped to a single AMQP channel, and each group gets its own channel:

```
Worker process
  ├── channel A  (prefetch=1)  ──▶  consumes group.item1
  ├── channel B  (prefetch=1)  ──▶  consumes group.item2
  └── channel C  (prefetch=1)  ──▶  consumes group.item3
```

Effect per channel:
- RabbitMQ delivers **at most 1 unacked message** from that channel's queue
- The next message is only delivered **after the current one is acked**
- Other channels (other groups) are completely unaffected

This means:
- `item1` and `item2` run **in parallel** (different channels)
- Within `item1`, messages run **serially** (same channel, prefetch=1)
- A slow `item1` does not block `item2` at all

### Concurrency limit (maxConcurrentGroups)

Redis tracks which groups each worker is currently processing:

```
worker:{workerId}:active  →  SET { "item1", "item2" }
group:{groupId}:lock      →  STRING "{workerId}"   (with TTL)
```

When a new message arrives for a **new** group:
- If `|active| < maxConcurrentGroups` → acquire slot → process
- If `|active| >= maxConcurrentGroups` → nack → republish after backoff delay

When a message arrives for an **already active** group on this worker:
- Always accepted immediately (no slot check needed)

When a group's queue becomes empty after processing:
- Slot is released → worker can now pick up a new group

### Distributed lock (cluster safety)

Each `groupId` has a Redis lock (`SET NX PX`). Only one worker in the cluster holds the lock for a given group at a time. If a worker crashes, the lock expires automatically (default: 60s) and another worker can take over.

---

## Configuration

```ts
const mq = new GroupRabbitMQ({
  amqpUrl: 'amqp://localhost',          // required
  redisUrl: 'redis://localhost',         // required
  exchangeName: 'group.exchange',        // default
  exchangeType: 'direct',               // 'direct' | 'topic'
  queuePrefix: 'group',                 // queues named group.<groupId>
  dlxExchangeName: 'group.dlx',         // dead-letter exchange
  messageTtl: 1_800_000,               // 30min, before going to DLX
  maxRequeueAttempts: 3,                // before giving up and DLX-ing
  requeueDelayMs: 200,                  // base backoff (exponential)
});
```

## Consume options

```ts
await mq.consume(handler, {
  maxConcurrentGroups: 5,   // default: Infinity (no limit)
  workerId: 'my-worker-1',  // default: auto uuid
});
```

## Message context

```ts
await mq.consume(async (payload, ctx) => {
  ctx.groupId    // e.g. "item1"
  ctx.messageId  // unique UUID
  ctx.sequence   // monotonic counter per group
  ctx.publishedAt // ISO timestamp

  // Soft requeue — put message back at END of group queue
  await ctx.requeue();
});
```

## Error handling

- **Handler throws** → message sent to `group.dead` queue (DLX), slot released
- **At capacity** → message requeued after exponential backoff, up to `maxRequeueAttempts`
- **Worker crash** → Redis lock expires after 60s, another worker can acquire the group
- **Broker restart** → queues and exchange are durable, messages survive

---

## Running the example

```bash
# Start dependencies
docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:3-management
docker run -d -p 6379:6379 redis

# Install and run
npm install
npx ts-node examples/basic-usage.ts
```

Expected output:
```
  [12:00:00.100] ▶ START  group=item1  seq=1  val=batata
  [12:00:00.102] ▶ START  group=item2  seq=1  val=tomate   ← parallel
  [12:00:00.600] ✓ END    group=item1  seq=1
  [12:00:00.601] ▶ START  group=item1  seq=2  val=ariba    ← next in order
  [12:00:00.703] ✓ END    group=item2  seq=1
  [12:00:00.703] ▶ START  group=item3  seq=1  val=cebola   ← slot freed
  [12:00:01.100] ✓ END    group=item1  seq=2
  [12:00:00.900] ✓ END    group=item3  seq=1
```
