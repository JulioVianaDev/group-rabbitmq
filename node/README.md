# Group RabbitMQ

A TypeScript library for **RabbitMQ** that combines:

- **Per-group strict ordering** — messages with the same `groupId` are processed one after another.
- **Parallelism across groups** — different `groupId`s can be processed at the same time.
- **Per-process fan-out limit** — cap how many *distinct* groups one worker handles concurrently (`maxConcurrentGroups`).
- **Cluster safety** — Redis-backed locks and slots so only one worker in the cluster owns a given group at a time.
- **Optional horizontal scaling** — dynamic subscription balancing across processes when you enable **dynamic worker balancing** and the **management API**.

Dependencies: **RabbitMQ**, **Redis**, and (for discovery / balancing) the **RabbitMQ management plugin** (HTTP API on port `15672` in the default Docker image).

### Go publishers (same queues as Node)

A compatible **Go publisher** lives in [`../go/`](../go/README.md): it uses the same Redis sequence keys, exchange topology, and JSON envelope, so you can **publish from Go** and **consume with Node workers** without changing the consumer code. Run the example with `go run ./example/producer` from the `go` directory.

**npm package:** [`group-rabbitmq`](https://www.npmjs.com/package/group-rabbitmq) (published from the `node/` directory).

---

## Motivation

Typical AMQP usage is either “one queue, one consumer” (no parallelism) or “many consumers on one queue” (parallelism but **no ordering** per logical stream).

Many domains need **both**:

- Orders for **the same customer or aggregate** must be handled **in order**.
- Work for **different** customers can run **in parallel**.

This library models each `groupId` as its **own queue**, routes with a **direct exchange**, and uses **prefetch = 1 per group channel** so ordering is preserved per group while other groups are not blocked.

It also uses **Redis** to:

- Assign a **monotonic sequence number** per group (for observability and tests).
- Track **which groups a worker is actively processing** and enforce **`maxConcurrentGroups`**.
- Hold a **distributed lock per group** so that across machines, only one worker processes a given group at a time.

---

## Concepts

| Concept | Meaning |
|--------|---------|
| **Group** | Identified by `groupId` (string). All messages for that id share one queue and one ordering stream. |
| **Sequence** | Stored in Redis per group; incremented on publish so you can verify ordering and debug. |
| **Worker** | One OS process running `GroupRabbitMQ` with a consumer. Identified by `workerId` (default: UUID). |
| **Slot** | Permission for this worker to process another *new* group when under `maxConcurrentGroups`. |
| **Lock** | Redis key per group so only one worker in the cluster processes that group at a time. |
| **Dynamic worker balancing** | Optional mode: workers register in Redis; each group is assigned to exactly one worker by **consistent hashing** over live workers; subscriptions are updated on a timer. Requires **management URL** to list queues. |

---

## Architecture (high level)

```
Publishers                RabbitMQ                           Consumers
    │                         │                                  │
    │  routingKey = groupId   │   queue per group:               │
    ├────────────────────────►│   {queuePrefix}.{groupId}        │
    │                         │                                  │
    │                         │   ◄── prefetch=1 per group       │
    │                         │        (separate AMQP channel)   │
    │                         │                                  │
    │                         │                    Redis: locks,  │
    │                         │                    slots, sequence│
```

- **Exchange**: `direct` by default; `routingKey = groupId`.
- **Queues**: `{queuePrefix}.{groupId}` (durable). Dead-letter exchange for poison / TTL.
- **Single-active-consumer**: group queues are declared with `x-single-active-consumer` so if multiple workers *subscribe* to the same queue, RabbitMQ keeps **one active consumer** and ordering is not split across consumers on the same queue.

---

## Tradeoffs

| Choice | Benefit | Cost |
|--------|---------|------|
| **One queue per group** | Strong ordering per group; natural isolation | Many queues if you have many groups; operations must tolerate queue proliferation |
| **Redis for sequence + locks** | Fast coordination; survives restarts with persistence | Extra dependency; must size Redis and monitor |
| **Prefetch 1 per group channel** | Simple serial processing per group | One channel per subscribed group per worker (more channels than a single shared consumer) |
| **Dynamic balancing** | Add/remove workers; groups migrate by hash | Requires **management API**; periodic rebalance adds latency to ownership changes; **hash changes** when worker count changes (groups may move) |
| **Consistent hashing** | Stable mapping given worker set | Not “fair” per message; hot groups stay on one worker |

---

## Limitations

- **Group IDs are dynamic strings**: you must eventually **subscribe** to queues (manually or via `discovery` / **dynamic worker balancing**). Unknown groups do not create consumers until subscribed.
- **Management API**: listing queues for discovery/balancing needs the plugin and correct URL/credentials.
- **Rebalancing**: when worker count changes, **which worker owns a group** can change. In-flight work and queue depth must be acceptable for your use case.
- **Ordering**: guaranteed **per group queue** for messages successfully consumed in order. Retries for capacity/lock use **delayed requeue** to avoid moving messages to the tail of the queue (which would break order).
- **AMQP reconnect**: if the broker connection drops, the consumer must be recreated (the library clears the consumer on reconnect; you should reconnect and call `consume` / balancing again in your app if you design for long-lived auto-reconnect).
- **Not a Kafka replacement**: this is AMQP + Redis patterns; throughput and retention semantics are those of RabbitMQ.

---

## Install

From [npm](https://www.npmjs.com/package/group-rabbitmq) (package is published from the `node/` folder in the repo):

```bash
npm install group-rabbitmq
```

Peer-style runtime dependencies are bundled; you need **RabbitMQ** and **Redis** at runtime.

For local development of this repo, use `npm install` inside `node/` and Docker Compose (see below).

---

## Quick start

```ts
import { GroupRabbitMQ } from 'group-rabbitmq';

const mq = new GroupRabbitMQ({
  amqpUrl: 'amqp://guest:guest@127.0.0.1:5672',
  redisUrl: 'redis://127.0.0.1:6379',
});

await mq.connect();

await mq.publish('order-1', { item: 'batata', qty: 3 });

await mq.consume(async (payload, ctx) => {
  console.log(ctx.groupId, ctx.sequence, payload);
}, { maxConcurrentGroups: 2 });

await mq.subscribeToGroups(['order-1', 'order-2']);

// ... later
await mq.disconnect();
```

---

## Configuration

`GroupRabbitMQ` accepts:

| Field | Description |
|-------|-------------|
| `amqpUrl` | **Required.** AMQP connection string. |
| `redisUrl` | **Required.** Used for sequence, locks, slots, and (optional) worker registry. |
| `exchangeName` | Default: `group.exchange`. |
| `exchangeType` | Default: `direct`. |
| `queuePrefix` | Default: `group`. Queues: `{prefix}.{groupId}`. Also namespaces Redis keys for this deployment. |
| `dlxExchangeName` | Default: `group.dlx`. Dead-letter exchange. |
| `messageTtl` | Message TTL before DLX (ms). |
| `maxRequeueAttempts` | Max retries when temporarily cannot lock / capacity. |
| `requeueDelayMs` | Base delay for exponential backoff. |
| `managementUrl` | e.g. `http://127.0.0.1:15672` — for `GroupDiscovery` and **dynamic worker balancing**. |
| `managementUsername` / `managementPassword` | Optional; default `guest` / `guest`. |
| `maxRetries` / `retryDelayMs` / `maxRetryDelayMs` | AMQP connection reconnect tuning. |

---

## Publishing

```ts
const msg = await mq.publish(groupId, payload, {
  messageId: 'optional-id',
  persistent: true,
});
// msg.sequence is per-group monotonic
```

`publishBatch` publishes multiple messages and assigns sequences per group.

---

## Consuming

```ts
await mq.consume(handler, {
  workerId: 'my-worker',           // optional; default UUID
  maxConcurrentGroups: 5,          // default: Infinity (no per-worker group cap)

  // Optional: horizontal scaling + dynamic groups
  dynamicWorkerBalancing: true,    // requires managementUrl
  rebalanceIntervalMs: 5000,       // how often to re-run discovery + subscription map
  workerHeartbeatTtlSec: 10,      // Redis liveness TTL for workers
});
```

Handler receives `(payload, ctx)` where `ctx` includes `groupId`, `messageId`, `sequence`, `publishedAt`, and `requeue()` (basic requeue to the group queue).

### Dynamic worker balancing

When `dynamicWorkerBalancing: true`:

- Each process registers in Redis under `{queuePrefix}:workers:active` and heartbeats under `{queuePrefix}:workers:heartbeat:{workerId}`.
- On each rebalance tick, the library lists group queues via the **management API**, computes active workers, and assigns each `groupId` to a worker with **consistent hashing**: `hash(groupId) % N === slot` among sorted worker ids.
- **Subscribe** to groups you own; **unsubscribe** from groups you no longer own.

This requires **`managementUrl`** in the main config. Without it, `dynamicWorkerBalancing` throws.

---

## Discovery (without full balancing)

If you only need to discover queue names:

```ts
const mq = new GroupRabbitMQ({ ..., managementUrl: 'http://127.0.0.1:15672' });
await mq.connect();
await mq.consume(handler);
await mq.watchGroups(5000); // polls management API, subscribes as groups appear
```

---

## Redis keys (namespaced by `queuePrefix`)

All state keys are prefixed with `{queuePrefix}:` when `queuePrefix` is set (recommended).

- `{queuePrefix}:worker:{workerId}:active` — set of group ids active on this worker.
- `{queuePrefix}:group:{groupId}:lock` — distributed lock (TTL).
- `{queuePrefix}:group:{groupId}:sequence` — sequence counter.
- With balancing: `{queuePrefix}:workers:active`, `{queuePrefix}:workers:heartbeat:{workerId}`.

---

## Error handling (summary)

- Handler throws → message rejected to DLX (no infinite loop); slot released.
- At capacity / locked elsewhere → delayed **requeue** (in-order) up to `maxRequeueAttempts`, then DLX.
- Worker crash → lock TTL expires; another worker can take the group.

---

## Local development

### Docker Compose

```bash
docker compose up -d
```

Starts RabbitMQ (AMQP `5672`, management `15672`) and Redis (`6379`).

### Scripts

| Command | Purpose |
|---------|---------|
| `npm test` | Unit tests |
| `npm run test:integration` | Integration tests (needs broker + Redis) |
| `npm run example:producer` | Example publisher |
| `npm run example:worker:1` … `:3` | Example workers (with `dynamicWorkerBalancing` in code) |
| `npm run example:report` | SQLite consumption report (example only) |
| `npm run example:db-clean` | Clear example SQLite log |

---

## License

ISC (see `package.json`).
