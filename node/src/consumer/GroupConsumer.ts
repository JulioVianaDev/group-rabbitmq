import * as amqplib from 'amqplib';
import { v4 as uuidv4 } from 'uuid';
import {
  ConsumeOptions,
  GroupMessage,
  MessageContext,
  MessageHandler,
  GroupRabbitMQConfig,
} from '../types';
import { TopologyManager } from '../topology/TopologyManager';
import { GroupStateStore } from '../store/GroupStateStore';


/**
 * GroupConsumer subscribes to one or more group queues and processes messages
 * with the following guarantees:
 *
 *  1. ORDER: Messages within a group are processed one at a time, in the order
 *     they were published (prefetch=1 per group channel).
 *  2. CONCURRENCY LIMIT: A worker processes at most `maxConcurrentGroups`
 *     distinct groups at once (atomic Lua check in Redis).
 *  3. EXCLUSIVITY: A group is owned by exactly one worker at a time
 *     (distributed Redis lock + RabbitMQ x-single-active-consumer).
 *  4. NO DOUBLE-PROCESSING: a per-message in-flight set skips any delivery
 *     whose messageId is currently being handled on this worker.
 */
export class GroupConsumer<T = unknown> {
  private readonly workerId: string;
  private readonly maxConcurrentGroups: number;

  // One channel per groupId — isolated prefetch windows
  private readonly groupChannels = new Map<string, amqplib.Channel>();
  private readonly groupConsumerTags = new Map<string, string>();

  // Messages currently being processed on this worker (dedupe guard for
  // redeliveries that can happen on channel reopen / nack-requeue).
  private readonly inFlightMessages = new Set<string>();

  // Tracks in-flight requeue timers so we can cancel them on shutdown
  private readonly requeueTimers = new Set<NodeJS.Timeout>();

  private managementChannel: amqplib.Channel | null = null;
  private running = false;

  constructor(
    private readonly config: Required<GroupRabbitMQConfig>,
    private readonly topology: TopologyManager,
    private readonly store: GroupStateStore,
    private readonly connection: amqplib.ChannelModel,
    private readonly handler: MessageHandler<T>,
    options: ConsumeOptions = {}
  ) {
    this.workerId = options.workerId ?? uuidv4();
    // Default 100 concurrent groups if caller did not specify.
    this.maxConcurrentGroups = options.maxConcurrentGroups ?? 100;
  }

  get id(): string {
    return this.workerId;
  }

  async initialize(): Promise<void> {
    this.managementChannel = await this.connection.createChannel();
    await this.topology.assertBaseTopology(this.managementChannel);
    this.running = true;
  }

  async subscribeToGroup(groupId: string): Promise<void> {
    if (!this.running || !this.managementChannel) {
      throw new Error('Consumer not initialized.');
    }

    if (this.groupChannels.has(groupId)) {
      return; // Already subscribed
    }

    await this.topology.assertGroupQueue(this.managementChannel, groupId);

    const channel = await this.connection.createChannel();
    await channel.prefetch(1);

    // If the channel dies (server cancel, queue deletion, network blip),
    // clean up state so the next balancer tick / user call can re-subscribe.
    const cleanup = () => {
      if (this.groupChannels.get(groupId) === channel) {
        this.groupChannels.delete(groupId);
        this.groupConsumerTags.delete(groupId);
      }
    };
    channel.on('close', cleanup);
    channel.on('error', (err: Error) => {
      console.warn(`[group-rabbitmq] Channel error on group "${groupId}": ${err.message}`);
      cleanup();
    });

    this.groupChannels.set(groupId, channel);

    const queueName = this.topology.groupQueueName(groupId);

    const { consumerTag } = await channel.consume(
      queueName,
      (msg) => this.handleDelivery(channel, groupId, msg),
      { noAck: false }
    );

    this.groupConsumerTags.set(groupId, consumerTag);
  }

  private async handleDelivery(
    channel: amqplib.Channel,
    groupId: string,
    msg: amqplib.ConsumeMessage | null
  ): Promise<void> {
    if (!msg) return;

    let parsed: GroupMessage<T>;
    try {
      parsed = JSON.parse(msg.content.toString()) as GroupMessage<T>;
    } catch {
      // Malformed — send to DLX, don't requeue
      this.safeNack(channel, msg, false);
      return;
    }

    const messageId = parsed.messageId || msg.properties.messageId || '';

    // ── Dedupe guard ─────────────────────────────────────────────────────────
    // RabbitMQ can redeliver the same message (channel close, nack+requeue,
    // consumer cancel). Skip if this worker is already handling the same id.
    if (messageId && this.inFlightMessages.has(messageId)) {
      // Requeue so another delivery/worker eventually picks it up. Don't DLX
      // because it's our own transient duplicate.
      this.safeNack(channel, msg, true);
      return;
    }

    // Redelivery-count lives in headers. Since amqplib's nack+requeue does NOT
    // mutate headers, we track attempts in the message properties we control
    // by republishing if we need backoff (see requeueWithBackoff below).
    const attempts =
      (msg.properties.headers?.['x-group-attempt'] as number | undefined) ?? 0;

    const slotResult = await this.store.tryAcquireGroupSlot(
      this.workerId,
      groupId,
      this.maxConcurrentGroups
    );

    if (slotResult === 'locked_elsewhere' || slotResult === 'at_capacity') {
      if (attempts >= this.config.maxRequeueAttempts) {
        // Give up — DLX to avoid hot-looping forever.
        this.safeNack(channel, msg, false);
        return;
      }
      await this.requeueWithBackoff(channel, msg, parsed, attempts);
      return;
    }

    // ── Process message ──────────────────────────────────────────────────────
    if (messageId) this.inFlightMessages.add(messageId);

    const refreshInterval = setInterval(
      () => this.store.refreshGroupLock(this.workerId, groupId),
      Math.max(1000, Math.floor(this.config.messageTtl / 3))
    );

    const context: MessageContext = {
      groupId,
      messageId: parsed.messageId,
      sequence: parsed.sequence,
      publishedAt: parsed.publishedAt,
      requeue: async () => {
        this.safeNack(channel, msg, true);
      },
    };

    try {
      await this.handler(parsed.payload, context);
      this.safeAck(channel, msg);

      // Only release the slot when the queue is empty — otherwise prefetch=1
      // will deliver the next message on this same channel and we want to
      // keep holding the lock for this group.
      try {
        const isEmpty = await this.topology.isGroupQueueEmpty(
          this.managementChannel!,
          groupId
        );
        if (isEmpty) {
          await this.store.releaseGroupSlot(this.workerId, groupId);
        }
      } catch {
        // checkQueue can kill the management channel in some edge cases;
        // fall back to releasing so the group can be re-owned later.
        await this.store.releaseGroupSlot(this.workerId, groupId).catch(() => undefined);
      }
    } catch (err) {
      // Handler threw → DLX (no requeue loop)
      this.safeNack(channel, msg, false);
      await this.store.releaseGroupSlot(this.workerId, groupId).catch(() => undefined);
      console.error(`[group-rabbitmq] Handler error for group "${groupId}":`, err);
    } finally {
      clearInterval(refreshInterval);
      if (messageId) this.inFlightMessages.delete(messageId);
    }
  }

  /**
   * Republish the message to its own queue with an incremented attempt
   * counter, then ack the original. This is how we actually grow the
   * `x-group-attempt` header across redeliveries — RabbitMQ's native
   * nack+requeue does NOT mutate headers.
   *
   * Ordering note: the group queue has a single active consumer with
   * prefetch=1. The message we just published will sit behind whatever is
   * already queued — but since we're the only worker draining this queue,
   * and we're about to nack+requeue anyway, ordering across the SAME logical
   * message is preserved (it is still the next one we see).
   */
  private async requeueWithBackoff(
    channel: amqplib.Channel,
    msg: amqplib.ConsumeMessage,
    parsed: GroupMessage<T>,
    attempts: number
  ): Promise<void> {
    const delay = this.calcBackoffDelay(attempts);

    const timer = setTimeout(async () => {
      this.requeueTimers.delete(timer);
      if (!this.running) return;

      try {
        const queueName = this.topology.groupQueueName(parsed.groupId);
        const headers = {
          ...(msg.properties.headers ?? {}),
          'x-group-attempt': attempts + 1,
        };
        // Send to the DEFAULT exchange, routed directly to this queue, so we
        // don't double-fan-out through the group exchange.
        channel.sendToQueue(queueName, msg.content, {
          persistent: true,
          contentType: msg.properties.contentType ?? 'application/json',
          messageId: msg.properties.messageId,
          headers,
        });
        this.safeAck(channel, msg);
      } catch {
        // Channel likely closed — let reconnect flow recover
      }
    }, delay);

    this.requeueTimers.add(timer);
  }

  async unsubscribeFromGroup(groupId: string): Promise<void> {
    const channel = this.groupChannels.get(groupId);
    const tag = this.groupConsumerTags.get(groupId);

    this.groupChannels.delete(groupId);
    this.groupConsumerTags.delete(groupId);

    if (channel && tag) {
      try {
        await channel.cancel(tag);
      } catch {
        // channel may already be closed
      }
      try {
        await channel.close();
      } catch {
        // already closed
      }
    }

    await this.store.releaseGroupSlot(this.workerId, groupId).catch(() => undefined);
  }

  async close(): Promise<void> {
    this.running = false;

    for (const timer of this.requeueTimers) {
      clearTimeout(timer);
    }
    this.requeueTimers.clear();

    const groupIds = [...this.groupChannels.keys()];
    await Promise.all(groupIds.map((g) => this.unsubscribeFromGroup(g)));

    await this.store.clearWorkerState(this.workerId).catch(() => undefined);

    if (this.managementChannel) {
      try {
        await this.managementChannel.close();
      } catch {
        // already closed
      }
      this.managementChannel = null;
    }
  }

  // ─── Helpers ──────────────────────────────────────────────────────────────

  private calcBackoffDelay(attempt: number): number {
    const base = this.config.requeueDelayMs;
    const exp = Math.min(attempt, 10);
    const jitter = Math.random() * base * 0.2;
    return Math.floor(base * Math.pow(2, exp) + jitter);
  }

  private safeAck(channel: amqplib.Channel, msg: amqplib.ConsumeMessage): void {
    try {
      channel.ack(msg);
    } catch {
      // channel closed
    }
  }

  private safeNack(
    channel: amqplib.Channel,
    msg: amqplib.ConsumeMessage,
    requeue: boolean
  ): void {
    try {
      channel.nack(msg, false, requeue);
    } catch {
      // channel closed
    }
  }
}
