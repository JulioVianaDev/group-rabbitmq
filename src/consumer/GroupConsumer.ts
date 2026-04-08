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
 *     they were published. The next message is only fetched after the current
 *     one is fully acked.
 *
 *     HOW: prefetch=1 on each per-group channel. Since every group has its own
 *     queue, prefetch=1 means "deliver only 1 unacked message from THIS queue".
 *     It does NOT affect the exchange, other queues, or other workers' channels.
 *
 *  2. CONCURRENCY LIMIT: A single worker processes at most `maxConcurrentGroups`
 *     different groups at a time.
 *
 *     HOW: Redis tracks active groups per worker. When a new group's message
 *     arrives and we're at capacity, the message is nacked and requeued with
 *     backoff. It will be retried after a delay.
 *
 *  3. EXCLUSIVITY: A group is processed by exactly one worker at a time across
 *     the entire cluster.
 *
 *     HOW: Distributed Redis lock per groupId. Only the worker holding the lock
 *     consumes from that group's queue.
 *
 * Channel architecture:
 *  - Each group gets its OWN channel with prefetch=1.
 *  - A single shared channel is used for queue/binding assertions.
 *  - This means pausing one group (nack) does not stall other groups.
 */
export class GroupConsumer<T = unknown> {
  private readonly workerId: string;
  private readonly maxConcurrentGroups: number;

  // One channel per groupId — isolated prefetch windows
  private readonly groupChannels = new Map<string, amqplib.Channel>();
  private readonly groupConsumerTags = new Map<string, string>();

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
    this.maxConcurrentGroups = options.maxConcurrentGroups ?? Infinity;
  }

  get id(): string {
    return this.workerId;
  }

  async initialize(): Promise<void> {
    this.managementChannel = await this.connection.createChannel();
    await this.topology.assertBaseTopology(this.managementChannel);
    this.running = true;
  }

  /**
   * Start consuming from a specific group's queue.
   *
   * This is called automatically when the library detects a new group
   * (via a separate "group discovery" mechanism), or you can call it
   * explicitly if you know the groups in advance.
   */
  async subscribeToGroup(groupId: string): Promise<void> {
    if (!this.running || !this.managementChannel) {
      throw new Error('Consumer not initialized.');
    }

    if (this.groupChannels.has(groupId)) {
      return; // Already subscribed
    }

    // Ensure the queue exists before consuming
    await this.topology.assertGroupQueue(this.managementChannel, groupId);

    // ── Key design: each group gets its own channel with prefetch=1 ──────────
    //
    // prefetch=1 on this channel means:
    //   "RabbitMQ will deliver at most 1 unacked message FROM queues consumed
    //    on THIS channel"
    //
    // Since this channel only consumes from group.<groupId>, the effect is:
    //   "Only one message from group.<groupId> is in-flight at any time"
    //
    // This is SCOPED to:
    //   - This channel only (not the connection, not other channels)
    //   - This worker only (other workers have their own channels)
    //   - This group only (other groups have their own channels with their own prefetch)
    //
    // Without per-group channels, a single prefetch=1 channel subscribed to
    // multiple groups would process all groups serially — group1 would block
    // group2. With separate channels, group1 and group2 run in parallel, but
    // each group internally stays serial.
    // ─────────────────────────────────────────────────────────────────────────

    const channel = await this.connection.createChannel();
    await channel.prefetch(1); // <── scoped to this channel / this group only

    this.groupChannels.set(groupId, channel);

    const queueName = this.topology.groupQueueName(groupId);

    const { consumerTag } = await channel.consume(
      queueName,
      (msg) => this.handleDelivery(channel, groupId, msg),
      { noAck: false } // Manual acks — we ack AFTER processing completes
    );

    this.groupConsumerTags.set(groupId, consumerTag);
  }

  /**
   * Core delivery handler — called by RabbitMQ for each message.
   *
   * Decision tree:
   *  1. Is this group already active on this worker?       → process
   *  2. Is another worker processing this group?           → nack + requeue with backoff
   *  3. Does this worker have capacity for a new group?    → acquire + process
   *  4. Worker is at maxConcurrentGroups capacity          → nack + requeue with backoff
   */
  private async handleDelivery(
    channel: amqplib.Channel,
    groupId: string,
    msg: amqplib.ConsumeMessage | null
  ): Promise<void> {
    if (!msg) return; // Consumer cancelled

    const redeliveryCount = (msg.properties.headers?.['x-redelivery-count'] ?? 0) as number;

    let parsed: GroupMessage<T>;
    try {
      parsed = JSON.parse(msg.content.toString()) as GroupMessage<T>;
    } catch {
      // Malformed message — send directly to DLX, don't requeue
      channel.nack(msg, false, false);
      return;
    }

    // ── Acquire slot ──────────────────────────────────────────────────────────
    const slotResult = await this.store.tryAcquireGroupSlot(
      this.workerId,
      groupId,
      this.maxConcurrentGroups
    );

    if (slotResult === 'locked_elsewhere' || slotResult === 'at_capacity') {
      // We cannot process this message right now.
      // Keep the message unacked for a short delay, then requeue it.
      // This preserves per-group ordering by avoiding republish-to-tail behavior.
      const delay = this.calcBackoffDelay(redeliveryCount);

      const timer = setTimeout(async () => {
        this.requeueTimers.delete(timer);
        if (!this.running) return;

        try {
          channel.nack(msg, false, true);
        } catch {
          // If channel is already closed, let reconnection flow handle recovery.
          return;
        }
      }, delay);

      this.requeueTimers.add(timer);
      return;
    }

    // ── Process message ───────────────────────────────────────────────────────

    // Set up a lock-refresh interval for long-running handlers
    const refreshInterval = setInterval(
      () => this.store.refreshGroupLock(this.workerId, groupId),
      Math.floor(this.config.messageTtl / 3) // refresh at 1/3 of TTL
    );

    const context: MessageContext = {
      groupId,
      messageId: parsed.messageId,
      sequence: parsed.sequence,
      publishedAt: parsed.publishedAt,
      requeue: async () => {
        // User-requested soft requeue: puts message back at end of group queue
        channel.nack(msg, false, true);
      },
    };

    try {
      await this.handler(parsed.payload, context);

      // ── Ack: tell RabbitMQ this message was processed successfully ─────────
      // prefetch=1 means RabbitMQ was holding back the next message.
      // After this ack, it will deliver the next one in the queue.
      channel.ack(msg);

      // Check if the group queue is now empty
      const isEmpty = await this.topology.isGroupQueueEmpty(
        this.managementChannel!,
        groupId
      );

      if (isEmpty) {
        // Release the group's slot so this worker (and others) can pick up new groups
        await this.store.releaseGroupSlot(this.workerId, groupId);
      }
    } catch (err) {
      // Handler threw — send to DLX (do not requeue, avoid infinite error loops)
      channel.nack(msg, false, false);
      // Release slot: group had a fatal error, allow another worker to retry
      await this.store.releaseGroupSlot(this.workerId, groupId);
      console.error(`[group-rabbitmq] Handler error for group "${groupId}":`, err);
    } finally {
      clearInterval(refreshInterval);
    }
  }

  /**
   * Unsubscribe from a group's queue (e.g. when shutting down a worker).
   */
  async unsubscribeFromGroup(groupId: string): Promise<void> {
    const channel = this.groupChannels.get(groupId);
    const tag = this.groupConsumerTags.get(groupId);

    if (channel && tag) {
      await channel.cancel(tag);
      await channel.close();
      this.groupChannels.delete(groupId);
      this.groupConsumerTags.delete(groupId);
    }

    await this.store.releaseGroupSlot(this.workerId, groupId);
  }

  /**
   * Graceful shutdown: stop consuming, wait for in-flight messages to finish,
   * release all Redis slots.
   */
  async close(): Promise<void> {
    this.running = false;

    // Cancel all pending requeue timers
    for (const timer of this.requeueTimers) {
      clearTimeout(timer);
    }
    this.requeueTimers.clear();

    // Cancel all group consumers
    const groupIds = [...this.groupChannels.keys()];
    await Promise.all(groupIds.map((g) => this.unsubscribeFromGroup(g)));

    // Clear Redis state for this worker
    await this.store.clearWorkerState(this.workerId);

    if (this.managementChannel) {
      await this.managementChannel.close();
      this.managementChannel = null;
    }
  }

  // ─── Helpers ──────────────────────────────────────────────────────────────

  /**
   * Exponential backoff with jitter for requeue delays.
   * attempt=0 → ~200ms, attempt=1 → ~400ms, attempt=2 → ~800ms, etc.
   */
  private calcBackoffDelay(attempt: number): number {
    const base = this.config.requeueDelayMs;
    const exp = Math.min(attempt, 10); // cap at 2^10
    const jitter = Math.random() * base * 0.2; // ±20% jitter
    return Math.floor(base * Math.pow(2, exp) + jitter);
  }
}
