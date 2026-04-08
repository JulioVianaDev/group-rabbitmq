export type ExchangeType = 'direct' | 'topic' | 'fanout' | 'headers';

export interface GroupRabbitMQConfig {
  amqpUrl: string;
  redisUrl: string;
  exchangeName?: string;
  exchangeType?: ExchangeType;
  queuePrefix?: string;
  dlxExchangeName?: string;
  messageTtl?: number;
  maxRequeueAttempts?: number;
  requeueDelayMs?: number;
}

export interface PublishOptions {
  messageId?: string;
  headers?: Record<string, unknown>;
  persistent?: boolean;
}

export interface ConsumeOptions {
  maxConcurrentGroups?: number;
  workerId?: string;
  /**
   * When true, registers this process in Redis and periodically subscribes/unsubscribes
   * to group queues so each group is owned by exactly one worker (consistent hashing).
   * Requires `managementUrl` on the main config. Use for horizontal scaling with dynamic groups.
   */
  dynamicWorkerBalancing?: boolean;
  /** How often to re-run discovery + subscription mapping. Default: 5000 */
  rebalanceIntervalMs?: number;
  /** Redis heartbeat TTL for liveness (seconds). Default: 10 */
  workerHeartbeatTtlSec?: number;
}

export interface GroupMessage<T = unknown> {
  groupId: string;
  messageId: string;
  sequence: number;
  publishedAt: string;
  payload: T;
}

export interface MessageContext {
  groupId: string;
  messageId: string;
  sequence: number;
  publishedAt: string;
  requeue: () => Promise<void>;
}

export type MessageHandler<T> = (payload: T, context: MessageContext) => Promise<void> | void;
