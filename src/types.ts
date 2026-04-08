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
