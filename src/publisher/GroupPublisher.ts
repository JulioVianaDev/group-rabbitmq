import * as amqplib from 'amqplib';
import { v4 as uuidv4 } from 'uuid';
import { GroupRabbitMQConfig, GroupMessage, PublishOptions } from '../types';
import { TopologyManager } from '../topology/TopologyManager';
import { GroupStateStore } from '../store/GroupStateStore';

export class GroupPublisher<T = unknown> {
  private channel: amqplib.Channel | null = null;

  constructor(
    private readonly config: Required<GroupRabbitMQConfig>,
    private readonly topology: TopologyManager,
    private readonly store: GroupStateStore,
    private readonly connection: amqplib.ChannelModel
  ) {}

  async initialize(): Promise<void> {
    this.channel = await this.connection.createChannel();
    await this.topology.assertBaseTopology(this.channel);
  }

  async close(): Promise<void> {
    if (this.channel) {
      await this.channel.close();
      this.channel = null;
    }
  }

  async publish(groupId: string, payload: T, options: PublishOptions = {}): Promise<GroupMessage<T>> {
    if (!this.channel) throw new Error('Publisher not initialized.');
    await this.topology.assertGroupQueue(this.channel, groupId);
    const sequence = await this.store.nextSequence(groupId);
    const message: GroupMessage<T> = {
      groupId,
      messageId: options.messageId ?? uuidv4(),
      sequence,
      publishedAt: new Date().toISOString(),
      payload,
    };

    const ok = this.channel.publish(
      this.config.exchangeName,
      groupId,
      Buffer.from(JSON.stringify(message)),
      {
        persistent: options.persistent ?? true,
        contentType: 'application/json',
        headers: options.headers,
        messageId: message.messageId,
      }
    );

    if (!ok) {
      await new Promise<void>((resolve) => this.channel!.once('drain', () => resolve()));
    }

    return message;
  }

  async publishBatch(
    messages: Array<{ groupId: string; payload: T }>,
    options: PublishOptions = {}
  ): Promise<GroupMessage<T>[]> {
    const out: GroupMessage<T>[] = [];
    for (const message of messages) {
      out.push(await this.publish(message.groupId, message.payload, options));
    }
    return out;
  }
}
