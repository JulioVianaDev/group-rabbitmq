import * as amqplib from 'amqplib';
import { GroupRabbitMQConfig } from '../types';

export class TopologyManager {
  constructor(private readonly config: Required<GroupRabbitMQConfig>) {}

  groupQueueName(groupId: string): string {
    return `${this.config.queuePrefix}.${groupId}`;
  }

  deadLetterQueueName(): string {
    return `${this.config.queuePrefix}.dead`;
  }

  async assertBaseTopology(channel: amqplib.Channel): Promise<void> {
    await channel.assertExchange(this.config.exchangeName, this.config.exchangeType, {
      durable: true,
    });
    await channel.assertExchange(this.config.dlxExchangeName, 'fanout', {
      durable: true,
    });
    await channel.assertQueue(this.deadLetterQueueName(), { durable: true });
    await channel.bindQueue(this.deadLetterQueueName(), this.config.dlxExchangeName, '');
  }

  async assertGroupQueue(channel: amqplib.Channel, groupId: string): Promise<string> {
    const queueName = this.groupQueueName(groupId);
    await channel.assertQueue(queueName, {
      durable: true,
      messageTtl: this.config.messageTtl,
      deadLetterExchange: this.config.dlxExchangeName,
      // Guarantees only one active consumer per group queue at a time.
      // With multiple workers subscribed, this preserves per-group ordering.
      arguments: {
        'x-single-active-consumer': true,
      },
    });
    await channel.bindQueue(queueName, this.config.exchangeName, groupId);
    return queueName;
  }

  async isGroupQueueEmpty(channel: amqplib.Channel, groupId: string): Promise<boolean> {
    const queueName = this.groupQueueName(groupId);
    const status = await channel.checkQueue(queueName);
    return status.messageCount === 0;
  }
}
