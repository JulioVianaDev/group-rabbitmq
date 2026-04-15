import {
  ConsumeOptions,
  GroupRabbitMQConfig,
  GroupMessage,
  MessageHandler,
  PublishOptions,
} from './types';
import { ConnectionManager } from './connection/ConnectionManager';
import { GroupConsumer } from './consumer/GroupConsumer';
import { GroupDiscovery } from './discovery/GroupDiscovery';
import { GroupMonitor } from './monitor/GroupMonitor';
import { GroupPublisher } from './publisher/GroupPublisher';
import { GroupStateStore } from './store/GroupStateStore';
import { TopologyManager } from './topology/TopologyManager';
import { WorkerClusterBalancer } from './balancer/WorkerClusterBalancer';

const DEFAULT_CONFIG: Omit<Required<GroupRabbitMQConfig>, 'amqpUrl' | 'redisUrl'> = {
  exchangeName: 'group.exchange',
  exchangeType: 'direct',
  queuePrefix: 'group',
  dlxExchangeName: 'group.dlx',
  messageTtl: 30 * 60 * 1000,
  maxRequeueAttempts: 3,
  requeueDelayMs: 200,
};

export interface GroupRabbitMQFullConfig extends GroupRabbitMQConfig {
  managementUrl?: string;
  managementUsername?: string;
  managementPassword?: string;
  maxRetries?: number;
  retryDelayMs?: number;
  maxRetryDelayMs?: number;
}

export class GroupRabbitMQ<T = unknown> {
  private readonly config: Required<GroupRabbitMQConfig>;
  private readonly fullConfig: GroupRabbitMQFullConfig;

  private connectionManager: ConnectionManager;
  private topology: TopologyManager;
  private store: GroupStateStore;

  private publisher: GroupPublisher<T> | null = null;
  private consumer: GroupConsumer<T> | null = null;

  private _discovery: GroupDiscovery | null = null;
  private _monitor: GroupMonitor | null = null;
  private _balancer: WorkerClusterBalancer | null = null;

  constructor(config: GroupRabbitMQFullConfig) {
    this.fullConfig = config;
    this.config = { ...DEFAULT_CONFIG, ...config };

    this.connectionManager = new ConnectionManager({
      amqpUrl: config.amqpUrl,
      maxRetries: config.maxRetries,
      retryDelayMs: config.retryDelayMs,
      maxRetryDelayMs: config.maxRetryDelayMs,
    });

    this.topology = new TopologyManager(this.config);
    this.store = new GroupStateStore(this.config.redisUrl, this.config.queuePrefix);
  }

  async connect(): Promise<void> {
    await this.store.connect();
    await this.connectionManager.connect();

    this.connectionManager.on('connected', async () => {
      if (this.publisher) {
        await this.publisher.close().catch(() => undefined);
        this.publisher = null;
      }
      if (this._balancer) {
        await this._balancer.stop().catch(() => undefined);
        this._balancer = null;
      }
      if (this.consumer) {
        console.warn('[group-rabbitmq] Reconnected — consumer channels need re-init');
        await this.consumer.close().catch(() => undefined);
        this.consumer = null;
      }
    });

    if (this.fullConfig.managementUrl) {
      this._discovery = new GroupDiscovery({
        managementUrl: this.fullConfig.managementUrl,
        username: this.fullConfig.managementUsername,
        password: this.fullConfig.managementPassword,
        queuePrefix: this.config.queuePrefix,
      });
    }
  }

  async disconnect(): Promise<void> {
    if (this._balancer) {
      await this._balancer.stop().catch(() => undefined);
      this._balancer = null;
    }
    this._discovery?.stopWatching();
    this._monitor?.stopLogging();
    if (this.consumer) { await this.consumer.close(); this.consumer = null; }
    if (this.publisher) { await this.publisher.close(); this.publisher = null; }
    await this.store.disconnect();
    await this.connectionManager.disconnect();
  }

  async publish(groupId: string, payload: T, options?: PublishOptions): Promise<GroupMessage<T>> {
    return (await this.getPublisher()).publish(groupId, payload, options);
  }

  async publishBatch(messages: Array<{ groupId: string; payload: T }>, options?: PublishOptions): Promise<GroupMessage<T>[]> {
    return (await this.getPublisher()).publishBatch(messages, options);
  }

  async consume(handler: MessageHandler<T>, options: ConsumeOptions = {}): Promise<void> {
    this.assertConnected();
    const conn = this.connectionManager.currentConnection!;
    this.consumer = new GroupConsumer<T>(this.config, this.topology, this.store, conn, handler, options);
    await this.consumer.initialize();
    this._monitor = new GroupMonitor(this.store, this.consumer.id, options.maxConcurrentGroups ?? 100, this._discovery ?? undefined);

    if (options.dynamicWorkerBalancing) {
      if (!this._discovery) {
        throw new Error('[group-rabbitmq] dynamicWorkerBalancing requires managementUrl in config.');
      }
      this._balancer = new WorkerClusterBalancer({
        redisUrl: this.config.redisUrl,
        queuePrefix: this.config.queuePrefix,
        workerId: this.consumer.id,
        rebalanceIntervalMs: options.rebalanceIntervalMs,
        workerHeartbeatTtlSec: options.workerHeartbeatTtlSec,
        discoverGroups: () => this._discovery!.discoverGroups(),
        subscribeToGroup: (groupId) => this.subscribeToGroup(groupId),
        unsubscribeFromGroup: (groupId) => this.unsubscribeFromGroup(groupId),
      });
      await this._balancer.start();
    }
  }

  async subscribeToGroups(groupIds: string[]): Promise<void> {
    if (!this.consumer) throw new Error('Call consume() before subscribeToGroups().');
    await Promise.all(groupIds.map((g) => this.consumer!.subscribeToGroup(g)));
  }

  async subscribeToGroup(groupId: string): Promise<void> {
    if (!this.consumer) throw new Error('Call consume() before subscribeToGroup().');
    await this.consumer.subscribeToGroup(groupId);
  }

  async watchGroups(pollIntervalMs = 5_000): Promise<void> {
    if (!this._discovery) throw new Error('watchGroups() requires managementUrl in config.');
    if (!this.consumer) throw new Error('Call consume() before watchGroups().');

    const existing = await this._discovery.discoverGroups();
    if (existing.length > 0) {
      console.log(`[group-rabbitmq] Discovered ${existing.length} existing groups:`, existing);
      await this.subscribeToGroups(existing);
    }

    this._discovery.watch(async (newGroupId) => {
      console.log(`[group-rabbitmq] New group discovered: ${newGroupId}`);
      await this.subscribeToGroup(newGroupId);
    }, pollIntervalMs);
  }

  async unsubscribeFromGroup(groupId: string): Promise<void> {
    if (this.consumer) await this.consumer.unsubscribeFromGroup(groupId);
  }

  get workerId(): string | undefined { return this.consumer?.id; }
  get discovery(): GroupDiscovery | null { return this._discovery; }
  get monitor(): GroupMonitor | null { return this._monitor; }

  private async getPublisher(): Promise<GroupPublisher<T>> {
    this.assertConnected();
    if (!this.publisher) {
      const conn = this.connectionManager.currentConnection!;
      this.publisher = new GroupPublisher<T>(this.config, this.topology, this.store, conn);
      await this.publisher.initialize();
    }
    return this.publisher;
  }

  private assertConnected(): void {
    if (!this.connectionManager.currentConnection) {
      throw new Error('[group-rabbitmq] Not connected. Call connect() first.');
    }
  }
}
