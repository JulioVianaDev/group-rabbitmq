import Redis from 'ioredis';

export interface WorkerClusterBalancerOptions {
  redisUrl: string;
  /** Must match GroupRabbitMQ queue prefix (used for Redis key namespace). */
  queuePrefix: string;
  /** Same id as GroupConsumer worker (ConsumeOptions.workerId). */
  workerId: string;
  rebalanceIntervalMs?: number;
  workerHeartbeatTtlSec?: number;
  discoverGroups: () => Promise<string[]>;
  subscribeToGroup: (groupId: string) => Promise<void>;
  unsubscribeFromGroup: (groupId: string) => Promise<void>;
  /** Optional log hook (e.g. for debugging). */
  onLog?: (message: string) => void;
}

function hashToInt(input: string): number {
  let hash = 0;
  for (let i = 0; i < input.length; i++) {
    hash = (hash * 31 + input.charCodeAt(i)) >>> 0;
  }
  return hash;
}

function shouldThisWorkerOwnGroup(groupId: string, workerId: string, sortedWorkers: string[]): boolean {
  if (sortedWorkers.length <= 1) return true;
  const slot = hashToInt(groupId) % sortedWorkers.length;
  return sortedWorkers[slot] === workerId;
}

/**
 * Registers this process in Redis and periodically re-subscribes to group queues so that
 * each group is handled by exactly one worker (consistent hashing over active workers).
 * Works with RabbitMQ single-active-consumer queues to preserve per-group ordering.
 */
export class WorkerClusterBalancer {
  private readonly redis: Redis;
  private readonly workersSetKey: string;
  private readonly heartbeatKey: string;
  private readonly heartbeatPrefix: string;
  private readonly subscribedGroups = new Set<string>();
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private rebalanceTimer: NodeJS.Timeout | null = null;
  private readonly opts: Required<
    Pick<WorkerClusterBalancerOptions, 'rebalanceIntervalMs' | 'workerHeartbeatTtlSec'>
  > &
    WorkerClusterBalancerOptions;

  constructor(opts: WorkerClusterBalancerOptions) {
    this.opts = {
      ...opts,
      rebalanceIntervalMs: opts.rebalanceIntervalMs ?? 5000,
      workerHeartbeatTtlSec: opts.workerHeartbeatTtlSec ?? 10,
    };
    this.workersSetKey = `${this.opts.queuePrefix}:workers:active`;
    this.heartbeatPrefix = `${this.opts.queuePrefix}:workers:heartbeat:`;
    this.heartbeatKey = `${this.heartbeatPrefix}${this.opts.workerId}`;
    this.redis = new Redis(this.opts.redisUrl, {
      maxRetriesPerRequest: null,
      enableReadyCheck: true,
    });
    this.redis.on('error', (err) => {
      this.log(`redis warning: ${err.message}`);
    });
  }

  private log(msg: string): void {
    this.opts.onLog?.(`[group-rabbitmq:balancer] ${msg}`);
  }

  async start(): Promise<void> {
    const ttlMs = this.opts.workerHeartbeatTtlSec * 1000;
    const heartbeatEvery = Math.max(1000, Math.floor(ttlMs / 2));

    const register = async (): Promise<void> => {
      await this.redis.sadd(this.workersSetKey, this.opts.workerId);
      await this.redis.set(this.heartbeatKey, '1', 'EX', this.opts.workerHeartbeatTtlSec);
    };

    const getActiveWorkers = async (): Promise<string[]> => {
      const allWorkers = await this.redis.smembers(this.workersSetKey);
      const workerStates = await Promise.all(
        allWorkers.map(async (id) => ({
          id,
          alive: (await this.redis.exists(`${this.heartbeatPrefix}${id}`)) === 1,
        }))
      );
      const active = workerStates.filter((w) => w.alive).map((w) => w.id).sort();
      const inactive = workerStates.filter((w) => !w.alive).map((w) => w.id);
      if (inactive.length > 0) {
        await this.redis.srem(this.workersSetKey, ...inactive);
      }
      return active;
    };

    const rebalance = async (): Promise<void> => {
      let groups: string[];
      try {
        groups = await this.opts.discoverGroups();
      } catch (err) {
        this.log(`discoverGroups failed: ${(err as Error).message}`);
        return;
      }
      const activeWorkers = await getActiveWorkers();

      for (const groupId of groups) {
        const iOwn = shouldThisWorkerOwnGroup(groupId, this.opts.workerId, activeWorkers);
        const isSub = this.subscribedGroups.has(groupId);

        if (iOwn && !isSub) {
          await this.opts.subscribeToGroup(groupId);
          this.subscribedGroups.add(groupId);
          this.log(`subscribed group=${groupId} workers=${activeWorkers.join(',')}`);
        } else if (!iOwn && isSub) {
          await this.opts.unsubscribeFromGroup(groupId);
          this.subscribedGroups.delete(groupId);
          this.log(`unsubscribed group=${groupId} workers=${activeWorkers.join(',')}`);
        }
      }
    };

    await register();
    try {
      await rebalance();
    } catch (err) {
      this.log(`initial rebalance warning: ${(err as Error).message}`);
    }

    this.heartbeatTimer = setInterval(() => {
      register().catch((err) => this.log(`heartbeat: ${(err as Error).message}`));
    }, heartbeatEvery);

    this.rebalanceTimer = setInterval(() => {
      rebalance().catch((err) => this.log(`rebalance: ${(err as Error).message}`));
    }, this.opts.rebalanceIntervalMs);
  }

  async stop(): Promise<void> {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    if (this.rebalanceTimer) {
      clearInterval(this.rebalanceTimer);
      this.rebalanceTimer = null;
    }
    await this.redis.del(this.heartbeatKey).catch(() => undefined);
    await this.redis.srem(this.workersSetKey, this.opts.workerId).catch(() => undefined);
    await this.redis.quit().catch(() => undefined);
  }
}
