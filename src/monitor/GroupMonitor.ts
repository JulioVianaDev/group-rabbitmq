import { GroupDiscovery } from '../discovery/GroupDiscovery';
import { GroupStateStore } from '../store/GroupStateStore';

export interface WorkerHealth {
  workerId: string;
  activeGroups: string[];
  activeGroupCount: number;
  maxConcurrentGroups: number;
  utilizationPercent: number;
}

export interface SystemHealth {
  workers: WorkerHealth[];
  queueDepths: Record<string, number | undefined>;
  deadLetterCount: number;
  timestamp: string;
}

/**
 * GroupMonitor provides observability into the group-rabbitmq system.
 *
 * Usage:
 *   const monitor = new GroupMonitor({ store, discovery, workerId, maxConcurrentGroups });
 *
 *   // One-shot health check
 *   const health = await monitor.getWorkerHealth();
 *   console.log(health);
 *
 *   // Periodic metrics logging
 *   monitor.startLogging(10_000); // every 10s
 *   monitor.stopLogging();
 */
export class GroupMonitor {
  private logInterval: NodeJS.Timeout | null = null;

  constructor(
    private readonly store: GroupStateStore,
    private readonly workerId: string,
    private readonly maxConcurrentGroups: number,
    private readonly discovery?: GroupDiscovery
  ) { }

  /**
   * Get health snapshot for this worker.
   */
  async getWorkerHealth(): Promise<WorkerHealth> {
    const activeGroups = await this.store.getActiveGroups(this.workerId);
    const activeGroupCount = activeGroups.length;
    const cap = this.maxConcurrentGroups === Infinity ? 100 : this.maxConcurrentGroups;
    const utilizationPercent =
      this.maxConcurrentGroups === Infinity ? 0 : Math.round((activeGroupCount / cap) * 100);

    return {
      workerId: this.workerId,
      activeGroups,
      activeGroupCount,
      maxConcurrentGroups: this.maxConcurrentGroups,
      utilizationPercent,
    };
  }

  /**
   * Get full system health (requires GroupDiscovery to be configured).
   */
  async getSystemHealth(): Promise<SystemHealth> {
    const [workerHealth, rawDepths] = await Promise.all([
      this.getWorkerHealth(),
      this.discovery?.getQueueSummary() ?? ({} as Record<string, number>),
    ]);

    const depths = rawDepths as Record<string, number>;
    const deadLetterCount = depths['dead'] ?? 0;
    const queueDepths: Record<string, number> = Object.fromEntries(
      Object.entries(depths).filter(([k]) => k !== 'dead')
    );

    return {
      workers: [workerHealth],
      queueDepths,
      deadLetterCount,
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Start logging health metrics to console at a given interval.
   */
  startLogging(intervalMs = 10_000): void {
    if (this.logInterval) return;
    this.logInterval = setInterval(async () => {
      try {
        const health = await this.getWorkerHealth();
        console.log(
          `[group-rabbitmq:monitor] worker=${health.workerId.slice(0, 8)}... ` +
          `active=${health.activeGroupCount}/${health.maxConcurrentGroups === Infinity ? '∞' : health.maxConcurrentGroups} ` +
          `groups=[${health.activeGroups.join(', ')}] ` +
          `utilization=${health.utilizationPercent}%`
        );
      } catch (err) {
        console.error('[group-rabbitmq:monitor] Error:', err);
      }
    }, intervalMs);
  }

  stopLogging(): void {
    if (this.logInterval) {
      clearInterval(this.logInterval);
      this.logInterval = null;
    }
  }
}
