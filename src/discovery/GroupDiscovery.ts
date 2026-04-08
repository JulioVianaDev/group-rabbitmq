
/**
 * GroupDiscovery queries the RabbitMQ Management HTTP API to find queues that
 * belong to this library (prefix: group.<something>).
 *
 * This solves the "I don't know which groups exist at startup" problem.
 * Instead of hardcoding group names in subscribeToGroups(), you can call
 * discoverGroups() and get back all active group IDs automatically.
 *
 * Requires the RabbitMQ Management plugin (enabled by default in most setups).
 *
 * Usage:
 *   const discovery = new GroupDiscovery({ managementUrl: 'http://localhost:15672', ... });
 *   const groupIds  = await discovery.discoverGroups(); // ['item1', 'item2', ...]
 *   await mq.subscribeToGroups(groupIds);
 *
 *   // Or: watch for new groups and subscribe on-the-fly
 *   discovery.watch(async (newGroupId) => {
 *     await mq.subscribeToGroup(newGroupId);
 *   });
 */
export class GroupDiscovery {
  private readonly managementUrl: string;
  private readonly username: string;
  private readonly password: string;
  private readonly queuePrefix: string;
  private readonly vhost: string;

  private watchInterval: NodeJS.Timeout | null = null;
  private knownGroups = new Set<string>();

  constructor(options: {
    /** RabbitMQ management URL, e.g. "http://localhost:15672" */
    managementUrl: string;
    username?: string;
    password?: string;
    /** Must match GroupRabbitMQConfig.queuePrefix. Default: "group" */
    queuePrefix?: string;
    /** RabbitMQ vhost. Default: "/" */
    vhost?: string;
  }) {
    this.managementUrl = options.managementUrl.replace(/\/$/, '');
    this.username = options.username ?? 'guest';
    this.password = options.password ?? 'guest';
    this.queuePrefix = options.queuePrefix ?? 'group';
    this.vhost = options.vhost ?? '/';
  }

  /**
   * Query the Management API once and return all known group IDs.
   * Excludes the dead-letter queue (group.dead).
   */
  async discoverGroups(): Promise<string[]> {
    const queues = await this.fetchQueues();
    const prefix = `${this.queuePrefix}.`;

    return queues
      .map((q: any) => (q as { name: string }).name)
      .filter((name: any) => name.startsWith(prefix) && !name.endsWith('.dead'))
      .map((name: any) => (name as string).slice(prefix.length));
  }

  /**
   * Poll the Management API every `intervalMs` milliseconds.
   * Calls `onNewGroup` whenever a group queue appears that wasn't known before.
   *
   * @param onNewGroup  Callback receiving the new groupId
   * @param intervalMs  Polling interval. Default: 5000ms
   */
  watch(onNewGroup: (groupId: string) => Promise<void>, intervalMs = 5_000): void {
    if (this.watchInterval) return; // already watching

    const poll = async () => {
      try {
        const groups = await this.discoverGroups();
        for (const groupId of groups) {
          if (!this.knownGroups.has(groupId)) {
            this.knownGroups.add(groupId);
            await onNewGroup(groupId).catch((err) =>
              console.error(`[group-rabbitmq:discovery] Error subscribing to "${groupId}":`, err)
            );
          }
        }
      } catch (err) {
        console.warn('[group-rabbitmq:discovery] Poll failed:', (err as Error).message);
      }
    };

    // Run immediately then on interval
    poll();
    this.watchInterval = setInterval(poll, intervalMs);
  }

  /**
   * Stop polling.
   */
  stopWatching(): void {
    if (this.watchInterval) {
      clearInterval(this.watchInterval);
      this.watchInterval = null;
    }
  }

  /**
   * Get queue depth for a specific group (useful for monitoring).
   */
  async getGroupQueueDepth(groupId: string): Promise<number> {
    const vhostEncoded = encodeURIComponent(this.vhost);
    const queueName = `${this.queuePrefix}.${groupId}`;
    const url = `${this.managementUrl}/api/queues/${vhostEncoded}/${encodeURIComponent(queueName)}`;

    const res = await this.apiFetch(url);
    if (!res.ok) return 0;
    const data = await res.json() as Record<string, unknown>;
    return (data.messages as number) ?? 0;
  }

  /**
   * Get a summary of all group queue depths (for dashboards / health checks).
   */
  async getQueueSummary(): Promise<Record<string, number>> {
    const queues = await this.fetchQueues();
    const prefix = `${this.queuePrefix}.`;
    const summary: Record<string, number> = {};

    for (const _q of queues) {
      const q = _q as { name: string; messages: number };
      if (q.name.startsWith(prefix) && !q.name.endsWith('.dead')) {
        const groupId = q.name.slice(prefix.length);
        summary[groupId] = q.messages ?? 0;
      }
    }

    return summary;
  }

  // ─── Private ──────────────────────────────────────────────────────────────

  private async fetchQueues(): Promise<unknown[]> {
    const vhostEncoded = encodeURIComponent(this.vhost);
    const url = `${this.managementUrl}/api/queues/${vhostEncoded}`;
    const res = await this.apiFetch(url);
    if (!res.ok) {
      throw new Error(`Management API error: ${res.status} ${res.statusText}`);
    }
    return res.json() as Promise<unknown[]>;
  }

  private apiFetch(url: string): Promise<Response> {
    const credentials = Buffer.from(`${this.username}:${this.password}`).toString('base64');
    return fetch(url, {
      headers: { Authorization: `Basic ${credentials}` },
    });
  }
}
