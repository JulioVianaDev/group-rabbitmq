import Redis from 'ioredis';

/**
 * GroupStateStore manages per-worker active group state in Redis.
 *
 * Data model:
 *
 *   worker:{workerId}:active          → Redis SET of currently active groupIds
 *   group:{groupId}:lock              → Redis string (workerId), distributed lock
 *   group:{groupId}:sequence          → Redis string (number), global sequence counter
 *
 * Key invariants:
 *  1. A groupId can only be processed by ONE worker at a time (distributed lock).
 *  2. A worker can process at most `maxConcurrentGroups` different groups.
 *  3. A new message for a group already active in this worker is always accepted.
 */
export class GroupStateStore {
  private readonly redis: Redis;
  private readonly namespace: string;

  // Lock TTL: if a worker dies without releasing, the lock expires automatically.
  // Should be longer than your longest expected message processing time.
  private readonly LOCK_TTL_MS = 60_000;

  constructor(redisUrl: string, namespace?: string) {
    this.redis = new Redis(redisUrl, {
      maxRetriesPerRequest: 3,
      enableReadyCheck: true,
      lazyConnect: true,
    });
    this.namespace = namespace ? `${namespace}:` : '';
  }

  async connect(): Promise<void> {
    await this.redis.connect();
  }

  async disconnect(): Promise<void> {
    await this.redis.quit();
  }

  // ─── Group slot management ─────────────────────────────────────────────────

  /**
   * Try to acquire a processing slot for `groupId` on this worker.
   *
   * Returns one of:
   *  - "acquired"     → slot granted, worker should process the message
   *  - "already_mine" → this worker already owns this group, proceed
   *  - "locked_elsewhere" → another worker owns this group, requeue
   *  - "at_capacity"  → this worker is at maxConcurrentGroups, requeue
   */
  async tryAcquireGroupSlot(
    workerId: string,
    groupId: string,
    maxConcurrentGroups: number
  ): Promise<'acquired' | 'already_mine' | 'locked_elsewhere' | 'at_capacity'> {
    const lockKey = this.lockKey(groupId);
    const activeKey = this.activeKey(workerId);

    // Check if this worker already owns this group
    const isAlreadyMine = await this.redis.sismember(activeKey, groupId);
    if (isAlreadyMine) {
      // Refresh the lock TTL so it doesn't expire mid-processing
      await this.redis.pexpire(lockKey, this.LOCK_TTL_MS);
      return 'already_mine';
    }

    // Check if another worker owns this group's distributed lock
    const existingOwner = await this.redis.get(lockKey);
    if (existingOwner && existingOwner !== workerId) {
      return 'locked_elsewhere';
    }

    // Check if this worker has capacity for a new group
    const activeCount = await this.redis.scard(activeKey);
    if (activeCount >= maxConcurrentGroups) {
      return 'at_capacity';
    }

    // Atomically set distributed lock + add to worker's active set
    // Using a pipeline ensures both operations succeed or both fail
    const pipeline = this.redis.pipeline();
    pipeline.set(lockKey, workerId, 'PX', this.LOCK_TTL_MS, 'NX');
    pipeline.sadd(activeKey, groupId);
    const results = await pipeline.exec();

    // results[0] = SET NX result: "OK" if acquired, null if already locked
    const lockResult = results?.[0]?.[1];
    if (lockResult === null) {
      // Another worker sneaked in between our check and set (race condition)
      // Rollback the SADD
      await this.redis.srem(activeKey, groupId);
      return 'locked_elsewhere';
    }

    return 'acquired';
  }

  /**
   * Release a group's slot when its queue is empty or processing is done.
   * Removes from worker's active set AND releases the distributed lock.
   */
  async releaseGroupSlot(workerId: string, groupId: string): Promise<void> {
    const lockKey = this.lockKey(groupId);
    const activeKey = this.activeKey(workerId);

    // Only release the lock if WE own it (guard against stale release after TTL expiry)
    const owner = await this.redis.get(lockKey);
    if (owner === workerId) {
      const pipeline = this.redis.pipeline();
      pipeline.del(lockKey);
      pipeline.srem(activeKey, groupId);
      await pipeline.exec();
    } else {
      // Lock already expired/transferred — just clean up local active set
      await this.redis.srem(activeKey, groupId);
    }
  }

  /**
   * Refresh the lock TTL for an active group.
   * Call periodically during long-running message processing to prevent
   * premature lock expiry.
   */
  async refreshGroupLock(workerId: string, groupId: string): Promise<boolean> {
    const lockKey = this.lockKey(groupId);
    const owner = await this.redis.get(lockKey);
    if (owner !== workerId) return false;
    await this.redis.pexpire(lockKey, this.LOCK_TTL_MS);
    return true;
  }

  /**
   * Get all groups currently active on a worker.
   */
  async getActiveGroups(workerId: string): Promise<string[]> {
    return this.redis.smembers(this.activeKey(workerId));
  }

  /**
   * Count active groups on a worker.
   */
  async getActiveGroupCount(workerId: string): Promise<number> {
    return this.redis.scard(this.activeKey(workerId));
  }

  // ─── Sequence counter ──────────────────────────────────────────────────────

  /**
   * Atomically increment and return the next sequence number for a group.
   * Guarantees strict ordering even across multiple publishers.
   */
  async nextSequence(groupId: string): Promise<number> {
    return this.redis.incr(this.sequenceKey(groupId));
  }

  // ─── Cleanup ──────────────────────────────────────────────────────────────

  /**
   * Clear all active group state for a worker (call on graceful shutdown).
   */
  async clearWorkerState(workerId: string): Promise<void> {
    const activeKey = this.activeKey(workerId);
    const groups = await this.redis.smembers(activeKey);

    const pipeline = this.redis.pipeline();
    for (const groupId of groups) {
      const lockKey = this.lockKey(groupId);
      // Only delete locks we own
      pipeline.eval(
        `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`,
        1,
        lockKey,
        workerId
      );
    }
    pipeline.del(activeKey);
    await pipeline.exec();
  }

  // ─── Key helpers ──────────────────────────────────────────────────────────

  private activeKey(workerId: string): string {
    return `${this.namespace}worker:${workerId}:active`;
  }

  private lockKey(groupId: string): string {
    return `${this.namespace}group:${groupId}:lock`;
  }

  private sequenceKey(groupId: string): string {
    return `${this.namespace}group:${groupId}:sequence`;
  }
}
