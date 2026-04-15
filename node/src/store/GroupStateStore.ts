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

    // Fast path compatibility: if the local active set says "mine" AND the lock
    // is still owned by us, extend the TTL. If the lock was taken by another
    // worker (TTL expired during a pause), drop the stale entry and fall
    // through to the atomic acquire so we cannot refresh someone else's lock.
    const isAlreadyMine = await this.redis.sismember(activeKey, groupId);
    if (isAlreadyMine) {
      const owner = await this.redis.get(lockKey);
      if (owner === workerId) {
        await this.redis.pexpire(lockKey, this.LOCK_TTL_MS);
        return 'already_mine';
      }
      // Stale local state — remove before re-acquiring via the atomic script.
      await this.redis.srem(activeKey, groupId);
    }

    // Atomic check-and-set via Lua: verify lock owner, enforce capacity,
    // acquire lock, and add to active set in one round trip. This closes the
    // TOCTOU race between SCARD and SADD that allowed exceeding the cap.
    //
    // KEYS[1] = lockKey
    // KEYS[2] = activeKey
    // ARGV[1] = workerId
    // ARGV[2] = maxConcurrentGroups (stringified int)
    // ARGV[3] = lockTtlMs (stringified int)
    // ARGV[4] = groupId
    // Returns: "acquired" | "already_mine" | "locked_elsewhere" | "at_capacity"
    const script = `
      local owner = redis.call("GET", KEYS[1])
      if owner and owner ~= ARGV[1] then
        return "locked_elsewhere"
      end
      if owner == ARGV[1] then
        if redis.call("SISMEMBER", KEYS[2], ARGV[4]) == 1 then
          redis.call("PEXPIRE", KEYS[1], tonumber(ARGV[3]))
          return "already_mine"
        end
      end
      local max = tonumber(ARGV[2])
      local count = redis.call("SCARD", KEYS[2])
      if count >= max then
        return "at_capacity"
      end
      redis.call("SET", KEYS[1], ARGV[1], "PX", tonumber(ARGV[3]))
      redis.call("SADD", KEYS[2], ARGV[4])
      return "acquired"
    `;

    const maxArg =
      maxConcurrentGroups === Infinity ? '2147483647' : String(maxConcurrentGroups);

    const result = (await this.redis.eval(
      script,
      2,
      lockKey,
      activeKey,
      workerId,
      maxArg,
      String(this.LOCK_TTL_MS),
      groupId
    )) as string;

    return result as 'acquired' | 'already_mine' | 'locked_elsewhere' | 'at_capacity';
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
