/**
 * test/GroupStateStore.test.ts — unit tests with manual ioredis mock
 */

const mockRedis = {
  connect:   jest.fn().mockResolvedValue(undefined),
  quit:      jest.fn().mockResolvedValue(undefined),
  sismember: jest.fn(),
  scard:     jest.fn(),
  sadd:      jest.fn(),
  srem:      jest.fn(),
  smembers:  jest.fn(),
  get:       jest.fn(),
  set:       jest.fn(),
  del:       jest.fn(),
  pexpire:   jest.fn(),
  incr:      jest.fn(),
  eval:      jest.fn(),
  pipeline:  jest.fn(),
};

jest.mock('ioredis', () => jest.fn().mockImplementation(() => mockRedis));

import { GroupStateStore } from './src/store/GroupStateStore';

function makePipeline(execResult: unknown[][]) {
  const pipe = {
    set:  jest.fn().mockReturnThis(),
    sadd: jest.fn().mockReturnThis(),
    srem: jest.fn().mockReturnThis(),
    del:  jest.fn().mockReturnThis(),
    eval: jest.fn().mockReturnThis(),
    exec: jest.fn().mockResolvedValue(execResult),
  };
  mockRedis.pipeline.mockReturnValue(pipe);
  return pipe;
}

describe('GroupStateStore', () => {
  let store: GroupStateStore;

  beforeEach(async () => {
    jest.clearAllMocks();
    store = new GroupStateStore('redis://localhost');
    await store.connect();
  });

  describe('tryAcquireGroupSlot', () => {
    it('returns "already_mine" when worker already owns the group', async () => {
      mockRedis.sismember.mockResolvedValue(1);
      mockRedis.pexpire.mockResolvedValue(1);
      const result = await store.tryAcquireGroupSlot('worker-1', 'group-A', 3);
      expect(result).toBe('already_mine');
      expect(mockRedis.pexpire).toHaveBeenCalled();
    });

    it('returns "locked_elsewhere" when another worker holds the lock', async () => {
      mockRedis.sismember.mockResolvedValue(0);
      mockRedis.get.mockResolvedValue('worker-2');
      const result = await store.tryAcquireGroupSlot('worker-1', 'group-A', 3);
      expect(result).toBe('locked_elsewhere');
    });

    it('returns "at_capacity" when worker is at maxConcurrentGroups', async () => {
      mockRedis.sismember.mockResolvedValue(0);
      mockRedis.get.mockResolvedValue(null);
      mockRedis.scard.mockResolvedValue(3);
      const result = await store.tryAcquireGroupSlot('worker-1', 'group-D', 3);
      expect(result).toBe('at_capacity');
    });

    it('returns "acquired" when slot is free and worker has capacity', async () => {
      mockRedis.sismember.mockResolvedValue(0);
      mockRedis.get.mockResolvedValue(null);
      mockRedis.scard.mockResolvedValue(1);
      makePipeline([['OK'], [1]]);
      const result = await store.tryAcquireGroupSlot('worker-1', 'group-B', 3);
      expect(result).toBe('acquired');
    });

    it('returns "locked_elsewhere" on SET NX race condition', async () => {
      mockRedis.sismember.mockResolvedValue(0);
      mockRedis.get.mockResolvedValue(null);
      mockRedis.scard.mockResolvedValue(0);
      mockRedis.srem.mockResolvedValue(1);
      // ioredis pipeline.exec() returns [[err, value], ...] pairs
      // SET NX returning null means the key already exists (lock stolen)
      makePipeline([[null, null], [null, 1]]);
      const result = await store.tryAcquireGroupSlot('worker-1', 'group-C', 3);
      expect(result).toBe('locked_elsewhere');
      expect(mockRedis.srem).toHaveBeenCalledWith('worker:worker-1:active', 'group-C');
    });
  });

  describe('releaseGroupSlot', () => {
    it('deletes lock and removes from active set when worker owns the lock', async () => {
      mockRedis.get.mockResolvedValue('worker-1');
      const pipe = makePipeline([[1], [1]]);
      await store.releaseGroupSlot('worker-1', 'group-A');
      expect(pipe.del).toHaveBeenCalledWith('group:group-A:lock');
      expect(pipe.srem).toHaveBeenCalledWith('worker:worker-1:active', 'group-A');
    });

    it('only removes from active set when lock was re-acquired by another', async () => {
      mockRedis.get.mockResolvedValue('worker-2');
      mockRedis.srem.mockResolvedValue(1);
      await store.releaseGroupSlot('worker-1', 'group-A');
      expect(mockRedis.srem).toHaveBeenCalledWith('worker:worker-1:active', 'group-A');
      expect(mockRedis.pipeline).not.toHaveBeenCalled();
    });
  });

  describe('refreshGroupLock', () => {
    it('returns true and refreshes TTL when worker owns lock', async () => {
      mockRedis.get.mockResolvedValue('worker-1');
      mockRedis.pexpire.mockResolvedValue(1);
      const ok = await store.refreshGroupLock('worker-1', 'group-A');
      expect(ok).toBe(true);
      expect(mockRedis.pexpire).toHaveBeenCalledWith('group:group-A:lock', 60_000);
    });

    it('returns false when another worker owns the lock', async () => {
      mockRedis.get.mockResolvedValue('worker-2');
      const ok = await store.refreshGroupLock('worker-1', 'group-A');
      expect(ok).toBe(false);
      expect(mockRedis.pexpire).not.toHaveBeenCalled();
    });
  });

  describe('nextSequence', () => {
    it('returns monotonically increasing numbers per group', async () => {
      mockRedis.incr
        .mockResolvedValueOnce(1)
        .mockResolvedValueOnce(2)
        .mockResolvedValueOnce(3);
      expect(await store.nextSequence('group-A')).toBe(1);
      expect(await store.nextSequence('group-A')).toBe(2);
      expect(await store.nextSequence('group-A')).toBe(3);
      expect(mockRedis.incr).toHaveBeenCalledWith('group:group-A:sequence');
    });
  });

  describe('getActiveGroups / getActiveGroupCount', () => {
    it('returns current active group list', async () => {
      mockRedis.smembers.mockResolvedValue(['grp-1', 'grp-2']);
      const groups = await store.getActiveGroups('worker-1');
      expect(groups).toEqual(['grp-1', 'grp-2']);
    });

    it('returns count of active groups', async () => {
      mockRedis.scard.mockResolvedValue(2);
      const count = await store.getActiveGroupCount('worker-1');
      expect(count).toBe(2);
    });
  });

  describe('clearWorkerState', () => {
    it('releases all locks and clears the active set', async () => {
      mockRedis.smembers.mockResolvedValue(['grp-A', 'grp-B']);
      const pipe = {
        eval: jest.fn().mockReturnThis(),
        del:  jest.fn().mockReturnThis(),
        exec: jest.fn().mockResolvedValue([[1], [1], [1]]),
      };
      mockRedis.pipeline.mockReturnValue(pipe);
      await store.clearWorkerState('worker-1');
      expect(pipe.eval).toHaveBeenCalledTimes(2); // one Lua CAS per group
      expect(pipe.del).toHaveBeenCalledWith('worker:worker-1:active');
      expect(pipe.exec).toHaveBeenCalled();
    });
  });
});
