import { Cluster } from 'ioredis';
import RedisClient from './redis';

export class DistributedLockManager {
  private redis: Cluster;
  private readonly lockPrefix: string;
  private readonly defaultTTLSeconds: number;

  constructor(options?: {
    lockPrefix?: string;
    defaultTTLSeconds?: number;
  }) {
    this.redis = RedisClient.getClient();
    this.lockPrefix = options?.lockPrefix ?? 'lock:';
    this.defaultTTLSeconds = options?.defaultTTLSeconds ?? 30;
  }

  private getLockKey(key: string): string {
    return `${this.lockPrefix}${key}`;
  }

  private async ensureConnection(): Promise<void> {
    if (!RedisClient.isReady()) {
      await new Promise(resolve => setTimeout(resolve, 1000));
      if (!RedisClient.isReady()) {
        throw new Error('Redis connection not ready');
      }
    }
  }

  async acquireLock(
    key: string,
    ttlSeconds: number = this.defaultTTLSeconds
  ): Promise<boolean> {
    await this.ensureConnection();
    const lockKey = this.getLockKey(key);

    const acquired = await this.redis.setex(
      lockKey,
      ttlSeconds,
      '1',
    );

    return acquired === 'OK';
  }

  async releaseLock(key: string): Promise<void> {
    await this.ensureConnection();
    const lockKey = this.getLockKey(key);
    await this.redis.del(lockKey);
  }

  async withLock<T>(
    key: string,
    operation: () => Promise<T>,
    ttlSeconds: number = this.defaultTTLSeconds
  ): Promise<T | null> {
    try {
      const acquired = await this.acquireLock(key, ttlSeconds);

      if (!acquired) {
        return null;
      }

      try {
        return await operation();
      } finally {
        await this.releaseLock(key);
      }
    } catch (error) {
      console.error(`Error in withLock for key ${key}:`, error);
      await this.releaseLock(key);
      throw error;
    }
  }
} 