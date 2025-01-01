import { ethers } from "ethers";
import { Cluster } from 'ioredis';
import { DistributedLockManager } from './lock';
import RedisClient from './redis';

// Constants
const CACHE_EXPIRY_SECONDS = 60;
const BLUR_POOL_ADDRESS = "0x0000000000A39bb272e79075ade125fd351887Ac";
const WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";

// Environment configuration
const config = {
  API_KEY: process.env.API_KEY,
  ALCHEMY_KEY: process.env.ALCHEMY_KEY,
  OPENSEA_API_URL: 'https://api.nfttools.website/opensea/__api/graphql/',
};

// Type Definitions
type BalanceType = 'weth' | 'beth';

interface Dependencies {
  provider: ethers.providers.Provider;
}

class BalanceChecker {
  private deps: Dependencies;
  private lockManager: DistributedLockManager;
  private redis: Cluster;

  constructor(deps: Dependencies) {
    this.deps = deps;
    this.redis = RedisClient.getClient();
    this.lockManager = new DistributedLockManager({
      lockPrefix: '{balance_lock}:',
      defaultTTLSeconds: CACHE_EXPIRY_SECONDS
    });
  }

  private async getCachedBalance(cacheKey: string): Promise<number | null> {
    try {
      const cachedBalance = await this.redis.get(cacheKey);
      return cachedBalance ? Number(cachedBalance) : null;
    } catch (error) {
      console.error('Redis Cluster cache error:', error);
      return null;
    }
  }

  private async setCachedBalance(cacheKey: string, balance: number): Promise<void> {
    try {
      await this.redis.set(
        cacheKey,
        balance.toString(),
        'EX',
        CACHE_EXPIRY_SECONDS
      );
    } catch (error) {
      console.error('Redis Cluster cache set error:', error);
    }
  }

  async getWethBalance(address: string): Promise<number> {
    const cacheKey = `weth_balance:${address}`;
    let cachedBalance = await this.getCachedBalance(cacheKey);

    const result = await this.lockManager.withLock(
      `weth_${address}`,
      async () => {
        // Check cache again after acquiring lock
        const cachedBalanceAfterLock = await this.getCachedBalance(cacheKey);
        if (cachedBalanceAfterLock !== null) {
          return cachedBalanceAfterLock;
        }

        const wethContract = new ethers.Contract(
          WETH_ADDRESS,
          ['function balanceOf(address) view returns (uint256)'],
          this.deps.provider
        );

        // Retry logic with exponential backoff
        const maxRetries = 3;
        for (let attempt = 0; attempt <= maxRetries; attempt++) {
          try {
            const balance = await wethContract.balanceOf(address);
            const formattedBalance = Number(ethers.utils.formatEther(balance));

            await this.setCachedBalance(cacheKey, formattedBalance);
            return formattedBalance;
          } catch (error) {
            if (attempt === maxRetries) {
              console.error("Error fetching WETH balance after retries:", error);
              return cachedBalance ?? 0;
            }
            const backoffTime = Math.pow(2, attempt) * 1000;
            await new Promise(resolve => setTimeout(resolve, backoffTime));
          }
        }
      }
    );

    return result ?? (cachedBalance ?? 0);
  }

  async getBethBalance(address: string): Promise<number> {
    const cacheKey = `beth_balance:${address}`;
    let cachedBalance = await this.getCachedBalance(cacheKey);

    const result = await this.lockManager.withLock(
      `beth_${address}`,
      async () => {
        const cachedBalanceAfterLock = await this.getCachedBalance(cacheKey);
        if (cachedBalanceAfterLock !== null) {
          return cachedBalanceAfterLock;
        }

        const wethContract = new ethers.Contract(
          BLUR_POOL_ADDRESS,
          ['function balanceOf(address) view returns (uint256)'],
          this.deps.provider
        );

        const maxRetries = 3;
        for (let attempt = 0; attempt <= maxRetries; attempt++) {
          try {
            const balance = await wethContract.balanceOf(address);
            const formattedBalance = Number(ethers.utils.formatEther(balance));

            await this.setCachedBalance(cacheKey, formattedBalance);
            return formattedBalance;
          } catch (error) {
            if (attempt === maxRetries) {
              console.error("Error fetching BETH balance after retries:", error);
              return cachedBalance ?? 0;
            }
            const backoffTime = Math.pow(2, attempt) * 1000; // Exponential backoff
            await new Promise(resolve => setTimeout(resolve, backoffTime));
          }
        }
      }
    );

    return result ?? (cachedBalance ?? 0);
  }
}

// Usage example
export function createBalanceChecker(deps: Dependencies): BalanceChecker {
  return new BalanceChecker(deps);
}
