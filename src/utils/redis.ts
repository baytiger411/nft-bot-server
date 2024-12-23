import { config } from 'dotenv';
import Redis, { Cluster } from 'ioredis';

config()

const REDIS_URI = process.env.REDIS_URI as string

const getRedisNodes = () => {
  if (!REDIS_URI) {
    throw new Error('REDIS_URL is not defined in the environment variables');
  }

  return [
    { host: '127.0.0.1', port: 8001 },
    { host: '127.0.0.1', port: 8002 },
    { host: '127.0.0.1', port: 8003 },
    { host: '127.0.0.1', port: 8004 },
    { host: '127.0.0.1', port: 8005 },
    { host: '127.0.0.1', port: 8006 },
    { host: '127.0.0.1', port: 8007 },
    { host: '127.0.0.1', port: 8008 },
    { host: '127.0.0.1', port: 8009 },
  ];
};

const RETRY_MAX_ATTEMPTS = 30;
const INITIAL_RETRY_DELAY = 2000;

const defaultConfig = {
  maxRetriesPerRequest: null,
  retryStrategy: (times: number) => {
    const delay = Math.min(times * INITIAL_RETRY_DELAY, 20000);
    console.log(`Retrying connection attempt ${times} after ${delay}ms`);
    return delay;
  },
  clusterRetryStrategy: (times: number) => {
    if (times > RETRY_MAX_ATTEMPTS) {
      console.error('Max retry attempts reached, giving up...');
      return null;
    }
    const delay = Math.min(times * INITIAL_RETRY_DELAY, 20000);
    console.log(`Retrying cluster connection attempt ${times} after ${delay}ms`);
    return delay;
  },
  redisOptions: {
    enableReadyCheck: true,
    maxRetriesPerRequest: null,
    connectTimeout: 30000,
    retryStrategy: (times: number) => {
      const delay = Math.min(times * INITIAL_RETRY_DELAY, 20000);
      console.log(`Retrying redis connection attempt ${times} after ${delay}ms`);
      return delay;
    }
  }
};

class RedisClient {
  private static instance: RedisClient;
  private client: Cluster | null = null;
  private isConnected: boolean = false;
  private connectionAttempts: number = 0;
  private readonly maxConnectionAttempts = 10;

  private constructor() {
    setTimeout(() => this.connect(), 5000);
  }

  public static getInstance(): RedisClient {
    if (!RedisClient.instance) {
      RedisClient.instance = new RedisClient();
    }
    return RedisClient.instance;
  }

  private async connect() {
    if (!this.client) {
      try {
        console.log('Initializing Redis Cluster connection...');

        this.client = new Redis.Cluster(getRedisNodes(), {
          ...defaultConfig,
          scaleReads: "all",
          redisOptions: {
            ...defaultConfig.redisOptions,
            lazyConnect: true,
          },
          enableOfflineQueue: true,
          slotsRefreshTimeout: 30000,
          dnsLookup: (address, callback) => callback(null, address),
          natMap: {
            'redis-node-1:6379': { host: '127.0.0.1', port: 8001 },
            'redis-node-2:6379': { host: '127.0.0.1', port: 8002 },
            'redis-node-3:6379': { host: '127.0.0.1', port: 8003 },
            'redis-node-4:6379': { host: '127.0.0.1', port: 8004 },
            'redis-node-5:6379': { host: '127.0.0.1', port: 8005 },
            'redis-node-6:6379': { host: '127.0.0.1', port: 8006 },
            'redis-node-7:6379': { host: '127.0.0.1', port: 8007 },
            'redis-node-8:6379': { host: '127.0.0.1', port: 8008 },
            'redis-node-9:6379': { host: '127.0.0.1', port: 8009 }
          }
        });

        this.client.on('error', (err) => {
          this.isConnected = false;
          console.error('Redis Client Error:', err.message);
          if (this.connectionAttempts < this.maxConnectionAttempts) {
            this.connectionAttempts++;
            setTimeout(() => {
              console.log('Attempting to reconnect after error...');
              this.connect();
            }, 5000);
          }
        });

        this.client.on('connect', () => {
          console.log('Redis Cluster connecting...');
        });

        this.client.on('ready', () => {
          this.isConnected = true;
          this.connectionAttempts = 0;
          console.log('Redis Cluster is ready');
        });

        this.client.on('reconnecting', () => {
          console.log('Redis Cluster reconnecting...');
        });

        this.client.on('node error', (err: Error, node: any) => {
          console.error('Redis Cluster Node Error:', err.message);
          if (node) {
            console.error('Node details:', {
              id: node.id || 'unknown',
              address: node.address || 'unknown',
              options: node.options || {}
            });
          }
        });

        await this.client.connect()

      } catch (error) {
        if (this.connectionAttempts < this.maxConnectionAttempts) {
          this.connectionAttempts++;
          setTimeout(() => this.connect(), 5000);
        } else {
          throw error;
        }
      }
    }
  }

  public getClient(): Cluster {
    if (!this.client) {
      this.connect();
    }
    return this.client!;
  }

  public isReady(): boolean {
    return this.isConnected;
  }
}

export default RedisClient.getInstance();
