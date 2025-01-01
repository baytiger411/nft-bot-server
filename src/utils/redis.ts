import { config } from 'dotenv';
import Redis, { Cluster } from 'ioredis';

config()

const REDIS_NODES = process.env.REDIS_NODES as string

const getRedisNodes = () => {
  if (!REDIS_NODES) {
    throw new Error('REDIS_NODES is not defined in the environment variables');
  }
  return JSON.parse(REDIS_NODES)
};

const REDIS_NET_MAP = JSON.parse(process.env.REDIS_NET_MAP as string)

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
  },
  slotsRefreshTimeout: 30000,
  showFriendlyErrorStack: true,
  enableOfflineQueue: true,
  reconnectOnError: (err: Error) => {
    const targetError = 'READONLY';
    if (err.message.includes(targetError)) {
      return true;
    }
    return false;
  }
};

class RedisClient {
  private static instance: RedisClient;
  private client: Cluster | null = null;
  private isConnected: boolean = false;
  private connectionAttempts: number = 0;
  private readonly maxConnectionAttempts = 10;
  private connecting: boolean = false;

  private constructor() {
    this.connect();
  }

  public static getInstance(): RedisClient {
    if (!RedisClient.instance) {
      RedisClient.instance = new RedisClient();
    }
    return RedisClient.instance;
  }

  private async connect() {
    if (this.connecting || this.isConnected) {
      return;
    }

    this.connecting = true;

    if (!this.client) {
      try {
        console.log('Initializing Redis Cluster connection...');

        this.client = new Redis.Cluster(getRedisNodes(), {
          ...defaultConfig,
          scaleReads: 'all',
          redisOptions: {
            ...defaultConfig.redisOptions,
            lazyConnect: true,
          },
          enableOfflineQueue: true,
          slotsRefreshTimeout: 30000,
          dnsLookup: (address, callback) => callback(null, address),
          natMap: REDIS_NET_MAP
        });

        this.setupEventHandlers();
      } catch (error) {
        console.error('Redis connection error:', error);
        this.isConnected = false;
        this.client = null;

        if (this.connectionAttempts < this.maxConnectionAttempts) {
          this.connectionAttempts++;
          setTimeout(() => {
            this.connecting = false;
            this.connect();
          }, 5000);
        } else {
          this.connecting = false;
          throw error;
        }
      }
    }
  }

  private setupEventHandlers() {
    if (!this.client) return;

    this.client.on('error', (err) => {
      this.isConnected = false;
      console.error('Redis Client Error:', err.message);
      if (!this.connecting && this.connectionAttempts < this.maxConnectionAttempts) {
        this.connectionAttempts++;
        setTimeout(() => this.connect(), 5000);
      }
    });

    this.client.on('ready', () => {
      this.isConnected = true;
      this.connecting = false;
      this.connectionAttempts = 0;
      console.log('Redis Cluster is ready');
    });

    this.client.on('connect', () => {
      console.log('Redis Cluster connecting...');
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
