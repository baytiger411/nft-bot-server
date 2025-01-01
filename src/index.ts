import express from "express";
import { config } from "dotenv";
import bodyParser from "body-parser";
import { clearTimeout } from 'timers';
import os from "os"
import cors from "cors";
import WebSocket, { Server as WebSocketServer } from 'ws';
import http from 'http';
import { initialize } from "./init";
import { bidOnOpensea, cancelOrder, fetchOpenseaListings, fetchOpenseaOffers, IFee } from "./marketplace/opensea";
import { bidOnBlur, cancelBlurBid, fetchBlurBid, fetchBlurCollectionStats } from "./marketplace/blur/bid";
import { bidOnMagiceden, cancelMagicEdenBid, fetchMagicEdenCollectionStats, fetchMagicEdenOffer, fetchMagicEdenTokens } from "./marketplace/magiceden";
import { getCollectionDetails, getCollectionStats } from "./functions";
import mongoose from 'mongoose';
import Task from "./models/task.model";
import { Queue, Worker, Job, QueueOptions, JobType } from "bullmq";
import Wallet from "./models/wallet.model";
import redisClient from "./utils/redis";
import { WETH_CONTRACT_ADDRESS, WETH_MIN_ABI } from "./constants";
import { BigNumber, constants, Contract, ethers, utils, Wallet as Web3Wallet } from "ethers";
import { DistributedLockManager } from "./utils/lock";

let RATE_LIMIT = Number(process.env.RATE_LIMIT);
let API_KEY = process.env.API_KEY;
const bidStats: BidCounts = {};
export const errorStats: BidCounts = {};
const skipStats: BidCounts = {};
const redis = redisClient.getClient()
const QUEUE_NAME = '{bull}BIDDING_BOT';
const CPU_COUNT = os.cpus().length;
const WORKER_COUNT = Math.max(2, Math.min(CPU_COUNT - 1, 4));
const JOB_TIMEOUT = 30000;

const SEAPORT = '0x1e0049783f008a0085193e00003d00cd54003c71';
const CANCEL_PRIORITY = {
  OPENSEA: 1,
  MAGICEDEN: 1,
  BLUR: 1
};

const TOKEN_BID_PRIORITY = {
  OPENSEA: 4,
  MAGICEDEN: 4,
  BLUR: 4
};

const TRAIT_BID_PRIORITY = {
  OPENSEA: 4,
  MAGICEDEN: 4,
  BLUR: 4
};

const COLLECTION_BID_PRIORITY = {
  OPENSEA: 4,
  MAGICEDEN: 4,
  BLUR: 4
};
config()

const lockManager = new DistributedLockManager({
  lockPrefix: '{marketplace}:',
  defaultTTLSeconds: 60
});

export { redis, lockManager, queue };

const QUEUE_OPTIONS: QueueOptions = {
  prefix: '{bull}',
  connection: redis,
  defaultJobOptions: {
    removeOnComplete: true,
    removeOnFail: true,
    attempts: 1,
    backoff: {
      type: 'fixed',
      delay: 1000
    },
  }
};

const workers = Array.from({ length: WORKER_COUNT }, (_, index) => new Worker(
  QUEUE_NAME,
  async (job) => {
    try {
      // Add timeout protection for job processing
      const result = await Promise.race([
        processJob(job),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error('Job timeout')), JOB_TIMEOUT)
        )
      ]);

      broadcastBidRates()
      return result;
    } catch (error) {
      throw error;
    }
  },
  {
    connection: redis,
    prefix: '{bull}',
    concurrency: RATE_LIMIT,
    lockDuration: 30000,
    stalledInterval: 30000,
    maxStalledCount: 1,

    name: `worker-${index + 1}`
  }
));



const BROADCAST_INTERVAL = 1000;
setInterval(broadcastBidRates, BROADCAST_INTERVAL);


// Add queue cleanup interval
setInterval(async () => {
  try {
    // Clean completed and failed jobs older than 1 hour

    // Clean stalled jobs
    await queue.clean(30000, 10000, 'wait');

    console.log(BLUE + '🧹 Queue cleanup completed' + RESET);
  } catch (error) {
    console.error(RED + 'Queue cleanup error:', error, RESET);
  }
}, 300000); // Run every 5 minutes


const queue = new Queue(QUEUE_NAME, QUEUE_OPTIONS);

export const MAGENTA = '\x1b[35m';
export const BLUE = '\x1b[34m';
export const RESET = '\x1b[0m';
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
const RED = '\x1b[31m';
const GOLD = '\x1b[33m';

const OPENSEA = "OPENSEA";
const MAGICEDEN = "MAGICEDEN";
const BLUR = "BLUR";

export const currentTasks: ITask[] = [];

export const OPENSEA_SCHEDULE = "OPENSEA_SCHEDULE"
export const OPENSEA_TRAIT_BID = "OPENSEA_TRAIT_BID"
export const BLUR_TRAIT_BID = "BLUR_TRAIT_BID"
export const BLUR_SCHEDULE = "BLUR_SCHEDULE"
const MAGICEDEN_SCHEDULE = "MAGICEDEN_SCHEDULE"
const MAGICEDEN_TOKEN_BID = "MAGICEDEN_TOKEN_BID"
export const OPENSEA_TOKEN_BID = "OPENSEA_TOKEN_BID"
export const OPENSEA_TOKEN_BID_COUNTERBID = "OPENSEA_TOKEN_BID_COUNTERBID"
export const OPENSEA_TRAIT_BID_COUNTERBID = "OPENSEA_TRAIT_BID_COUNTERBID"
export const OPENSEA_COLLECTION_BID_COUNTERBID = "OPENSEA_COLLECTION_BID_COUNTERBID"
export const BLUR_COLLECTION_BID_COUNTERBID = "BLUR_COLLECTION_BID_COUNTERBID"
export const BLUR_TRAIT_BID_COUNTERBID = "BLUR_TRAIT_BID_COUNTERBID"
export const MAGICEDEN_COLLECTION_BID_COUNTERBID = "MAGICEDEN_COLLECTION_BID_COUNTERBID"
export const MAGICEDEN_TOKEN_BID_COUNTERBID = "MAGICEDEN_TOKEN_BID_COUNTERBID"
export const MAGICEDEN_TRAIT_BID_COUNTERBID = "MAGICEDEN_TRAIT_BID_COUNTERBID"
const MAGICEDEN_TRAIT_BID = "MAGICEDEN_TRAIT_BID"
const CANCEL_OPENSEA_BID = "CANCEL_OPENSEA_BID"
const CANCEL_MAGICEDEN_BID = "CANCEL_MAGICEDEN_BID"
const CANCEL_BLUR_BID = "CANCEL_BLUR_BID"
const MAGICEDEN_MARKETPLACE = "0x9A1D00bEd7CD04BCDA516d721A596eb22Aac6834"
const MAX_RETRIES: number = 5;
const MARKETPLACE_WS_URL = "wss://wss-marketplace.nfttools.website";
const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY as string;
const PRIORITIZED_THRESHOLD = RATE_LIMIT * WORKER_COUNT;
const OPENSEA_PROTOCOL_ADDRESS = "0x0000000000000068F116a894984e2DB1123eB395"
const HEALTH_CHECK_INTERVAL = 10000;

const MAX_ACTIVE_JOBS = WORKER_COUNT * RATE_LIMIT;
const QUEUE_LOW_WATERMARK = MAX_ACTIVE_JOBS * 0.75;
const MAX_MEMORY_USAGE = 75;
const MAX_PRIORITIZED_JOBS = 1000 * WORKER_COUNT;
const DRAIN_THRESHOLD = 100;

const app = express();
const port = process.env.PORT || 3003;

app.use(bodyParser.json());
app.use(cors());

const server = http.createServer(app);
const wss = new WebSocketServer({ server });
let ws: WebSocket;
let heartbeatIntervalId: NodeJS.Timeout | null = null;
let reconnectTimeoutId: NodeJS.Timeout | null = null;
let retryCount: number = 0;

const taskIntervals = new Map<string, NodeJS.Timeout>();
const activeSubscriptions: Set<string> = new Set();
const clients = new Set<WebSocket>();


const walletsArr: string[] = []

const MIN_BID_DURATION = 60

function cleanupMemory() {
  if (global.gc) {
    global.gc();
  }
}


async function monitorHealth() {
  try {
    console.log({ bidStats });
    const counts = await queue.getJobCounts();
    const used = process.memoryUsage();
    const memoryStats = {
      rss: `${Math.round(used.rss / 1024 / 1024)}MB`,
      heapTotal: `${Math.round(used.heapTotal / 1024 / 1024)}MB`,
      heapUsed: `${Math.round(used.heapUsed / 1024 / 1024)}MB`,
      external: `${Math.round(used.external / 1024 / 1024)}MB`,
    }

    const totalMemory = os.totalmem();
    const memoryUsagePercent = (used.heapUsed / totalMemory) * 100;

    if (memoryUsagePercent > 75) {
      console.log(RED + `⚠️  HIGH MEMORY USAGE (${memoryUsagePercent.toFixed(2)}%) - INITIATING CLEANUP... ⚠️` + RESET);
      await queue.pause();
      cleanupMemory();

      setTimeout(async () => {
        await queue.resume();
      }, 5000);
    }

    if (counts.active > PRIORITIZED_THRESHOLD) {
      await queue.pause();
      setTimeout(async () => {
        await queue.resume();
      }, 2500);
    } else {
      await queue.resume();
    }

    const totalJobs = Object.values(counts).reduce((a, b) => a + b, 0);

    console.log('\n📊 ═══════ Health Monitor Stats ═══════ 📊');

    console.log(BLUE + '\n💾 Memory Usage:' + RESET);
    Object.entries(memoryStats).forEach(([key, value]) => {
      console.log(`   ${key}: ${value}`);
    });

    console.log(BLUE + '\n📋 Queue Status:' + RESET);
    Object.entries(counts).forEach(([key, value]) => {
      const color = value > RATE_LIMIT * 2 ? RED : value > RATE_LIMIT ? YELLOW : GREEN;
      console.log(`   ${key}: ${color}${value}${RESET}`);
    });
    console.log(`   Total Jobs: ${totalJobs}`);

    const activeWorkers = workers.filter(w => w.isRunning()).length;
    console.log(BLUE + '\n👷 Worker Status:' + RESET, `${GREEN}✅ ${activeWorkers}/${WORKER_COUNT} Workers Active${RESET}`);

  } catch (error) {
    console.error(RED + '❌ Error monitoring health:' + RESET, error);
  }
}


setInterval(monitorHealth, HEALTH_CHECK_INTERVAL);

// Update cleanup function to handle multiple workers
async function cleanup() {
  console.log(YELLOW + '\n=== Starting Cleanup ===');

  // Clear task locks
  taskLockMap.clear();
  console.log('Cleared task locks...');

  console.log('Closing workers...');
  await Promise.all(workers.map(worker => worker.close()));

  console.log('Closing queue...');
  await queue.close();

  console.log(GREEN + '=== Cleanup Complete ===\n' + RESET);
  process.exit(0);
}


process.on('SIGTERM', cleanup);
process.on('SIGINT', cleanup);

// Add this near the top with other global variables
export const activeTasks: Map<string, ITask> = new Map();

// Modify fetchCurrentTasks to populate activeTasks
async function fetchCurrentTasks() {
  try {
    console.log(BLUE + '\n=== Fetching Current Tasks ===' + RESET);
    const tasks = await Task.find({}).lean().exec() as unknown as ITask[];
    const wallets = (await Wallet.find({}).lean()).map((wallet: any) => wallet.address);

    walletsArr.push(...wallets);
    const formattedTasks = tasks.map((task) => ({
      ...task,
      _id: task._id.toString(),
      user: task.user.toString()
    }));

    // Clear and populate activeTasks Map
    activeTasks.clear();

    formattedTasks.forEach(task => {
      activeTasks.set(task._id, task);
    });

    console.log(`Found ${formattedTasks.length} tasks`);
    currentTasks.push(...formattedTasks);

    console.log('Creating initial jobs...');
    const jobs = formattedTasks
      .filter((task) => task.running)
      .flatMap(task =>
        task.running && task.selectedMarketplaces.map(m => m.toLowerCase()).includes("opensea")
          ? [{ name: OPENSEA_SCHEDULE, data: task }]
          : []
      ).concat(
        formattedTasks.flatMap(task =>
          task.running && task.selectedMarketplaces.map(m => m.toLowerCase()).includes("blur")
            ? [{ name: BLUR_SCHEDULE, data: task }]
            : []
        )
      ).concat(
        formattedTasks.flatMap(task =>
          task.running && task.selectedMarketplaces.map(m => m.toLowerCase()).includes("magiceden")
            ? [{ name: MAGICEDEN_SCHEDULE, data: task }]
            : []
        )
      );

    console.log(`Created ${jobs.length} initial jobs`);
    // await processBulkJobs(jobs);



    console.log(GREEN + '=== Task Initialization Complete ===\n' + RESET);
  } catch (error) {
    console.error(RED + 'Error fetching current tasks:', error, RESET);
  }
}

const DOWNTIME_THRESHOLD = 5 * 60 * 1000;
const LAST_RUNTIME_KEY = 'server:last_runtime';

async function startServer() {
  try {
    await mongoose.connect(process.env.MONGODB_URI as string, {
      serverSelectionTimeoutMS: 15000,
      connectTimeoutMS: 15000,
      socketTimeoutMS: 30000,
      maxPoolSize: 10,
      retryWrites: true
    });
    console.log('Connected to MongoDB');

    // Get Redis client instance and ensure it's ready

    // Wrap Redis operations in try-catch
    try {
      const lastRuntime = await redis.get(LAST_RUNTIME_KEY);
      console.log('Last runtime:', lastRuntime ? new Date(parseInt(lastRuntime)).toLocaleString() : 'Never');

      const currentTime = Date.now();

      if (lastRuntime) {
        const downtime = currentTime - parseInt(lastRuntime);
        if (downtime > DOWNTIME_THRESHOLD) {
          console.log(YELLOW + `Server was down for ${Math.round(downtime / 60000)} minutes. Clearing queue...` + RESET);
          const states: JobType[] = ['active', 'prioritized', 'delayed', 'failed', 'completed', 'wait', 'paused'];
          for (const state of states) {
            const jobs = await queue.getJobs([state]);
            await Promise.allSettled(jobs.map(async (job) => {
              const currentState = await job.getState();
              if (currentState !== 'active') {
                await job?.remove();
              }
            }));
          }
        }
      }

      await initialize(RATE_LIMIT);

      await redis.set(LAST_RUNTIME_KEY, currentTime.toString());
    } catch (redisError) {
      console.error('Redis operation failed:', redisError);
      // Consider whether to exit here based on your requirements
    }

    setInterval(async () => {
      await redis.set(LAST_RUNTIME_KEY, Date.now().toString());
    }, 60000);

    server.listen(port, () => {
      console.log(`Magic happening on http://localhost:${port}`);
      console.log(`WebSocket server is running on ws://localhost:${port}`);
    });
    await fetchCurrentTasks()

    // Initialize task locks for existing tasks
    for (const [taskId, task] of activeTasks.entries()) {
      if (task.running) {
        taskLockMap.set(taskId, {
          lockedUntil: 0,
          loopCount: 0
        });
      }
    }

  } catch (error) {
    console.error(RED + 'Failed to start server:' + RESET, error);
    process.exit(1);
  }
}

startServer()
  .then(() => {
    runScheduledLoop().catch(error => {
      console.error('Failed to run scheduled loop:', error);
    })
    connectWebSocket().catch(error => {
      console.error('Failed to connect to WebSocket:', error);
    })
  })
  .catch(error => {
    console.error('Failed to start server:', error);
  });


function broadcastBidRates() {
  const bidRates = getAllBidRates();
  const message = JSON.stringify({
    type: 'bidRatesUpdate',
    data: { bidRates, bidCounts: bidStats, skipCounts: skipStats, errorCounts: errorStats }
  });

  clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}


// Update the WebSocket connection handler to send initial state
wss.on('connection', async (ws) => {
  // Add the new client to the set
  clients.add(ws);

  console.log(GREEN + 'New WebSocket connection' + RESET);


  ws.onmessage = async (event: WebSocket.MessageEvent) => {
    try {
      const message = JSON.parse(event.data as string);
      switch (message.endpoint) {
        case 'new-task':
          await processNewTask(message.data);
          break;
        case "import-tasks":
          await processImportedTasks(message.data);
          break;
        case 'updated-task':
          await processUpdatedTask(message.data);
          break;
        case 'toggle-status':
          await updateStatus(message.data);
          break;
        case 'stop-task':
          await stopTask(message.data, false);
          break;
        case 'update-multiple-tasks-status':
          await updateMultipleTasksStatus(message.data);
          break;
        case 'update-marketplace':
          await updateMarketplace(message.data)
          break
        case 'retry-connection':
          await connectWebSocket()
          break
        case 'update-config':
          await updateConfig(message.data)
          break
        default:
          console.warn(YELLOW + `Unknown endpoint: ${message.endpoint}` + RESET);
      }
    } catch (error) {
      console.error(RED + 'Error handling WebSocket message:' + RESET, error);
    }
  };

  ws.onclose = () => {
    // Remove client when disconnected
    clients.delete(ws);
    console.log(YELLOW + 'WebSocket connection closed' + RESET);
  };
});


import fs from 'fs/promises';
import path from 'path';
import { log } from "console";

async function updateConfig(data: any) {
  try {
    const { apiKey, rateLimit } = data;
    const oldRateLimit = RATE_LIMIT;

    API_KEY = apiKey
    RATE_LIMIT = rateLimit
    const envPath = path.resolve(process.cwd(), '.env');

    let envContent = '';
    try {
      envContent = await fs.readFile(envPath, 'utf-8');
    } catch (error) {
      console.log(YELLOW + 'Creating new .env file' + RESET);
      await fs.writeFile(envPath, '', 'utf-8');
    }

    const envLines = envContent.split('\n').filter(line => line.trim());
    const newEnv = new Map(
      envLines.map(line => {
        const [key, ...values] = line.split('=');
        return [key, values.join('=')];
      })
    );

    if (apiKey) {
      newEnv.set('API_KEY', apiKey);
      process.env.API_KEY = apiKey;
    }

    if (rateLimit) {
      newEnv.set('RATE_LIMIT', rateLimit.toString());
      process.env.RATE_LIMIT = rateLimit.toString();
    }


    const newContent = Array.from(newEnv.entries())
      .map(([key, value]) => `${key}=${value}`)
      .join('\n');

    // Write to file
    await fs.writeFile(envPath, newContent + '\n', 'utf-8');

    console.log(GREEN + 'Configuration updated successfully' + RESET);

    if (oldRateLimit !== rateLimit) {
      console.log(YELLOW + 'Rate limit changed. Restarting server components...' + RESET);
      await queue.pause();
      await initialize(RATE_LIMIT);
    }

    await queue.resume();

    console.log(GREEN + 'Server components restarted successfully' + RESET);

    return {
      apiKey: process.env.API_KEY,
      rateLimit: process.env.RATE_LIMIT
    };

  } catch (error) {
    console.error(RED + 'Error updating configuration:' + RESET, error);
    throw error;
  }
}

async function processJob(job: Job) {
  switch (job.name) {
    case OPENSEA_COLLECTION_BID_COUNTERBID:
      return await openseaCollectionCounterBid(job.data)
    case MAGICEDEN_COLLECTION_BID_COUNTERBID:
      return await magicedenCollectionCounterBid(job.data)
    case BLUR_COLLECTION_BID_COUNTERBID:
      return await blurCollectionCounterBid(job.data)
    case OPENSEA_TOKEN_BID_COUNTERBID:
      return await openseaTokenCounterBid(job.data);
    case BLUR_TRAIT_BID_COUNTERBID:
      return await blurTraitCounterBid(job.data);
    case OPENSEA_TRAIT_BID_COUNTERBID:
      return await openseaTraitCounterBid(job.data);
    case MAGICEDEN_TOKEN_BID_COUNTERBID:
      return await magicedenTokenCounterBid(job.data);
    case MAGICEDEN_TRAIT_BID_COUNTERBID:
      return await magicedenTraitCounterBid(job.data);
    case OPENSEA_SCHEDULE:
      return await processOpenseaScheduledBid(job.data);
    case OPENSEA_TRAIT_BID:
      return await processOpenseaTraitBid(job.data);
    case OPENSEA_TOKEN_BID:
      return await processOpenseaTokenBid(job.data);
    case BLUR_SCHEDULE:
      return await processBlurScheduledBid(job.data);
    case BLUR_TRAIT_BID:
      return await processBlurTraitBid(job.data);
    case MAGICEDEN_SCHEDULE:
      return await processMagicedenScheduledBid(job.data);
    case MAGICEDEN_TRAIT_BID:
      return await processMagicedenTraitBid(job.data);
    case MAGICEDEN_TOKEN_BID:
      return await processMagicedenTokenBid(job.data);
    case CANCEL_OPENSEA_BID:
      return await bulkCancelOpenseaBid(job.data);
    case CANCEL_MAGICEDEN_BID:
      return await bulkCancelMagicedenBid(job.data);
    case CANCEL_BLUR_BID:
      return await blukCancelBlurBid(job.data);

    default:
      throw new Error(`Unknown job type: ${job.name}`);
  }
}

// Add these at the top with other constants
interface TaskLock {
  locked: boolean;
  lockUntil: number;
  loopCount: number;
  lastReleaseTime?: number;
}

// Add with other constants
const taskLockMap = new Map<string, TaskLockInfo>();
const DEFAULT_LOCK_DURATION = 60000; // 60 seconds in milliseconds

async function runScheduledLoop() {

  const RATE_LIMIT = process.env.RATE_LIMIT
  const API_KEY = process.env.API_KEY

  if (!API_KEY) {
    console.log(RED + 'API_KEY is not set' + RESET);
    return
  }


  if (!RATE_LIMIT) {
    console.log(RED + 'RATE_LIMIT is not set' + RESET);
    return
  }

  while (activeTasks.size > 0) {
    try {
      for (const [taskId, task] of activeTasks.entries()) {
        if (!task.running) continue;

        const lockInfo = taskLockMap.get(taskId) || {
          lockedUntil: 0,
          loopCount: 0
        };

        const now = Date.now();

        // Check if task is locked
        if (now < lockInfo.lockedUntil) {
          const remainingTime = Math.ceil((lockInfo.lockedUntil - now) / 1000);
          continue;
        }

        // Check for active prioritized jobs
        if (await hasActivePrioritizedJobs(task)) {
          lockInfo.lockedUntil = now + DEFAULT_LOCK_DURATION;
          taskLockMap.set(taskId, lockInfo);
          continue;
        }

        // Update lock info
        lockInfo.loopCount++;
        lockInfo.lockedUntil = now + DEFAULT_LOCK_DURATION;
        lockInfo.lastUnlockTime = now + DEFAULT_LOCK_DURATION; // Store next unlock time
        taskLockMap.set(taskId, lockInfo);

        console.log(BLUE + `Starting loop #${lockInfo.loopCount} for ${task.contract.slug} (${taskId})` + RESET);
        console.log(BLUE + `Next unlock time: ${new Date(lockInfo.lastUnlockTime).toLocaleTimeString()}` + RESET);

        // Process the task
        await startTask(task, true);
      }



      // Small delay between iterations
      await new Promise(resolve => setTimeout(resolve, 1000));

    } catch (error) {
      console.error(RED + 'Error in runScheduledLoop:', error, RESET);
    }
  }
}


// Add cleanup function for stale locks
function cleanupTaskLocks() {
  const now = Date.now();

  for (const [taskId, lockInfo] of taskLockMap.entries()) {
    // Remove lock info for tasks that are no longer active or running
    const task = activeTasks.get(taskId);
    if (!task || !task.running) {
      taskLockMap.delete(taskId);
    }
  }
}

// Run cleanup every minute
setInterval(cleanupTaskLocks, 60000);

// Add these near the top with other interfaces
interface TaskLockInfo {
  lockedUntil: number;
  loopCount: number;
  lastUnlockTime?: number;
}

function getJobIds(marketplace: string, task: ITask) {
  const jobIds: string[] = []
  const selectedTraits = transformNewTask(task.selectedTraits)
  const isTraitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0

  const tokenBid = task.bidType === "token" && task.tokenIds.length > 0

  const traits = transformOpenseaTraits(selectedTraits);

  if (isTraitBid) {
    traits.map((trait) => {
      const uniqueKey = `${trait.type}-${trait.value}`
      jobIds.push(`${task._id}-${task.contract.slug}-${marketplace.toLowerCase()}-${uniqueKey}`)
    })
  } else if (tokenBid) {
    task.tokenIds.map((tokenId) => {
      jobIds.push(`${task._id}-${task.contract.slug}-${marketplace.toLowerCase()}-${tokenId}`)
    })
  } else {
    jobIds.push(`${task._id}-${task.contract.slug}-${marketplace.toLowerCase()}-collection`)
  }

  return jobIds
}

const DELAY_MS = 1000;
// const MAX_CONCURRENT_BATCHES = 1

function createJobKey(job: Job) {
  let uniqueKey;
  let type, value, trait;
  switch (job.name) {
    case 'OPENSEA_TRAIT_BID':
      trait = JSON.parse(job.data.trait)
      type = trait.type
      value = trait.value
      uniqueKey = `opensea-${type}-${value}`
      break;
    case 'BLUR_TRAIT_BID':
      const traitObj = JSON.parse(job.data.trait);
      const traitKey = Object.keys(traitObj)[0];
      const traitValue = traitObj[traitKey];
      uniqueKey = `blur-${traitKey}-${traitValue}`;
      break;
    case 'MAGICEDEN_TRAIT_BID':
      type = job.data.trait.attributeKey
      value = job.data.trait.attributeValue
      uniqueKey = `magiceden-${type}-${value}`
      break;
    case 'OPENSEA_TOKEN_BID':
      uniqueKey = `opensea-${job.data.asset.tokenId}`
      break;
    case 'MAGICEDEN_TOKEN_BID':
      uniqueKey = `magiceden-${job.data.tokenId}`;
      break;
    default:
      uniqueKey = 'collection';
  }
  const baseKey = `${job?.data?._id}-${job?.data?.contract?.slug || job?.data?.slug}` || Math.floor(Math.random() * Number.MAX_SAFE_INTEGER).toString()
  const key = `${baseKey}-${uniqueKey}`
  return key
}

async function processBulkJobs(jobs: any[], createKey = false) {
  if (!jobs?.length) return;

  const rateLimiter = new RateLimiter(MAX_PRIORITIZED_JOBS);

  const chunks = chunk(jobs, MAX_PRIORITIZED_JOBS);

  for (const [chunkIndex, currentChunk] of chunks.entries()) {
    try {
      const memUsage = process.memoryUsage();
      const memoryUsagePercent = (memUsage.heapUsed / os.totalmem()) * 100;

      if (memoryUsagePercent > MAX_MEMORY_USAGE) {
        console.log(YELLOW + `Memory usage high (${memoryUsagePercent.toFixed(1)}%). Waiting for GC...` + RESET);
        await new Promise(resolve => setTimeout(resolve, 5000));
        global.gc?.();
        continue;
      }

      const counts = await queue.getJobCounts();
      const prioritizedCount = counts.prioritized || 0;

      if (prioritizedCount > MAX_PRIORITIZED_JOBS) {
        await waitForQueueDrain();
        continue;
      }

      const validJobs = currentChunk.filter(job => {
        if (!job?.name || !job?.data) return false;

        const taskId = job?.data?._id;
        if (!taskId) return true;

        const task = activeTasks.get(taskId);
        if (!task?.running) {
          return false;
        }
        return true;
      });

      if (!validJobs.length) continue;

      await rateLimiter.acquire();
      await queue.addBulk(
        validJobs.map(job => ({
          name: job.name,
          data: job.data,
          opts: {
            ...(createKey && { jobId: createJobKey(job) }),
            ...job?.opts,
            removeOnComplete: true,
            removeOnFail: true,
            timeout: JOB_TIMEOUT,
            attempts: 3,
            backoff: {
              type: 'exponential',
              delay: 1000
            }
          }
        }))
      );
      await new Promise(resolve => setTimeout(resolve, 1000));
    } catch (error) {
      console.error(RED + `Error processing chunk ${chunkIndex + 1}:`, error, RESET);
    }
  }
}

class RateLimiter {
  private tokens: number;
  private lastRefill: number;
  private readonly rate: number;

  constructor(rate: number) {
    this.tokens = rate;
    this.lastRefill = Date.now();
    this.rate = rate;
  }

  async acquire(): Promise<void> {
    while (this.tokens <= 0) {
      const now = Date.now();
      const timePassed = now - this.lastRefill;
      const refillAmount = Math.floor(timePassed * (this.rate / 1000));

      if (refillAmount > 0) {
        this.tokens = Math.min(this.rate, this.tokens + refillAmount);
        this.lastRefill = now;
      }

      if (this.tokens <= 0) {
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }
    this.tokens--;
  }
}

// Helper function to wait for queue drain
async function waitForQueueDrain(): Promise<void> {
  while (true) {
    const counts = await queue.getJobCounts();
    if ((counts.prioritized || 0) <= DRAIN_THRESHOLD) {
      return;
    }
    await new Promise(resolve => setTimeout(resolve, 2000));
  }
}

// Helper function to chunk array
function chunk<T>(array: T[], size: number): T[][] {
  return Array.from({ length: Math.ceil(array.length / size) }, (_, i) =>
    array.slice(i * size, i * size + size)
  );
}

async function waitForQueueHealth(): Promise<void> {
  while (true) {
    const counts = await queue.getJobCounts();
    const totalInProgress = (counts.active || 0) + (counts.waiting || 0);

    if (totalInProgress <= QUEUE_LOW_WATERMARK) {
      console.log(GREEN + `Queue health restored (${totalInProgress} jobs in progress)` + RESET);
      return;
    }

    console.log(YELLOW + `Waiting for queue to drain... (${totalInProgress} jobs in progress)` + RESET);
    await new Promise(resolve => setTimeout(resolve, 5000));
  }
}


async function processImportedTasks(tasks: ITask[]) {
  try {
    console.log(BLUE + `\n===  Importing ${tasks.length} Tasks === ` + RESET);

    tasks.forEach(task => {
      console.log(`Collection: ${task.contract.slug} `);
      activeTasks.set(task._id, task);
      currentTasks.push(task);
    });


    const jobs = tasks.flatMap(task => [
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("opensea") ? [{ name: OPENSEA_SCHEDULE, data: task }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("blur") ? [{ name: BLUR_SCHEDULE, data: task }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("magiceden") ? [{ name: MAGICEDEN_SCHEDULE, data: task }] : []),
    ]);

    if (jobs.length > 0) {
      await processBulkJobs(jobs);
      console.log(`Successfully added ${jobs.length} jobs to the queue.`);
    }

    subscribeToCollections(tasks);

    console.log(GREEN + `=== Imported Tasks Processing Complete ===\n` + RESET);
  } catch (error) {
    console.error(RED + `Error processing imported tasks` + RESET, error);
  }
}


async function processNewTask(task: ITask) {
  try {
    console.log(BLUE + `\n === Processing New Task === ` + RESET);
    console.log(`Collection: ${task.contract.slug} `);
    console.log(`Task ID: ${task._id} `);

    // Add to activeTasks Map
    activeTasks.set(task._id, task);

    currentTasks.push(task);

    console.log(GREEN + `Added task to currentTasks(Total: ${currentTasks.length})` + RESET);
    const jobs = [
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("opensea") ? [{ name: OPENSEA_SCHEDULE, data: task }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("blur") ? [{ name: BLUR_SCHEDULE, data: task }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("magiceden") ? [{ name: MAGICEDEN_SCHEDULE, data: task }] : []),
    ];

    if (jobs.length > 0) {
      await processBulkJobs(jobs)
      console.log(`Successfully added ${jobs.length} jobs to the queue.`);
    }
    subscribeToCollections([task]);
    console.log(GREEN + `=== New Task Processing Complete ===\n` + RESET);
  } catch (error) {
    console.error(RED + `Error processing new task: ${task.contract.slug} ` + RESET, error);
  }
}

async function processUpdatedTask(task: ITask) {
  try {
    const existingTaskIndex = currentTasks.findIndex(t => t._id === task._id);
    if (existingTaskIndex !== -1) {
      currentTasks.splice(existingTaskIndex, 1, task);
      activeTasks.set(task._id, task);
      console.log(YELLOW + `Updated existing task: ${task.contract.slug} `.toUpperCase() + RESET);

      const jobs = await queue.getJobs(['prioritized', 'waiting', 'paused'])

      const filteredTasks = Object.fromEntries(
        Object.entries(task?.selectedTraits || {}).map(([category, traits]) => [
          category,
          traits.filter(trait => trait.availableInMarketplaces
            .map((item) => item.toLowerCase())
            .includes("blur"))
        ]).filter(([_, traits]) => traits.length > 0)
      );
      const selectedTraits = transformNewTask(filteredTasks)
      const expiry = getExpiry(task.bidDuration)

      if (task.selectedMarketplaces.map(m => m.toLowerCase()).includes("blur")) {
        const blurJobIds = getJobIds(BLUR.toLowerCase(), task)
        jobs.forEach(async (job) => {
          if (!job?.id) return
          if (blurJobIds.includes(job.id)) {
            const state = await job.getState();
            if (state !== 'active') {
              await job?.remove();
            }
          }
        })

        const traits = transformBlurTraits(selectedTraits)
        const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0
        let floor_price: number = 0

        if (task.bidPrice.minType == "eth" || task.openseaBidPrice.minType === "eth") {
          floor_price = 0
        } else {
          const stats = await fetchBlurCollectionStats(task.contract.slug);
          floor_price = stats.total.floor_price;
        }

        const { offerPriceEth, maxBidPriceEth } = calculateBidPrice(task, floor_price, "blur")
        const offerPrice = BigInt(Math.round(offerPriceEth * 1e18 / 1e16) * 1e16);

        if (traitBid) {
          await updateBlurTraitJobs(task, traits, offerPrice, expiry, maxBidPriceEth)
        }
      } else if (task.selectedMarketplaces.map(m => m.toLowerCase()).includes("opensea")) {
        const openseaJobIds = getJobIds(OPENSEA.toLowerCase(), task)
        jobs.forEach(async (job) => {
          if (!job?.id) return
          if (openseaJobIds.includes(job.id)) {
            const state = await job.getState();
            if (state !== 'active') {
              await job?.remove();
            }
          }
        })

        const traits = transformOpenseaTraits(selectedTraits)
        const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0

        const autoIds = task.tokenIds
          .filter(id => id.toString().toLowerCase().startsWith('bot'))
          .map(id => {
            const matches = id.toString().match(/\d+/);
            return matches ? parseInt(matches[0]) : null;
          })
          .filter(id => id !== null);

        const bottlomListing = await fetchOpenseaListings(task.contract.slug, autoIds[0]) ?? []
        const taskTokenIds = task.tokenIds
        const tokenIds = [...bottlomListing, ...taskTokenIds]
        const tokenBid = task.bidType === "token" && tokenIds.length > 0

        let floor_price: number = 0
        if (task.bidPrice.minType == "eth" || task.openseaBidPrice.minType === "eth") {
          floor_price = 0
        } else {
          const stats = await getCollectionStats(task.contract.slug);
          floor_price = stats?.total?.floor_price || 0;;
        }
        const { offerPriceEth, maxBidPriceEth } = calculateBidPrice(task, floor_price, "opensea")
        let offerPrice = BigInt(Math.ceil(offerPriceEth * 1e18));
        const collectionDetails = await getCollectionDetails(task.contract.slug);

        const creatorFees: IFee = collectionDetails.creator_fees.null !== undefined
          ? { null: collectionDetails.creator_fees.null }
          : Object.fromEntries(Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)]));

        if (traitBid) {
          await updateOpenseaTraitJobs(task, traits, offerPrice, expiry, maxBidPriceEth, creatorFees, collectionDetails)
        } else if (tokenBid) {
          await updateOpenseaTokenJobs(task, tokenIds, Number(offerPrice), expiry, maxBidPriceEth, creatorFees, collectionDetails)
        }
      } else if (task.selectedMarketplaces.map(m => m.toLowerCase()).includes("magiceden")) {
        const magicedenJobIds = getJobIds(MAGICEDEN.toLowerCase(), task)
        jobs.forEach(async (job) => {
          if (!job?.id) return
          if (magicedenJobIds.includes(job.id)) {
            const state = await job.getState();
            if (state !== 'active') {
              await job?.remove();
            }
          }
        })
        const traits = Object.entries(selectedTraits).flatMap(([key, values]) =>
          values.map(value => ({ attributeKey: key, attributeValue: value })))
        const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0
        let floor_price: number = 0

        if (task.bidPrice.minType == "eth" || task.openseaBidPrice.minType === "eth") {
          floor_price = 0
        } else {
          floor_price = Number(await fetchMagicEdenCollectionStats(task.contract.contractAddress))
        }
        const { offerPriceEth, maxBidPriceEth } = calculateBidPrice(task, floor_price as number, "magiceden")
        const offerPrice = Math.ceil(offerPriceEth * 1e18)
        const autoIds = task.tokenIds
          .filter(id => id.toString().toLowerCase().startsWith('bot'))
          .map(id => {
            const matches = id.toString().match(/\d+/);
            return matches ? parseInt(matches[0]) : null;
          })
          .filter(id => id !== null);

        const amount = autoIds[0]
        const bottlomListing = amount ? await fetchMagicEdenTokens(task.contract.contractAddress, amount) : []
        const taskTokenIds = task.tokenIds
        const tokenIds = bottlomListing ? [...bottlomListing, ...taskTokenIds] : [...taskTokenIds]
        const tokenBid = task.bidType === "token" && tokenIds.length > 0

        if (traitBid) {
          await updateMagicEdenTraitJobs(task, traits, offerPrice, expiry, maxBidPriceEth)
        } else if (tokenBid) {
          await updateMagicEdenTokenJobs(task, tokenIds, offerPrice, expiry, maxBidPriceEth)
        }
      }

    } else {
      console.log(RED + `Attempted to update non - existent task: ${task.contract.slug} ` + RESET);
    }
  } catch (error) {
    console.error(RED + `Error processing updated task: ${task.contract.slug} ` + RESET, error);
  }
}


async function updateMagicEdenTokenJobs(task: ITask, tokenIds: (number | string)[], offerPrice: number, expiry: number, maxBidPriceEth: number) {

  try {
    const jobs = tokenIds
      .filter(token => !isNaN(Number(token)))
      .map((token) => ({
        name: MAGICEDEN_TOKEN_BID,
        data: {
          _id: task._id,
          address: task.wallet.address,
          contractAddress: task.contract.contractAddress,
          quantity: 1,
          offerPrice,
          expiration: expiry,
          privateKey: task.wallet.privateKey,
          slug: task.contract.slug,
          tokenId: token,
          outbidOptions: task.outbidOptions,
          maxBidPriceEth
        },
        opts: { priority: TOKEN_BID_PRIORITY.MAGICEDEN }
      }));

    await processBulkJobs(jobs, true);
  } catch (error) {
    console.error(RED + `Error updating magiceden token jobs: ${task.contract.slug} ` + RESET, error);
  }

}

async function updateOpenseaTokenJobs(task: ITask, tokenIds: (number | string)[], offerPrice: number, expiry: number, maxBidPriceEth: number, creatorFees: IFee, collectionDetails: any) {

  try {
    const jobs = tokenIds
      .filter(token => !isNaN(Number(token)))
      .map((token) => ({
        name: OPENSEA_TOKEN_BID,
        data: {
          _id: task._id,
          address: task.wallet.address,
          privateKey: task.wallet.privateKey,
          slug: task.contract.slug,
          offerPrice: offerPrice.toString(),
          creatorFees,
          enforceCreatorFee: collectionDetails.enforceCreatorFee,
          asset: { contractAddress: task.contract.contractAddress, tokenId: token },
          expiry,
          outbidOptions: task.outbidOptions,
          maxBidPriceEth: maxBidPriceEth
        },
        opts: { priority: TOKEN_BID_PRIORITY.OPENSEA }
      }));

    await processBulkJobs(jobs, true);
  } catch (error) {
    console.error(RED + `Error updating opensea token jobs: ${task.contract.slug} ` + RESET, error);
  }

}

async function updateMagicEdenTraitJobs(task: ITask, traits: {
  attributeKey: string;
  attributeValue: string;
}[], offerPrice: number, expiration: number, maxBidPriceEth: number) {

  try {
    const traitJobs = traits.map((trait) => ({
      name: MAGICEDEN_TRAIT_BID,
      data: {
        _id: task._id,
        address: task.wallet.address,
        contractAddress: task.contract.contractAddress,
        quantity: 1,
        offerPrice,
        expiration,
        privateKey: task.wallet.privateKey,
        slug: task.contract.slug,
        trait,
        outbidOptions: task.outbidOptions,
        maxBidPriceEth
      },
      opts: { priority: TRAIT_BID_PRIORITY.MAGICEDEN }
    }));

    await processBulkJobs(traitJobs, true);

  } catch (error) {
    console.error(RED + `Error updating magiceden trait jobs: ${task.contract.slug} ` + RESET, error);
  }
}

async function updateOpenseaTraitJobs(task: ITask, traits: {
  [key: string]: string;
}[], offerPrice: bigint, expiry: number, maxBidPriceEth: number, creatorFees: IFee, collectionDetails: any) {

  try {
    const traitJobs = traits.map((trait) => ({
      name: OPENSEA_TRAIT_BID,
      data: {
        _id: task._id,
        address: task.wallet.address,
        privateKey: task.wallet.privateKey,
        slug: task.contract.slug,
        contractAddress: task.contract.contractAddress,
        offerPrice: offerPrice.toString(),
        creatorFees,
        enforceCreatorFee: collectionDetails.enforceCreatorFee,
        trait: JSON.stringify(trait),
        expiry,
        outbidOptions: task.outbidOptions,
        maxBidPriceEth: maxBidPriceEth
      },
      opts: { priority: TRAIT_BID_PRIORITY.OPENSEA }
    }));


    await processBulkJobs(traitJobs, true)

  } catch (error) {
    console.error(RED + `Error updating opensea trait jobs: ${task.contract.slug} ` + RESET, error);
  }
}

async function updateBlurTraitJobs(task: ITask, traits: {
  [key: string]: string;
}[], offerPrice: bigint, expiry: number, maxBidPriceEth: number) {
  try {
    const traitJobs = traits.map((trait) => ({
      name: BLUR_TRAIT_BID,
      data: {
        _id: task._id,
        address: task.wallet.address,
        privateKey: task.wallet.privateKey,
        contractAddress: task.contract.contractAddress,
        offerPrice: offerPrice.toString(),
        slug: task.contract.slug,
        trait: JSON.stringify(trait),
        expiry: expiry,
        outbidOptions: task.outbidOptions,
        maxBidPriceEth: maxBidPriceEth
      },
      opts: { priority: TRAIT_BID_PRIORITY.BLUR }
    }));
    await processBulkJobs(traitJobs, true)

  } catch (error) {
    console.error(RED + `Error updating blur trait jobs: ${task.contract.slug} ` + RESET, error);
  }
}

async function startTask(task: ITask, start: boolean) {
  const API_KEY = process.env.API_KEY
  if (!API_KEY) {
    console.log(RED + 'API_KEY is not set' + RESET);
    return
  }

  const RATE_LIMIT = process.env.RATE_LIMIT
  if (!RATE_LIMIT) {
    console.log(RED + 'RATE_LIMIT is not set' + RESET);
    return
  }

  const taskId = task._id.toString();
  try {
    await removeTaskFromAborted(taskId);
    const updatedTask = { ...task, running: true };
    activeTasks.set(taskId, updatedTask);
    const taskIndex = currentTasks.findIndex(t => t._id === task._id);
    if (taskIndex !== -1) {
      currentTasks[taskIndex].running = true;
    }
    console.log(GREEN + `Updated task ${task.contract.slug} running status to: ${start} `.toUpperCase() + RESET);
    if (task.outbidOptions.counterbid) {
      try {
        if (!ws || ws.readyState !== WebSocket.OPEN) {
          console.log(YELLOW + `WebSocket not ready, attempting to reconnect before subscribing to ${task.contract.slug}...` + RESET);
          // await connectWebSocket();
          return
        }

        console.log("subscribing to collection: ", task.contract.slug);
        const newTask = { ...task, running: true };
        await subscribeToCollections([newTask]);
      } catch (error) {
        console.error(RED + `Error subscribing to collection ${task.contract.slug}: ` + RESET, error);
      }
    }

    const jobs = [
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("opensea") ? [{ name: OPENSEA_SCHEDULE, data: { ...task, running: start } }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("blur") ? [{ name: BLUR_SCHEDULE, data: { ...task, running: start } }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("magiceden") ? [{ name: MAGICEDEN_SCHEDULE, data: { ...task, running: start } }] : []),
    ];

    await processBulkJobs(jobs);


  } catch (error) {
    console.error(RED + `Error starting task ${taskId}: ` + RESET, error);
  }
}


async function stopTask(task: ITask, start: boolean, marketplace?: string) {
  const taskId = task._id.toString();
  try {
    if (!marketplace || task.selectedMarketplaces.length === 0) {
      markTaskAsAborted(taskId);
      const updatedTask = { ...task, running: false };
      activeTasks.set(taskId, updatedTask);
    }
    if (!skipStats[taskId]) {
      skipStats[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }

    if (!errorStats[taskId]) {
      errorStats[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }

    if (marketplace) {
      skipStats[taskId][marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur'] = 0;
      errorStats[taskId][marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur'] = 0;
    } else {
      // Reset all stats when no specific marketplace is provided
      skipStats[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
      errorStats[taskId] = {
        opensea: 0,
        magiceden: 0,
        blur: 0
      };
    }

    await updateTaskStatus(task, start, marketplace);

    if (task.outbidOptions.counterbid && !marketplace) {
      try {
        await unsubscribeFromCollection(task);
      } catch (error) {
        console.error(RED + `Error unsubscribing from collection for task ${task.contract.slug}: ` + RESET, error);
      }
    }

    await cancelAllRelatedBids(task, marketplace)
    await removePendingAndWaitingBids(task, marketplace)
    await waitForRunningJobsToComplete(task, marketplace)
    await cancelAllRelatedBids(task, marketplace)

  } catch (error) {
    console.error(RED + `Error in stopTask for ${task.contract.slug}: ` + RESET, error);
    throw error;
  }
}


async function removePendingAndWaitingBids(task: ITask, marketplace?: string) {
  try {
    let jobIds: string[] = []
    switch (marketplace?.toLowerCase()) {
      case OPENSEA.toLowerCase():
        jobIds = getJobIds(OPENSEA.toLowerCase(), task)
        break;
      case MAGICEDEN.toLowerCase():
        jobIds = getJobIds(MAGICEDEN.toLowerCase(), task)
        break;
      case BLUR.toLowerCase():
        jobIds = getJobIds(BLUR.toLowerCase(), task)
        break;
      default:
        jobIds = [
          ...getJobIds(BLUR.toLowerCase(), task),
          ...getJobIds(MAGICEDEN.toLowerCase(), task),
          ...getJobIds(OPENSEA.toLowerCase(), task)
        ]
    }

    const BATCH_SIZE = 1000;
    const CONCURRENT_BATCHES = WORKER_COUNT;

    let totalJobsRemoved = 0;
    try {
      let start = 0;

      while (true) {
        const batchPromises = Array.from({ length: CONCURRENT_BATCHES }, async (_, i) => {
          const batchStart = start + (i * BATCH_SIZE);
          const batchEnd = batchStart + BATCH_SIZE;

          // Get jobs in batches using range
          const jobs: Job[] = await queue.getJobs(
            ['prioritized', 'waiting', 'paused'],
            batchStart,
            batchEnd
          );

          if (jobs.length === 0) return null;

          // Process jobs in parallel with rate limiting
          await Promise.all(jobs.map(async (job) => {
            if (!job?.id) return;
            if (jobIds.includes(job.id)) {
              try {
                const state = await job.getState();
                if (state === "prioritized" || state === "delayed" || state === "waiting") {
                  await job.remove();
                  totalJobsRemoved++;
                }
              } catch (error) {
                console.error(RED + `Error removing job ${job.id}: ` + RESET, error);
              }
            }
          }));

          return jobs.length;
        });

        const batchResults = await Promise.all(batchPromises);
        if (batchResults.every(result => result === null)) break;

        start += (BATCH_SIZE * CONCURRENT_BATCHES);

        // Add a small delay between batches to prevent overwhelming the queue
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    } catch (error) {
      console.error(RED + `Error removing jobs in batch: ${error} ` + RESET);
    }

    console.log('=== End Summary ====' + RESET);
    console.log(GREEN + 'Queue resumed after removing pending bids' + RESET);

  } catch (error) {
    console.error(RED + `Error removing pending bids: ${error} ` + RESET);
  }
}

async function waitForRunningJobsToComplete(task: ITask, marketplace?: string) {
  try {
    const checkInterval = 1000;
    let jobnames: string[] = [OPENSEA_SCHEDULE, OPENSEA_TRAIT_BID, OPENSEA_TOKEN_BID, MAGICEDEN_SCHEDULE, MAGICEDEN_TRAIT_BID, MAGICEDEN_TOKEN_BID, BLUR_SCHEDULE, BLUR_TRAIT_BID]

    switch (marketplace?.toLowerCase()) {
      case OPENSEA.toLowerCase():
        jobnames = [OPENSEA_SCHEDULE, OPENSEA_TRAIT_BID, OPENSEA_TOKEN_BID]
        break;

      case MAGICEDEN.toLowerCase():
        jobnames = [MAGICEDEN_SCHEDULE, MAGICEDEN_TRAIT_BID, MAGICEDEN_TOKEN_BID]
        break;

      case BLUR.toLowerCase():
        jobnames = [BLUR_SCHEDULE, BLUR_TRAIT_BID]
        break;
      default:
        jobnames = [OPENSEA_SCHEDULE, OPENSEA_TRAIT_BID, OPENSEA_TOKEN_BID, MAGICEDEN_SCHEDULE, MAGICEDEN_TRAIT_BID, MAGICEDEN_TOKEN_BID, BLUR_SCHEDULE, BLUR_TRAIT_BID]
    }

    console.log(YELLOW + 'Waiting for running jobs to complete...'.toUpperCase() + RESET);

    while (true) {
      await queue.pause();

      const activeJobs = await queue.getJobs(['active']);
      const relatedJobs = activeJobs?.filter(job => {
        const matchedId = job?.data?._id === task._id
        if (jobnames && jobnames?.length > 0) {
          return matchedId && jobnames?.includes(job?.name);
        }
        return matchedId;
      });

      if (relatedJobs?.length === 0) {
        await queue.resume()
        break;
      }
      await new Promise(resolve => setTimeout(resolve, checkInterval));
    }

  } catch (error: any) {
    console.error(RED + `Error waiting for running jobs to complete for task ${task.contract.slug}: ${error.message} ` + RESET);
  }
}

async function updateTaskStatus(task: ITask, running: boolean, marketplace?: string) {
  const taskIndex = currentTasks.findIndex(t => t._id === task._id);
  if (taskIndex !== -1) {
    if (marketplace) {
      return
    } else {
      currentTasks[taskIndex].running = running;
    }
  }
  console.log(running ? GREEN : RED + `${running ? 'Started' : 'Stopped'} processing task ${task.contract.slug} `.toUpperCase() + RESET);
}




const CANCEL_BATCH_SIZE = 1000; // Define batch size constant

async function cancelAllRelatedBids(task: ITask, marketplace?: string, attempt: number = 1): Promise<void> {
  try {
    const { openseaBids, magicedenBids, blurBids } = await getAllRelatedBids(task);

    const bidsToCancel = {
      opensea: (!marketplace || marketplace.toLowerCase() === OPENSEA.toLowerCase()) ? openseaBids : [],
      magiceden: (!marketplace || marketplace.toLowerCase() === MAGICEDEN.toLowerCase()) ? magicedenBids : [],
      blur: (!marketplace || marketplace.toLowerCase() === BLUR.toLowerCase()) ? blurBids : [],
    };

    for (const [marketplace, bids] of Object.entries(bidsToCancel)) {
      if (bids.length > 0) {
        try {
          for (let i = 0; i < bids.length; i += CANCEL_BATCH_SIZE) {
            const batchBids = bids.slice(i, i + CANCEL_BATCH_SIZE);

            switch (marketplace) {
              case "opensea":
                await cancelOpenseaBids(batchBids, task.wallet.privateKey, task._id);
                break;
              case "magiceden":
                await cancelMagicedenBids(batchBids, task.wallet.privateKey, task._id);
                break;
              case "blur":
                await cancelBlurBids(batchBids, task.wallet.privateKey, task._id);
                break;
            }
            await new Promise(resolve => setTimeout(resolve, 1000));
          }
        } catch (error) {
          console.error(RED + `Error cancelling bids for ${marketplace}: `, error, RESET);
        }
      }
    }

  } catch (error) {
    console.error(RED + `Unexpected error during bid cancellation(attempt ${attempt}): `, error, RESET);
  }
}


async function getAllRelatedBids(task: ITask) {
  try {
    const taskId = task._id

    const magicedenOrderTrackingKey = `{${taskId}}:magiceden:orders`;
    const magicedenOrderKeys = await redis.smembers(magicedenOrderTrackingKey);

    const openseaOrderTrackingKey = `{${taskId}}:opensea:orders`;
    const openseaOrderKeys = await redis.smembers(openseaOrderTrackingKey);

    const blurOrderTrackingKey = `{${taskId}}:blur:orders`;
    const blurOrderKeys = await redis.smembers(blurOrderTrackingKey);

    return { openseaBids: openseaOrderKeys, magicedenBids: magicedenOrderKeys, blurBids: blurOrderKeys };

  } catch (error) {
    console.log(RED + `Error getting all related bids for task ${task._id}: `, error, RESET);
    return { openseaBids: [], magicedenBids: [], blurBids: [] };
  }
}

async function cancelOpenseaBids(orderKeys: string[], privateKey: string, taskId: string) {
  if (!orderKeys.length) return
  const bidData = await redis.mget(orderKeys);
  const cancelData = bidData.map((order, index) => {
    if (!order) return
    const parsed = JSON.parse(order);
    return {
      name: CANCEL_OPENSEA_BID,
      data: { privateKey, orderId: parsed.orderId, taskId, orderKey: orderKeys[index] }
    }
  });
  await processBulkJobs(cancelData);
}

async function extractMagicedenOrderHash(orderKeys: string[]): Promise<string[]> {

  if (!orderKeys.length) return []
  const bidData = await redis.mget(orderKeys);
  const orderData = bidData.map((order, index) => {
    if (!order) return
    const parsed = JSON.parse(order);
    return parsed.payload
  })

  const extractedOrderIds = orderData
    .map(bid => {
      if (!bid) return null;
      try {
        const parsed = JSON.parse(bid);

        if (parsed.results) {
          return parsed.results[0].orderId;
        }
        if (parsed.message && parsed.orderId) {
          return parsed.orderId;
        }
        return null;
      } catch (e) {
        console.error('Error parsing bid data:', e);
        return null;
      }
    })
    .filter(id => id !== null);

  return extractedOrderIds as string[];
}

async function cancelMagicedenBids(orderKeys: string[], privateKey: string, taskId: string) {
  if (!orderKeys.length) return;
  for (let i = 0; i < orderKeys.length; i += 500) {
    const batchKeys = orderKeys.slice(i, i + 500);
    const extractedOrderIds = await extractMagicedenOrderHash(batchKeys)
    await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys: batchKeys, taskId });
  }
}

async function cancelBlurBids(orderKeys: string[], privateKey: string, taskId: string) {
  if (!orderKeys.length) return
  const orderData = await redis.mget(orderKeys);
  const cancelData = orderData.map((orderKey, index) => {
    if (!orderKey) return
    const order = JSON.parse(orderKey);
    const payload = order.payload
    return {
      name: CANCEL_BLUR_BID,
      data: { privateKey, payload, taskId, orderKey: orderKeys[index] }
    }
  })
  await processBulkJobs(cancelData);
}

async function updateStatus(task: ITask) {
  try {
    const { _id: taskId, running } = task;
    const start = !running;

    if (start) {
      // Update activeTasks Map
      const updatedTask = { ...task, running: true };
      activeTasks.set(taskId, updatedTask);
      await startTask(task, true);
    } else {
      // Update activeTasks Map
      const updatedTask = { ...task, running: false };
      activeTasks.set(taskId, updatedTask);
      await stopTask(task, false);
    }
  } catch (error) {
    console.error(RED + `Error updating status for task: ${task._id} ` + RESET, error);
  }
}

async function unsubscribeFromCollection(task: ITask) {
  try {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5 seconds
      if (!ws || ws.readyState !== WebSocket.OPEN) return;
    }

    const unsubscribeMessage = {
      "slug": task.contract.slug,
      "topic": task.contract.slug,
      "contractAddress": task.contract.contractAddress,
      "event": "leave_the_party",
      "clientId": task.user.toString(),
    };

    await new Promise<void>((resolve, reject) => {
      try {
        ws.send(JSON.stringify(unsubscribeMessage));
        console.log(`Unsubscribed from collection: ${task.contract.slug} `);
        resolve();
      } catch (err) {
        reject(err);
      }
    });
  } catch (error) {
    console.error(RED + `Error in unsubscribeFromCollection: ` + RESET, error);
    throw error;
  }
}

async function updateMarketplace(task: ITask) {
  try {
    const { _id: taskId, selectedMarketplaces: newMarketplaces } = task;
    const taskIndex = currentTasks?.findIndex(task => task?._id === taskId);

    if (taskIndex === -1) return;

    const currentMarketplaces = currentTasks[taskIndex].selectedMarketplaces;
    const currentSet = new Set(currentMarketplaces.map(m => m.toLowerCase()));
    const newSet = new Set(newMarketplaces.map(m => m.toLowerCase()));

    const outgoing = Array.from(currentSet).find(m => !newSet.has(m));
    const incoming = Array.from(newSet).find(m => !currentSet.has(m));

    if (outgoing) {
      console.log(RED + `Removing marketplace: ${outgoing.toUpperCase()} for collection: ${task.contract.slug} `.toUpperCase() + RESET);
    }

    currentTasks[taskIndex] = {
      ...currentTasks[taskIndex],
      selectedMarketplaces: [...newMarketplaces]
    };

    const updatedTask = {
      ...currentTasks[taskIndex],
      selectedMarketplaces: [...newMarketplaces]
    };
    currentTasks[taskIndex] = updatedTask;
    activeTasks.set(taskId, updatedTask);


    if (outgoing) {
      await handleOutgoingMarketplace(outgoing, task);
    }

    if (incoming) {
      const color = incoming.toLowerCase() === "magiceden" ? MAGENTA :
        incoming.toLowerCase() === "blur" ? GOLD : BLUE;

      console.log(color + `Adding marketplace: ${incoming.toUpperCase()} for collection: ${task.contract.slug} `.toUpperCase() + RESET);

      switch (incoming.toLowerCase()) {
        case "opensea":
          await queue.add(OPENSEA_SCHEDULE, task, { priority: COLLECTION_BID_PRIORITY.OPENSEA });
          break;
        case "magiceden":
          await queue.add(MAGICEDEN_SCHEDULE, task, { priority: COLLECTION_BID_PRIORITY.MAGICEDEN });
          break;
        case "blur":
          await queue.add(BLUR_SCHEDULE, task, { priority: COLLECTION_BID_PRIORITY.BLUR });
          break;
      }
    }

  } catch (error) {
    console.error(RED + `Error updating marketplace for task: ${task._id} ` + RESET, error);
  }
}

async function handleOutgoingMarketplace(marketplace: string, task: ITask) {
  try {
    await stopTask(task, false, marketplace);
  } catch (error) {
    console.error(RED + `Failed to handle outgoing marketplace ${marketplace} for task ${task.contract.slug}: ` + RESET, error);
  }
}

async function updateMultipleTasksStatus(data: { tasks: ITask[], running: boolean }) {
  try {
    const { tasks, running } = data;

    if (running) {
      await Promise.all(tasks.map(async (task) => {
        try {
          await startTask(task, true);
        } catch (error) {
          console.error(RED + `Error starting task ${task.contract.slug}: ` + RESET, error);
        }
      }));
    } else {
      await Promise.all(tasks.map(async (task) => {
        try {
          await stopTask(task, false);
        } catch (error) {
          console.error(RED + `Error stopping task ${task.contract.slug}: ` + RESET, error);
        }
      }));
    }
  } catch (error) {
    console.error(RED + 'Error updating multiple tasks status:' + RESET, error);
  }
}

let wsConnectionStatus: 'connected' | 'disconnected' | 'connecting' = 'disconnected';

async function connectWebSocket(): Promise<void> {
  wsConnectionStatus = 'connecting';
  ws = new WebSocket(MARKETPLACE_WS_URL);

  ws.addEventListener("open", async function open() {
    wsConnectionStatus = 'connected';
    clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify({ type: 'wsConnectionStatus', data: wsConnectionStatus }));
      }
    });
    console.log(GOLD + "CONNECTED TO MARKETPLACE EVENTS WEBSOCKET" + RESET);
    retryCount = 0;

    // Clear existing timeouts/intervals
    if (reconnectTimeoutId !== null) {
      clearTimeout(reconnectTimeoutId);
      reconnectTimeoutId = null;
    }

    if (heartbeatIntervalId !== null) {
      clearInterval(heartbeatIntervalId);
    }

    await subscribeToCollections(currentTasks as unknown as ITask[]);

    // Set up reconnection check
    heartbeatIntervalId = setInterval(() => {
      if (ws.readyState !== WebSocket.OPEN) {
        wsConnectionStatus = 'disconnected';

        clients.forEach(client => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({ type: 'wsConnectionStatus', data: wsConnectionStatus }));
          }
        });
        console.log(YELLOW + "WebSocket connection lost, attempting reconnect..." + RESET);
        ws.close();
        connectWebSocket();
      }
    }, 30000);

    ws.on("message", async function incoming(data: string) {
      try {
        const message = JSON.parse(data.toString())
        await handleCounterBid(message);
      } catch (error) {
      }
    });
  });

  ws.addEventListener("close", function close() {
    wsConnectionStatus = 'disconnected';

    clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify({ type: 'wsConnectionStatus', data: wsConnectionStatus }));
      }
    });

    console.log(RED + "DISCONNECTED FROM MARKETPLACE EVENTS WEBSCKET" + RESET);
    if (heartbeatIntervalId !== null) {
      clearInterval(heartbeatIntervalId);
      heartbeatIntervalId = null;
    }
    attemptReconnect();
  });

  ws.addEventListener("error", function error(err) {
    wsConnectionStatus = 'disconnected';
    clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify({ type: 'wsConnectionStatus', data: wsConnectionStatus }));
      }
    });
    attemptReconnect();
  });

}


function attemptReconnect(): void {
  if (retryCount < MAX_RETRIES) {
    if (reconnectTimeoutId !== null) {
      clearTimeout(reconnectTimeoutId);
    }
    let delay: number = Math.pow(2, retryCount) * 1000;
    console.log(`Attempting to reconnect in ${delay / 1000} seconds...`);
    reconnectTimeoutId = setTimeout(connectWebSocket, delay);
    retryCount++;
  } else {
    console.log("Max retries reached. Giving up on reconnecting.");
  }
}

async function handleCounterBid(message: any) {
  try {
    const { contractAddress, slug } = getMarketplaceDetails(message);

    if (!contractAddress && !slug) {
      console.log("Missing contractAddress and slug in message");
      return;
    }

    const relevantTasks = currentTasks.filter(task =>
      task?.outbidOptions?.counterbid &&
      task?.running &&
      (task?.contract?.contractAddress?.toLowerCase() === contractAddress?.toLowerCase() ||
        task?.contract?.slug?.toLowerCase() === slug?.toLowerCase())
    );

    if (!relevantTasks.length) return;

    await Promise.all(relevantTasks.map(task => handleCounterBidForTask(task, message)));
  } catch (error) {
    console.error("Error in handleCounterBid:", error);
  }

}

function getMarketplaceDetails(message: any): { contractAddress?: string, slug?: string } {
  switch (message.marketplace) {
    case BLUR:
      return { contractAddress: handleBlurMessages(message) };
    case OPENSEA:
      return handleOpenSeaMessages(message);
    case MAGICEDEN:
      return handleMagicEdenMessages(message);
    default:
      console.log(`Unknown marketplace: ${message.marketplace} `);
      return {};
  }
}



async function handleCounterBidForTask(task: any, message: any) {
  const selectedMarketplaces = task.selectedMarketplaces.map((m: string) => m.toLowerCase());

  if (selectedMarketplaces.includes('blur') && message.marketplace === BLUR) {
    await handleBlurCounterbid(message['1'], task);
  }

  if (selectedMarketplaces.includes('opensea') && message.marketplace === OPENSEA) {
    await handleOpenseaCounterbid(message, task);
  }

  if (selectedMarketplaces.includes('magiceden') && message.marketplace === MAGICEDEN) {
    await handleMagicEdenCounterbid(message, task)
  }
}


async function handleMagicEdenCounterbid(data: any, task: ITask) {
  try {
    const domain: string = data?.data?.source?.domain
    const wallets = await (await Wallet.find({}, { address: 1, _id: 0 }).exec()).map((wallet) => wallet.address.toLowerCase())
    const maker = data?.data?.maker?.toLowerCase()

    if (wallets.includes(maker)) return

    const expiry = getExpiry(task.bidDuration)
    const duration = expiry / 60 || 15;
    const currentTime = new Date().getTime();
    const expiration = Math.floor((currentTime + (duration * 60 * 1000)) / 1000);
    let floor_price: number = 0
    if (task.bidPrice.minType == "eth" || task.openseaBidPrice.minType === "eth") {
      floor_price = 0
    } else {
      floor_price = Number(await fetchMagicEdenCollectionStats(task.contract.contractAddress))
    }

    const { maxBidPriceEth } = calculateBidPrice(task, floor_price as number, "magiceden")
    const magicedenOutbidMargin = task.outbidOptions.magicedenOutbidMargin || 0.0001

    const bidType = data?.data?.criteria?.kind
    const tokenId = +data?.data?.criteria?.data?.token?.tokenId;

    const incomingBidAmount: number = Number(data?.data?.price?.amount?.raw);
    let offerPrice: number;

    const selectedTraits = transformNewTask(task.selectedTraits)

    if (bidType === "token") {
      const currentTask = activeTasks.get(task._id)
      if (!currentTask?.running || !currentTask.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes(MAGICEDEN.toLowerCase())) return

      const tokenIds = task.tokenIds.filter(id => !isNaN(Number(id)));

      const tokenBid = task.bidType === "token" && task.tokenIds.length > 0

      if (!tokenBid) return
      console.log(MAGENTA + '----------------------------------------------------------------------------------' + RESET);
      console.log(MAGENTA + `incoming bid for ${task.contract.slug}:${tokenId} for ${incomingBidAmount / 1e18} WETH on magiceden`.toUpperCase() + RESET);
      console.log(MAGENTA + '----------------------------------------------------------------------------------' + RESET);

      if (!tokenIds.includes(tokenId)) return

      const orderTrackingKey = `{${task._id}}:magiceden:orders`;
      const orderKeys = await getPatternKeys(orderTrackingKey, tokenId.toString()) || []

      const orderData = orderKeys.length ? await redis.mget(orderKeys) : []
      const offers = orderData.map((order: any) => {
        if (order.offer) return
        const parsed = JSON.parse(order)
        return parsed.offer
      })
      const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))
      if (incomingBidAmount < currentBidPrice) return

      offerPrice = Math.ceil((magicedenOutbidMargin * 1e18) + incomingBidAmount)
      if (maxBidPriceEth > 0 && Number(offerPrice / 1e18) > maxBidPriceEth) {
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + `magiceden counter offer ${offerPrice / 1e18} WETH for ${task.contract.slug} ${tokenId}  exceeds max bid price ${maxBidPriceEth} WETH ON MAGICEDEN.Skipping ...`.toUpperCase() + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);


        if (!skipStats[task._id]) {
          skipStats[task._id] = {
            opensea: 0,
            magiceden: 0,
            blur: 0
          };
        }
        skipStats[task._id]['magiceden']++;

        return;
      }

      const bidData: IMagicedenTokenBidData = {
        _id: task._id,
        address: task.wallet.address,
        contractAddress: task.contract.contractAddress,
        quantity: 1,
        offerPrice: Number(offerPrice),
        tokenId: tokenId,
        expiration: expiration.toString(),
        privateKey: task.wallet.privateKey,
        slug: task.contract.slug,
        outbidOptions: task.outbidOptions,
        maxBidPriceEth
      }

      if (orderKeys.length > 0) {
        const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
        await queue.add(CANCEL_MAGICEDEN_BID, {
          orderIds: extractedOrderIds,
          privateKey: task.wallet.privateKey,
          orderKeys,
          taskId: task._id
        });
      }
      const jobId = `counterbid-${task._id}-${task.contract.slug}-${MAGICEDEN.toLowerCase()}-${tokenId}`
      const job: Job = await queue.getJob(jobId)

      if (job) {
        if (incomingBidAmount <= Number(job.data.offerPrice)) {
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${incomingBidAmount / 1e18} WETH is less than or equal to existing bid ${Number(job.data.offerPrice) / 1e18} WETH Domain: ${domain} ` + RESET);
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          return
        } else {
          await job.updateData(bidData)
        }
      } else {
        await queue.add(
          MAGICEDEN_TOKEN_BID_COUNTERBID,
          bidData,
          {
            jobId,
          },

        );
      }

      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `Counterbidding incoming magiceden offer of ${Number(incomingBidAmount) / 1e18} WETH for ${task.contract.slug} ${tokenId} for ${Number(offerPrice) / 1e18} WETH ON MAGICEDEN`.toUpperCase() + RESET);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
    }
    if (bidType === "attribute") {

      const traitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0
      const trait = {
        attributeKey: data?.data?.criteria?.data?.attribute?.key,
        attributeValue: data?.data?.criteria?.data?.attribute?.value,
      }

      const hasTraits = checkMagicEdenTrait(selectedTraits, trait)
      if (!hasTraits || !traitBid) return

      console.log(MAGENTA + '----------------------------------------------------------------------------------' + RESET);
      console.log(MAGENTA + `incoming bid for ${task.contract.slug} ${JSON.stringify(trait)} for ${incomingBidAmount / 1e18} WETH on magiceden`.toUpperCase() + RESET);
      console.log(MAGENTA + '----------------------------------------------------------------------------------' + RESET);

      const orderTrackingKey = `{${task._id}}:magiceden:orders`;
      const orderKeys = await getPatternKeys(orderTrackingKey, `${trait.attributeKey}:${trait.attributeValue}`) || []
      const orderData = orderKeys.length ? await redis.mget(orderKeys) : []
      const offers = orderData.map((order: any) => {
        if (order.offer) return
        const parsed = JSON.parse(order)
        return parsed.offer
      })

      const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))

      if (incomingBidAmount < currentBidPrice) return

      offerPrice = Math.ceil((magicedenOutbidMargin * 1e18) + incomingBidAmount)
      if (maxBidPriceEth > 0 && Number(offerPrice / 1e18) > maxBidPriceEth) {
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + `counter Offer ${offerPrice / 1e18} WETH for ${task.contract.slug} ${JSON.stringify(trait)} exceeds max bid price ${maxBidPriceEth} WETH ON MAGICEDEN.Skipping ...`.toUpperCase() + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);

        if (!skipStats[task._id]) {
          skipStats[task._id] = {
            opensea: 0,
            magiceden: 0,
            blur: 0
          };
        }
        skipStats[task._id]['magiceden']++;
        return;
      }

      const bidData: IMagicedenTraitBidData = {
        taskId: task._id,
        bidCount: getIncrementedBidCount(MAGICEDEN, task.contract.slug, task._id),
        address: task.wallet.address,
        contractAddress: task.contract.contractAddress,
        quantity: 1,
        offerPrice: offerPrice,
        expiration: expiration.toString(),
        privateKey: task.wallet.privateKey,
        slug: task.contract.slug,
        trait: JSON.stringify(trait)
      }

      if (orderKeys.length > 0) {
        const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
        await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: task.wallet.privateKey, orderKeys: orderKeys, taskId: task._id });
      }


      const jobId = `counterbid-${task._id}-${task.contract.slug}-${MAGICEDEN.toLowerCase()}-${JSON.stringify(trait)}`
      const job: Job = await queue.getJob(jobId)

      if (job) {
        if (incomingBidAmount <= job.data.offerPrice) {
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${incomingBidAmount / 1e18} WETH is less than or equal to existing bid ${Number(job.data.offerPrice) / 1e18} WETH Domain: ${domain} ` + RESET);
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          return
        } else {
          await job.updateData(data)
        }
      } else {
        await queue.add(
          MAGICEDEN_TRAIT_BID_COUNTERBID,
          bidData,
          {
            jobId,
          },
        );
      }

      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `Counterbidding incoming offer of ${Number(incomingBidAmount) / 1e18} WETH for ${task.contract.slug} trait ${JSON.stringify(trait)} for ${Number(offerPrice) / 1e18} WETH ON MAGICEDEN `.toUpperCase() + RESET);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);


    }
    if (bidType === "collection") {
      if (maker === task.wallet.address.toLowerCase()) return

      const isTraitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0
      const tokenBid = task.bidType === "token" && task.tokenIds.length > 0
      const collectionBid = !isTraitBid && !tokenBid

      if (!collectionBid) return

      console.log(MAGENTA + '----------------------------------------------------------------------------------' + RESET);
      console.log(MAGENTA + `incoming collection offer for ${task.contract.slug} for ${incomingBidAmount / 1e18} WETH on magiceden`.toUpperCase() + RESET);
      console.log(MAGENTA + '----------------------------------------------------------------------------------' + RESET);


      const orderTrackingKey = `{${task._id}}:magiceden:orders`;
      const orderKeys = await getPatternKeys(orderTrackingKey, 'collection') || []
      const orderData = orderKeys.length ? await redis.mget(orderKeys) : []
      const offers = orderData.map((order: any) => {
        if (order.offer) return
        const parsed = JSON.parse(order)
        return parsed.offer
      })
      const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))

      if (incomingBidAmount < currentBidPrice) return

      offerPrice = Math.ceil((magicedenOutbidMargin * 1e18) + incomingBidAmount)

      if (maxBidPriceEth > 0 && Number(offerPrice) / 1e18 > maxBidPriceEth) {
        if (!skipStats[task._id]) {
          skipStats[task._id] = {
            opensea: 0,
            magiceden: 0,
            blur: 0
          };
        }
        skipStats[task._id]['magiceden']++;
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + `counter Offer ${offerPrice / 1e18} WETH for ${task.contract.slug}  exceeds max bid price ${maxBidPriceEth} WETH.Skipping ...`.toUpperCase() + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        return;
      }

      const bidCount = getIncrementedBidCount(MAGICEDEN, task.contract.slug, task._id)

      const bidData: IMagicedenCollectionBidData = {
        _id: task._id,
        bidCount,
        address: task.wallet.address,
        contractAddress: task.contract.contractAddress,
        quantity: 1,
        offerPrice: offerPrice,
        expiration: expiration.toString(),
        privateKey: task.wallet.privateKey,
        slug: task.contract.slug,
      }


      if (orderKeys.length > 0) {
        const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
        await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: task.wallet.privateKey, orderKeys: orderKeys, taskId: task._id });
      }

      const jobId = `counterbid-${task._id}-${task.contract.slug}-${MAGICEDEN.toLowerCase()}-collection`
      const job: Job = await queue.getJob(jobId)

      if (job) {
        if (incomingBidAmount <= job.data.offerPrice) {
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${incomingBidAmount / 1e18} WETH is less than or equal to existing bid ${Number(job.data.offerPrice) / 1e18} WETH Domain: ${domain} ` + RESET);
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          return
        } else {
          await job.updateData(bidData)
        }
      } else {
        await queue.add(
          MAGICEDEN_COLLECTION_BID_COUNTERBID,
          bidData,
          {
            jobId,
          },

        );
      }

      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `Counterbidding incoming magiceden offer of ${Number(incomingBidAmount) / 1e18} WETH for ${task.contract.slug} for ${Number(offerPrice) / 1e18} WETH`.toUpperCase() + RESET);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
    }
  } catch (error) {
    console.error(RED + `Error handling MAGICEDEN counterbid: ${JSON.stringify(error)} ` + RESET);
  }
}

async function handleOpenseaCounterbid(data: any, task: ITask) {
  try {
    const maker = data?.payload?.payload?.maker?.address.toLowerCase()
    const incomingBidAmount: number = Number(data?.payload?.payload?.base_price);
    const wallets = await (await Wallet.find({}, { address: 1, _id: 0 }).exec()).map((wallet) => wallet.address.toLowerCase())

    if (wallets.includes(maker)) return

    const expiry = getExpiry(task.bidDuration)
    let floor_price: number = 0

    if (task.bidPrice.minType == "eth" || task.openseaBidPrice.minType === "eth") {
      floor_price = 0
    } else {
      const stats = await getCollectionStats(task.contract.slug);
      floor_price = stats.total.floor_price;
    }

    const { maxBidPriceEth } = calculateBidPrice(task, Number(floor_price), "opensea")
    const openseaOutbidMargin = task.outbidOptions.openseaOutbidMargin || 0.0001

    const collectionDetails = await getCollectionDetails(task.contract.slug);
    const creatorFees: IFee = collectionDetails.creator_fees.null !== undefined
      ? { null: collectionDetails.creator_fees.null }
      : Object.fromEntries(Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)]));

    let offerPrice: number;
    let colletionOffer: bigint;

    const selectedTraits = transformNewTask(task.selectedTraits)

    if (data.event === "item_received_bid") {
      const currentTask = activeTasks.get(task._id)

      if (!currentTask?.running || !currentTask.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes(OPENSEA.toLowerCase())) return
      const tokenId = +data.payload.payload.protocol_data.parameters.consideration.find((item: any) => item.token.toLowerCase() === task.contract.contractAddress.toLowerCase()).identifierOrCriteria
      const tokenIds = task.tokenIds.filter(id => !isNaN(Number(id)));
      const tokenBid = task.bidType === "token" && task.tokenIds.length > 0

      if (!tokenBid) return

      if (!tokenIds.includes(tokenId)) return

      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);
      console.log(BLUE + `incoming offer for ${task.contract.slug}:${tokenId} for ${incomingBidAmount / 1e18} WETH on opensea`.toUpperCase() + RESET);
      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);

      const orderTrackingKey = `{${task._id}}:opensea:orders`;
      const orderKeys = await getPatternKeys(orderTrackingKey, tokenId.toString()) || []
      const orderData = orderKeys.length ? await redis.mget(orderKeys) : []

      const offers = orderData.map((order: any) => {
        if (order.offer) return
        const parsed = JSON.parse(order)
        return parsed.offer
      })

      const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))
      if (incomingBidAmount < currentBidPrice) return

      offerPrice = Math.ceil((openseaOutbidMargin * 1e18) + incomingBidAmount)

      if (maxBidPriceEth > 0 && Number(offerPrice / 1e18) > maxBidPriceEth) {
        if (!skipStats[task._id]) {
          skipStats[task._id] = {
            opensea: 0,
            magiceden: 0,
            blur: 0
          };
        }
        skipStats[task._id]['opensea']++;
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + `counter offer ${offerPrice / 1e18} WETH for ${task.contract.slug}:${tokenId}  exceeds max bid price ${maxBidPriceEth} WETH ON OPENSEA.Skipping ...`.toUpperCase() + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        return;
      }
      colletionOffer = BigInt(offerPrice)

      const asset = {
        contractAddress: task.contract.contractAddress,
        tokenId: tokenId
      }

      const bidData: IProcessOpenseaTokenBidData = {
        _id: task._id,
        address: task.wallet.address,
        privateKey: task.wallet.privateKey,
        slug: task.contract.slug,
        offerPrice: Number(colletionOffer),
        creatorFees,
        enforceCreatorFee: collectionDetails.enforceCreatorFee,
        asset,
        expiry,
        outbidOptions: task.outbidOptions,
        maxBidPriceEth
      }

      if (orderKeys.length > 0) {
        const bidData = await redis.mget(orderKeys);
        const cancelData = bidData.map((order, index) => {
          if (!order) return
          const parsed = JSON.parse(order);
          return {
            name: CANCEL_OPENSEA_BID,
            data: { privateKey: task.wallet.privateKey, orderId: parsed.orderId, taskId: task._id, orderKey: orderKeys[index] }
          }
        });
        await processBulkJobs(cancelData);
      }

      const jobId = `counterbid-${task._id}-${task.contract.slug}-${OPENSEA.toLowerCase()}-${asset.tokenId}`
      const job: Job = await queue.getJob(jobId)
      if (job) {
        if (incomingBidAmount <= job.data.offerPrice) {
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${incomingBidAmount / 1e18} WETH is less than or equal to existing bid ${Number(job.data.offerPrice) / 1e18} WETH` + RESET);
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          return
        } else {
          await job.updateData(bidData)
        }
      } else {
        await queue.add(
          OPENSEA_TOKEN_BID_COUNTERBID,
          bidData,
          {
            jobId,
          },
        );
      }

      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `Counterbidding incoming opensea offer of ${Number(incomingBidAmount) / 1e18} WETH for ${task.contract.slug}:${asset.tokenId} for ${Number(colletionOffer) / 1e18} WETH ON OPENSEA`.toUpperCase() + RESET);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
    }
    if (data.event === "trait_offer") {
      const traitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0
      const hasTraits = checkOpenseaTrait(selectedTraits, data.payload.payload.trait_criteria)

      if (!hasTraits || !traitBid) return
      const trait = {
        type: data.payload.payload.trait_criteria.trait_type,
        value: data.payload.payload.trait_criteria.trait_name
      }

      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);
      console.log(BLUE + `incoming offer for ${task.contract.slug}:${JSON.stringify(trait)} for ${incomingBidAmount / 1e18} WETH on opensea`.toUpperCase() + RESET);
      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);

      const orderTrackingKey = `{${task._id}}:opensea:orders`;
      const orderKeys = await getPatternKeys(orderTrackingKey, `${trait.type}:${trait.value}`) || []
      const orderData = orderKeys.length ? await redis.mget(orderKeys) : []

      const offers = orderData.map((order: any) => {
        if (order.offer) return
        const parsed = JSON.parse(order)
        return parsed.offer
      })
      const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))
      if (incomingBidAmount < currentBidPrice) return

      offerPrice = Math.ceil((openseaOutbidMargin * 1e18) + incomingBidAmount)

      if (maxBidPriceEth > 0 && Number(offerPrice / 1e18) > maxBidPriceEth) {
        if (!skipStats[task._id]) {
          skipStats[task._id] = {
            opensea: 0,
            magiceden: 0,
            blur: 0
          };
        }
        skipStats[task._id]['opensea']++;
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + `counter Offer ${offerPrice / 1e18} WETH for ${task.contract.slug} ${JSON.stringify(trait)}  exceeds max bid price ${maxBidPriceEth} WETH ON OPENSEA.Skipping ...`.toUpperCase() + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        return;
      }
      colletionOffer = BigInt(offerPrice)

      const jobId = `counterbid-${task._id}-${task.contract.slug}-${OPENSEA.toLowerCase()}-${trait}`
      const job: Job = await queue.getJob(jobId)
      const bidCount = getIncrementedBidCount(OPENSEA, task.contract.slug, task._id)

      const counterbidData: IProcessOpenseaTraitBidData = {
        _id: task._id,
        bidCount,
        address: task.wallet.address,
        privateKey: task.wallet.privateKey,
        slug: task.contract.slug,
        offerPrice: Number(colletionOffer),
        creatorFees,
        enforceCreatorFee: collectionDetails.enforceCreatorFee,
        expiry,
        trait: JSON.stringify(trait)
      }

      if (orderKeys.length > 0) {
        const bidData = await redis.mget(orderKeys);
        const cancelData = bidData.map((order, index) => {
          if (!order) return
          const parsed = JSON.parse(order);
          return {
            name: CANCEL_OPENSEA_BID,
            data: { privateKey: task.wallet.privateKey, orderId: parsed.orderId, taskId: task._id, orderKey: orderKeys[index] }
          }
        });
        await processBulkJobs(cancelData);
      }

      if (job) {
        if (incomingBidAmount <= job.data.offerPrice) {
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${incomingBidAmount / 1e18} WETH is less than or equal to existing bid ${Number(job.data.offerPrice) / 1e18} WETH` + RESET);
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          return
        } else {
          await job.updateData(data)
        }
      } else {
        await queue.add(
          OPENSEA_TRAIT_BID_COUNTERBID,
          counterbidData,
          {
            jobId,
          },
        );
      }
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `Counterbidding incoming opensea offer of ${Number(incomingBidAmount) / 1e18} WETH for ${task.contract.slug} ${JSON.stringify(trait)} for ${Number(colletionOffer) / 1e18} WETH ON OPENSEA`.toUpperCase() + RESET);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
    }

    if (data.event === "collection_offer") {
      const isTraitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0
      const tokenBid = task.bidType === "token" && task.tokenIds.length > 0
      const collectionBid = !isTraitBid && !tokenBid

      if (!collectionBid) return

      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);
      console.log(BLUE + `incoming collection offer for ${task.contract.slug} for ${incomingBidAmount / 1e18} WETH on opensea`.toUpperCase() + RESET);
      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);

      const orderTrackingKey = `{${task._id}}:opensea:orders`;
      const orderKeys = await getPatternKeys(orderTrackingKey, 'collection') || []
      const orderData = orderKeys.length ? await redis.mget(orderKeys) : []
      const offers = orderData.map((order: any) => {
        if (order.offer) return
        const parsed = JSON.parse(order)
        return parsed.offer
      })
      const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))

      if (incomingBidAmount < currentBidPrice) return
      offerPrice = Math.ceil((openseaOutbidMargin * 1e18) + incomingBidAmount)

      if (maxBidPriceEth > 0 && Number(offerPrice / 1e18) > maxBidPriceEth) {
        if (!skipStats[task._id]) {
          skipStats[task._id] = {
            opensea: 0,
            magiceden: 0,
            blur: 0
          };
        }
        skipStats[task._id]['opensea']++;
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + `counter Offer price ${offerPrice / 1e18} WETH for ${task.contract.slug} exceeds max bid price ${maxBidPriceEth} WETH ON OPENSEA.Skipping ...`.toUpperCase() + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        return;
      }
      colletionOffer = BigInt(offerPrice)
      const bidCount = getIncrementedBidCount(OPENSEA, task.contract.slug, task._id)

      const bidData: IOpenseaBidParams = {
        taskId: task._id,
        bidCount,
        walletAddress: task.wallet.address,
        walletPrivateKey: task.wallet.privateKey,
        slug: task.contract.slug,
        offerPrice: Number(colletionOffer),
        creatorFees,
        enforceCreatorFee: collectionDetails.enforceCreatorFee,
        expiry
      }

      if (orderKeys.length > 0) {
        const bidData = await redis.mget(orderKeys);
        const cancelData = bidData.map((order, index) => {
          if (!order) return
          const parsed = JSON.parse(order);
          return {
            name: CANCEL_OPENSEA_BID,
            data: { privateKey: task.wallet.privateKey, orderId: parsed.orderId, taskId: task._id, orderKey: orderKeys[index] }
          }
        });
        await processBulkJobs(cancelData);
      }

      const jobId = `counterbid-${task._id}-${task.contract.slug}-${OPENSEA.toLowerCase()}-collection`
      const job: Job = await queue.getJob(jobId)

      if (job) {
        if (incomingBidAmount <= job.data.offerPrice) {
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${incomingBidAmount / 1e18} WETH is less than or equal to existing bid ${Number(job.data.offerPrice) / 1e18} WETH` + RESET);
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          return
        } else {
          await job.updateData(bidData)
        }
      } else {
        await queue.add(
          OPENSEA_COLLECTION_BID_COUNTERBID,
          bidData,
          {
            jobId,
          },

        );
      }
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      console.log(GREEN + `Counterbidding incoming offer of ${Number(incomingBidAmount) / 1e18} WETH for ${task.contract.slug} for ${Number(colletionOffer) / 1e18} WETH ON OPENSEA`.toUpperCase() + RESET);
      console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
    }
  } catch (error) {
  }
}

async function handleBlurCounterbid(data: any, task: ITask) {
  const incomingBid: CombinedBid = data
  try {
    const expiry = getExpiry(task.bidDuration)
    let floor_price: number = 0

    if (task.bidPrice.minType == "eth" || task.openseaBidPrice.minType === "eth") {
      floor_price = 0
    } else {
      const stats = await fetchBlurCollectionStats(task.contract.slug);
      floor_price = stats.total.floor_price;
    }

    const { maxBidPriceEth } = calculateBidPrice(task, Number(floor_price), "blur")
    const selectedTraits = transformNewTask(task.selectedTraits)

    const traitBid = task.bidType === "collection" && selectedTraits && Object.keys(selectedTraits).length > 0

    const blurOutbidMargin = task.outbidOptions.blurOutbidMargin || 0.01

    if (traitBid) {
      const traits = transformBlurTraits(selectedTraits)
      const incomingTraitBids = incomingBid?.stats?.filter(item => item.criteriaType.toLowerCase() === "trait") || incomingBid?.updates?.filter(item => item.criteriaType.toLowerCase() === "trait")
      const hasMatchingTraits = checkBlurTraits(incomingTraitBids, traits);

      if (!hasMatchingTraits.length) return
      for (const traitBid of hasMatchingTraits) {
        const trait = JSON.stringify(traitBid.criteriaValue)
        const incomingPrice = Number(traitBid.bestPrice)
        console.log(GOLD + '---------------------------------------------------------------------------------' + RESET);
        console.log(GOLD + `incoming trait offer for ${task.contract.slug}: ${trait} for ${incomingPrice} BETH on blur`.toUpperCase() + RESET);
        console.log(GOLD + '---------------------------------------------------------------------------------' + RESET);

        const traitsObj = JSON.parse(trait);
        const [traitType, traitValue] = Object.entries(traitsObj)[0];
        const identifier = `${traitType}:${traitValue}`
        const orderTrackingKey = `{${task._id}}:blur:orders`;
        const orderKeys = await getPatternKeys(orderTrackingKey, identifier) || []
        const orderData = orderKeys.length ? await redis.mget(orderKeys) : []
        const offers = orderData.map((order: any) => {
          if (order.offer) return
          const parsed = JSON.parse(order)
          return parsed.offer
        })
        const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))
        if (incomingPrice <= currentBidPrice) return

        let offerPrice: any = Number((blurOutbidMargin + Number(incomingPrice)).toFixed(2))
        offerPrice = BigNumber.from(utils.parseEther(offerPrice.toString()).toString())

        if (Number(offerPrice) / 1e18 > maxBidPriceEth) {
          if (!skipStats[task._id]) {
            skipStats[task._id] = {
              opensea: 0,
              magiceden: 0,
              blur: 0
            };
          }
          skipStats[task._id]['blur']++;
          console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `counter Offer price ${Number(offerPrice) / 1e18} BETH exceeds max bid price ${maxBidPriceEth} BETH ON BLUR.Skipping ...`.toUpperCase() + RESET);
          console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
          return;
        }
        const jobId = `counterbid-${task._id}-${task.contract.slug}-${BLUR.toLowerCase()}-${trait}`
        const job: Job = await queue.getJob(jobId)
        const bidCount = getIncrementedBidCount(BLUR, task.contract.slug, task._id)
        const bidData: IBlurBidData = {
          taskId: task._id,
          bidCount: bidCount,
          address: task.wallet.address,
          privateKey: task.wallet.privateKey,
          contractAddress: task.contract.contractAddress,
          offerPrice: Number(offerPrice),
          slug: task.contract.slug,
          expiry
        }

        if (orderKeys.length > 0) {
          const orderData = await redis.mget(orderKeys);
          const cancelData = orderData.map((orderKey, index) => {
            if (!orderKey) return
            const order = JSON.parse(orderKey);
            const payload = order.payload
            return {
              name: CANCEL_BLUR_BID,
              data: { privateKey: task.wallet.privateKey, payload, taskId: task._id, orderKey: orderKeys[index] }
            }
          })
          await processBulkJobs(cancelData)
        }
        if (job) {
          if (incomingPrice <= job.data.offerPrice) {
            console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
            console.log(RED + `Skipping counterbid since incoming bid ${incomingPrice} WETH is less than or equal to existing bid ${Number(job.data.offerPrice)} WETH` + RESET);
            console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
            return
          } else {
            await job.updateData(data)
          }
        } else {
          await queue.add(
            BLUR_TRAIT_BID_COUNTERBID,
            bidData,
            {
              jobId,
            },
          );
        }
        console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
        console.log(GREEN + `Counterbidding incoming blur offer of ${Number(incomingPrice)} BETH for ${task.contract.slug} ${trait} for ${Number(offerPrice) / 1e18} BETH ON BLUR`.toUpperCase() + RESET);
        console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
      }

    } else {
      const collectionBid = incomingBid?.stats?.filter(item => item.criteriaType.toLowerCase() === "collection") || incomingBid?.updates?.filter(item => item.criteriaType.toLowerCase() === "collection")

      if (!collectionBid.length) return
      const bestPrice = collectionBid?.sort((a, b) => +b.bestPrice - +a.bestPrice)[0].bestPrice || "0"
      const incomingPrice = Number(bestPrice) * 1e18

      const redisKeyPattern = `{${task._id}:*: blur:${task.contract.slug} }: collection`;
      const redisKeys = await redis.keys(redisKeyPattern)
      const currentOffers = await Promise.all(redisKeys.map((key) => redis.get(key)))
      const currentBidPrice = !redisKeys.length ? 0 : Math.max(...currentOffers.map((offer) => Number(offer)))
      if (incomingPrice <= currentBidPrice) return

      const rawPrice = (blurOutbidMargin * 1e18) + Number(incomingPrice)
      const offerPrice = BigInt(Math.round(rawPrice / 1e16) * 1e16)

      if (maxBidPriceEth > 0 && Number(offerPrice) / 1e18 > maxBidPriceEth) {
        if (!skipStats[task._id]) {
          skipStats[task._id] = {
            opensea: 0,
            magiceden: 0,
            blur: 0
          };
        }
        skipStats[task._id]['blur']++;
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + `counter Offer price ${Number(offerPrice) / 1e18} BETH exceeds max bid price ${maxBidPriceEth} BETH ON BLUR.Skipping ...`.toUpperCase() + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        return;
      }



      const jobId = `counterbid-${task._id}-${task.contract.slug}-${BLUR.toLowerCase()}-collection`
      const job: Job = await queue.getJob(jobId)
      const bidCount = getIncrementedBidCount(BLUR, task.contract.slug, task._id)
      const bidData: IBlurBidData = {
        taskId: task._id,
        bidCount,
        address: task.wallet.address,
        privateKey: task.wallet.privateKey,
        contractAddress: task.contract.contractAddress,
        offerPrice: Number(offerPrice),
        slug: task.contract.slug,
        expiry
      }

      if (job) {
        if (incomingPrice <= job.data.offerPrice) {
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${incomingPrice / 1e18} WETH is less than or equal to existing bid ${Number(job.data.offerPrice) / 1e18} WETH` + RESET);
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          return
        } else {
          await job.updateData(bidData)
        }
      } else {
        await queue.add(
          BLUR_COLLECTION_BID_COUNTERBID,
          bidData,
          {
            jobId,
          },

        );
      }
    }

  } catch (error) {
    console.error(RED + `Error handling Blur counterbid: ${JSON.stringify(error)} ` + RESET);
  }
}

function handleMagicEdenMessages(message: any) {
  let slug, contractAddress;
  try {
    if (
      typeof message === "object" && message.event === "bid.created"
    ) {
      slug = message.payload?.payload?.collection?.slug;
      contractAddress = message.tags?.contract || message.payload?.payload?.asset_contract_criteria?.address;
    }
  } catch (error) {
    console.error("Error parsing MagicEden message:", error);
  }
  return { contractAddress, slug }
}

function handleOpenSeaMessages(message: any) {
  let contractAddress, slug

  try {
    if (
      typeof message === "object" &&
      (message.event === "item_received_bid" ||
        message.event === "trait_offer" ||
        message.event === "collection_offer"
      )
    ) {
      slug = message.payload?.payload?.collection?.slug;
      contractAddress = message.payload?.payload?.asset_contract_criteria?.address;
    }
  } catch (err) {
    console.error("Error parsing OpenSea message:", err);
  }
  return { contractAddress, slug }
}

function handleBlurMessages(message: any) {
  let contractAddress: string | undefined;
  try {
    contractAddress = message['1'].contractAddress;
  } catch (error) {
    console.error(`Failed to parse Blur message: `, error);
  }
  return contractAddress
}

// At the top of the file, add this global variable

async function processOpenseaScheduledBid(task: ITask) {
  try {
    if (!task.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("opensea")) return

    const filteredTasks = Object.fromEntries(
      Object.entries(task?.selectedTraits || {}).map(([category, traits]) => [
        category,

        traits.filter(trait => trait.availableInMarketplaces.includes("opensea"))
      ]).filter(([_, traits]) => traits.length > 0)
    );

    const selectedTraits = transformNewTask(filteredTasks)
    const expiry = getExpiry(task.bidDuration)
    const WALLET_ADDRESS: string = task.wallet.address;
    const WALLET_PRIVATE_KEY: string = task.wallet.privateKey;
    const collectionDetails = await getCollectionDetails(task.contract.slug);

    const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0
    let floor_price: number = 0
    if (task.bidPrice.minType == "eth" || task.openseaBidPrice.minType === "eth") {
      floor_price = 0
    } else {
      const stats = await getCollectionStats(task.contract.slug);
      floor_price = stats?.total?.floor_price || 0;;
    }

    const autoIds = task.tokenIds
      .filter(id => id.toString().toLowerCase().startsWith('bot'))
      .map(id => {
        const matches = id.toString().match(/\d+/);
        return matches ? parseInt(matches[0]) : null;
      })
      .filter(id => id !== null);

    const bottlomListing = await fetchOpenseaListings(task.contract.slug, autoIds[0]) ?? []
    const taskTokenIds = task.tokenIds

    const tokenIds = [...bottlomListing, ...taskTokenIds]
    const tokenBid = task.bidType === "token" && tokenIds.length > 0

    if (traitBid && !collectionDetails.trait_offers_enabled && !tokenBid) {
      console.log(RED + `Trait bidding is not available for ${task.contract.slug} on OpenSea.`.toUpperCase() + RESET);
      return;
    }

    const { offerPriceEth, maxBidPriceEth } = calculateBidPrice(task, floor_price, "opensea")

    const approved = await approveMarketplace(WETH_CONTRACT_ADDRESS, SEAPORT, task, maxBidPriceEth);

    if (!approved) return

    let offerPrice = BigInt(Math.ceil(offerPriceEth * 1e18));
    const creatorFees: IFee = collectionDetails.creator_fees.null !== undefined
      ? { null: collectionDetails.creator_fees.null }
      : Object.fromEntries(Object.entries(collectionDetails.creator_fees).map(([key, value]) => [key, Number(value)]));

    if (tokenBid) {
      const jobs = tokenIds
        .filter(token => !isNaN(Number(token)))
        .map((token) => ({
          name: OPENSEA_TOKEN_BID,
          data: {
            _id: task._id,
            address: WALLET_ADDRESS,
            privateKey: WALLET_PRIVATE_KEY,
            slug: task.contract.slug,
            offerPrice: offerPrice.toString(),
            creatorFees,
            enforceCreatorFee: collectionDetails.enforceCreatorFee,
            asset: { contractAddress: task.contract.contractAddress, tokenId: token },
            expiry,
            outbidOptions: task.outbidOptions,
            maxBidPriceEth: maxBidPriceEth
          },
          opts: { priority: TOKEN_BID_PRIORITY.OPENSEA }
        }));

      await processBulkJobs(jobs, true);

      console.log(`ADDED ${jobs.length} ${task.contract.slug} OPENSEA TOKEN BID JOBS TO QUEUE`);
    } else if (traitBid && collectionDetails.trait_offers_enabled) {
      const traits = transformOpenseaTraits(selectedTraits);
      const traitJobs = traits.map((trait) => ({
        name: OPENSEA_TRAIT_BID,
        data: {
          _id: task._id,
          address: WALLET_ADDRESS,
          privateKey: WALLET_PRIVATE_KEY,
          slug: task.contract.slug,
          contractAddress: task.contract.contractAddress,
          offerPrice: offerPrice.toString(),
          creatorFees,
          enforceCreatorFee: collectionDetails.enforceCreatorFee,
          trait: JSON.stringify(trait),
          expiry,
          outbidOptions: task.outbidOptions,
          maxBidPriceEth: maxBidPriceEth
        },
        opts: { priority: TRAIT_BID_PRIORITY.OPENSEA }
      }));

      if (task.running) {

        await processBulkJobs(traitJobs, true)

      };

      console.log(`ADDED ${traitJobs.length} ${task.contract.slug} OPENSEA TRAIT BID JOBS TO QUEUE`);
    } else if (task.bidType.toLowerCase() === "collection" && !traitBid) {
      const currentTask = activeTasks.get(task._id)

      if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) return;

      let colletionOffer = BigInt(offerPrice)

      const orderTrackingKey = `{${task._id}}:opensea:orders`;
      const orderKeys = await getPatternKeys(orderTrackingKey, 'collection') || []
      const [orderKey] = orderKeys
      const ttl = await redis.ttl(orderKey)
      const marketDataPromise = task.outbidOptions.outbid ?
        fetchOpenseaOffers(
          "COLLECTION",
          task.contract.slug,
          task.contract.contractAddress,
          {}
        ) :
        null;

      if (!task.outbidOptions.outbid) {
        if (ttl >= MIN_BID_DURATION) {
          return;
        }
      }
      else {
        const highestBid = await marketDataPromise;
        const highestBidAmount = typeof highestBid === 'object' && highestBid ? Number(highestBid.amount) : Number(highestBid);

        const owner = highestBid && typeof highestBid === 'object' ? highestBid.owner?.toLowerCase() : '';
        const isOwnBid = walletsArr
          .map(addr => addr.toLowerCase())
          .includes(owner);

        // CASE 1 - no top offer
        if (isOwnBid) {
          if (ttl > MIN_BID_DURATION) {
            return
          }
        }
        if (!isOwnBid) {

          const outbidMargin = (task.outbidOptions.openseaOutbidMargin || 0.0001) * 1e18
          colletionOffer = BigInt(highestBidAmount + outbidMargin)

          const offerPriceEth = Number(colletionOffer) / 1e18;
          if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
            if (!skipStats[task._id]) {
              skipStats[task._id] = {
                opensea: 0,
                magiceden: 0,
                blur: 0
              };
            }
            skipStats[task._id]['opensea']++;
            console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
            console.log(RED + `❌ Offer price ${offerPriceEth} WETH for ${task.contract.slug} collection bid exceeds max bid price ${maxBidPriceEth} WETH FOR OPENSEA.Skipping ...`.toUpperCase() + RESET);
            console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
            return;
          }

        }
        if (highestBidAmount === 0) {
          offerPrice = BigInt(Math.ceil(offerPriceEth * 1e18));
        }
      }

      const bidCount = getIncrementedBidCount(OPENSEA, task.contract.slug, task._id)
      if (orderKeys.length > 0) {
        const bidData = await redis.mget(orderKeys);
        const cancelData = bidData.map((order, index) => {
          if (!order) return
          const parsed = JSON.parse(order);
          return {
            name: CANCEL_OPENSEA_BID,
            data: { privateKey: task.wallet.privateKey, orderId: parsed.orderId, taskId: task._id, orderKey: orderKeys[index] }
          }
        });
        await processBulkJobs(cancelData);
      }

      await bidOnOpensea(
        task._id,
        bidCount,
        WALLET_ADDRESS,
        WALLET_PRIVATE_KEY,
        task.contract.slug,
        Number(offerPrice),
        creatorFees,
        collectionDetails.enforceCreatorFee,
        expiry
      )
      console.log(GREEN + `✅ Successfully placed bid of ${Number(offerPrice) / 1e18} ETH for ${task.contract.slug}` + RESET);
    }

  } catch (error) {
    console.error(RED + `Error processing OpenSea scheduled bid for task: ${task._id} ` + RESET, error);
  }
}

async function processBlurScheduledBid(task: ITask) {
  try {
    if (!task.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("blur")) return

    const filteredTasks = Object.fromEntries(
      Object.entries(task?.selectedTraits || {}).map(([category, traits]) => [
        category,
        traits.filter(trait => trait.availableInMarketplaces
          .map((item) => item.toLowerCase())
          .includes("blur"))
      ]).filter(([_, traits]) => traits.length > 0)
    );
    const tokenBid = task.bidType === "token"
    if (tokenBid) return
    const expiry = getExpiry(task.bidDuration)
    const WALLET_ADDRESS: string = task.wallet.address;
    const WALLET_PRIVATE_KEY: string = task.wallet.privateKey;
    const selectedTraits = transformNewTask(filteredTasks)
    const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0
    let floor_price: number = 0

    if (task.bidPrice.minType == "eth" || task.openseaBidPrice.minType === "eth") {
      floor_price = 0
    } else {
      const stats = await fetchBlurCollectionStats(task.contract.slug);
      floor_price = stats.total.floor_price;
    }

    const { offerPriceEth, maxBidPriceEth } = calculateBidPrice(task, floor_price, "blur")

    let offerPrice = BigInt(Math.round(offerPriceEth * 1e18 / 1e16) * 1e16);
    const contractAddress = task.contract.contractAddress

    if (traitBid) {
      const traits = transformBlurTraits(selectedTraits)
      const traitJobs = traits.map((trait) => ({
        name: BLUR_TRAIT_BID,
        data: {
          _id: task._id,
          address: WALLET_ADDRESS,
          privateKey: WALLET_PRIVATE_KEY,
          contractAddress,
          offerPrice: offerPrice.toString(),
          slug: task.contract.slug,
          trait: JSON.stringify(trait),
          expiry,
          outbidOptions: task.outbidOptions,
          maxBidPriceEth: maxBidPriceEth
        },
        opts: { priority: TRAIT_BID_PRIORITY.BLUR }
      }));

      if (task.running) {
        await processBulkJobs(traitJobs, true);
      }

      console.log(`ADDED ${traitJobs.length} ${task.contract.slug} BLUR TRAIT BID JOBS TO QUEUE`);
    } else if (task.bidType.toLowerCase() === "collection" && !traitBid) {
      const currentTask = activeTasks.get(task._id)

      if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur")) return;

      let colletionOffer = BigInt(offerPrice)
      const orderTrackingKey = `{${task._id}}:blur:orders`;
      const orderKeys = await getPatternKeys(orderTrackingKey, 'collection') || []
      const [orderKey] = orderKeys
      const ttl = await redis.ttl(orderKey)

      const marketDataPromise = task.outbidOptions.outbid ?
        fetchBlurBid(task.contract.slug, "COLLECTION", {}) :
        null;

      if (!task.outbidOptions.outbid) {
        if (ttl >= MIN_BID_DURATION) {
          return;
        }
      }
      else {
        const highestBid = await marketDataPromise;
        const highestBidAmount = Number(highestBid?.priceLevels[0].price) * 1e18
        const orderData = await redis.mget(orderKeys);

        const offers = orderData.map((order) => {
          if (!order) return
          const parsed = JSON.parse(order);
          return Number(parsed.offer)
        })

        const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))
        if (highestBidAmount > currentBidPrice) {
          if (highestBidAmount) {
            const outbidMargin = (task.outbidOptions.blurOutbidMargin || 0.01) * 1e18
            colletionOffer = BigInt(highestBidAmount + outbidMargin)

            const offerPriceEth = Number(colletionOffer) / 1e18;
            if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
              if (!skipStats[task._id]) {
                skipStats[task._id] = {
                  opensea: 0,
                  magiceden: 0,
                  blur: 0
                };
              }
              skipStats[task._id]['blur']++;
              console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
              console.log(RED + `❌ Offer price ${offerPriceEth} BETH for ${task.contract.slug} collection bid exceeds max bid price ${maxBidPriceEth} BETH FOR BLUR.Skipping ...`.toUpperCase() + RESET);
              console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
              return;
            }
          }
        }
      }
      const bidCount = getIncrementedBidCount(BLUR, task.contract.slug, task._id)
      if (orderKeys.length > 0) {
        const orderData = await redis.mget(orderKeys);
        const cancelData = orderData.map((orderKey, index) => {
          if (!orderKey) return
          const order = JSON.parse(orderKey);
          const payload = order.payload

          return {
            name: CANCEL_BLUR_BID,
            data: { privateKey: task.wallet.privateKey, payload, taskId: task._id, orderKey: orderKeys[index] }
          }
        })
        await processBulkJobs(cancelData);
      }
      await bidOnBlur(task._id, bidCount, WALLET_ADDRESS, WALLET_PRIVATE_KEY, contractAddress, colletionOffer, task.contract.slug, expiry);
    }


  } catch (error) {
    console.error(RED + `Error processing Blur scheduled bid for task: ${task._id} ` + RESET, error);
  }
}
async function processOpenseaTraitBid(data: {
  _id: string
  address: string;
  privateKey: string;
  slug: string;
  contractAddress: string;
  offerPrice: string;
  creatorFees: IFee;
  enforceCreatorFee: boolean;
  trait: string;
  expiry: number;
  outbidOptions: {
    outbid: boolean;
    blurOutbidMargin: number | null;
    openseaOutbidMargin: number | null;
    magicedenOutbidMargin: number | null;
    counterbid: boolean;
  }
  maxBidPriceEth: number;
}) {
  try {
    const { address, privateKey, slug, offerPrice, creatorFees, enforceCreatorFee, trait, expiry, outbidOptions, maxBidPriceEth, contractAddress, _id } = data

    const currentTask = activeTasks.get(_id)
    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) return;

    // Initialize bid amount and parse trait data
    let colletionOffer = BigInt(offerPrice)
    const { type, value } = JSON.parse(trait);

    const orderTrackingKey = `{${_id}}:opensea:orders`;

    const orderKeys = await getPatternKeys(orderTrackingKey, `${type}:${value}`) || []
    const [orderKey] = orderKeys
    const ttl = await redis.ttl(orderKey)
    const marketDataPromise = outbidOptions.outbid ?
      fetchOpenseaOffers('TRAIT', slug, contractAddress, JSON.parse(trait)) :
      null;

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION) return;
    }
    else {
      const highestBid = await marketDataPromise;
      const highestBidAmount = typeof highestBid === 'object' && highestBid ? Number(highestBid.amount) : Number(highestBid);
      const owner = highestBid && typeof highestBid === 'object' ? highestBid.owner?.toLowerCase() : '';
      const isOwnBid = walletsArr
        .map(addr => addr.toLowerCase())
        .includes(owner);

      if (isOwnBid) {
        if (ttl > MIN_BID_DURATION) {
          return
        }
      }
      if (!isOwnBid) {
        const outbidMargin = (outbidOptions.openseaOutbidMargin || 0.0001) * 1e18

        colletionOffer = BigInt(highestBidAmount + outbidMargin)
        const offerPriceEth = Number(colletionOffer) / 1e18;
        if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
          if (!skipStats[_id]) {
            skipStats[_id] = {
              opensea: 0,
              magiceden: 0,
              blur: 0
            };
          }
          skipStats[_id]['opensea']++;
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `❌ Offer price ${offerPriceEth} WETH for ${slug} ${trait} exceeds max bid price ${maxBidPriceEth} WETH FOR OPENSEA.Skipping ...`.toUpperCase() + RESET);
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          return
        }
      }

      if (highestBidAmount === 0) {
        colletionOffer = BigInt(offerPrice)
      }
    }

    const bidCount = getIncrementedBidCount(OPENSEA, slug, _id)
    if (orderKeys.length > 0) {
      const bidData = await redis.mget(orderKeys);
      const cancelData = bidData.map((order, index) => {
        if (!order) return
        const parsed = JSON.parse(order);
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey, orderId: parsed.orderId, taskId: _id, orderKey: orderKeys[index] }
        }
      });
      await processBulkJobs(cancelData)
    }

    await bidOnOpensea(
      _id,
      bidCount,
      address,
      privateKey,
      slug,
      Number(colletionOffer),
      creatorFees,
      enforceCreatorFee,
      expiry,
      trait
    )
  } catch (error) {
    console.error(RED + `❌ Error processing OpenSea trait bid for task: ${data?.slug} ` + RESET, error);
  }
}

async function processOpenseaTokenBid(data: IProcessOpenseaTokenBidData) {
  try {
    const { address, privateKey, slug, offerPrice, creatorFees, enforceCreatorFee, asset, expiry, outbidOptions, maxBidPriceEth, _id } = data
    const currentTask = activeTasks.get(_id)
    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) return;

    let colletionOffer = BigInt(offerPrice)

    const orderTrackingKey = `{${_id}}:opensea:orders`;
    const orderKeys = await getPatternKeys(orderTrackingKey, asset.tokenId.toString()) || []
    const [orderKey] = orderKeys

    const marketDataPromise = outbidOptions.outbid ?
      fetchOpenseaOffers("TOKEN", slug, asset.contractAddress, asset.tokenId.toString()) :
      null;

    const ttl = await redis.ttl(orderKey)

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION) return;

    } else {
      const highestBid = await marketDataPromise;
      const highestBidAmount = typeof highestBid === 'object' && highestBid ? Number(highestBid.amount) : Number(highestBid);
      const owner = highestBid && typeof highestBid === 'object' ? highestBid.owner?.toLowerCase() : '';

      const isOwnBid = walletsArr
        .map(addr => addr.toLowerCase())
        .includes(owner);

      if (isOwnBid) {
        if (ttl > MIN_BID_DURATION) {
          return
        }
      }
      if (!isOwnBid) {
        const outbidMargin = (outbidOptions.openseaOutbidMargin || 0.0001) * 1e18
        colletionOffer = BigInt(highestBidAmount + outbidMargin)

        const offerPriceEth = Number(colletionOffer) / 1e18;
        if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
          if (!skipStats[_id]) {
            skipStats[_id] = {
              opensea: 0,
              magiceden: 0,
              blur: 0
            };
          }
          skipStats[_id]['opensea']++;
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `❌ Offer price ${offerPriceEth} WETH for ${slug} token ${asset.tokenId} exceeds max bid price ${maxBidPriceEth} WETH FOR OPENSEA.Skipping ...`.toUpperCase() + RESET);
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          return;
        }
      }

      if (highestBidAmount === 0) {
        colletionOffer = BigInt(offerPrice)
      }
    }

    const bidCount = getIncrementedBidCount(OPENSEA, slug, _id)
    if (orderKeys.length > 0) {
      const bidData = await redis.mget(orderKeys);
      const cancelData = bidData.map((order, index) => {
        if (!order) return
        const parsed = JSON.parse(order);
        return {
          name: CANCEL_OPENSEA_BID,
          data: { privateKey, orderId: parsed.orderId, taskId: _id, orderKey: orderKeys[index] }
        }
      });
      await processBulkJobs(cancelData)
    }
    await bidOnOpensea(
      _id,
      bidCount,
      address,
      privateKey,
      slug,
      Number(colletionOffer),
      creatorFees,
      enforceCreatorFee,
      expiry,
      undefined,
      asset
    )
  } catch (error: any) {
    console.error(RED + `❌ Error processing OpenSea token bid for task: ${data?.slug} ` + RESET, error.response.data || error.message);
  }
}

async function openseaCollectionCounterBid(data: IOpenseaBidParams) {
  const { taskId, bidCount, walletAddress, walletPrivateKey, slug, offerPrice, creatorFees, enforceCreatorFee, expiry } = data
  const currentTask = activeTasks.get(taskId)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) {
    return
  }

  try {
    await bidOnOpensea(
      taskId,
      bidCount,
      walletAddress,
      walletPrivateKey,
      slug,
      Number(offerPrice),
      creatorFees,
      enforceCreatorFee,
      expiry,
    )

  } catch (error) {
    console.error(RED + `❌ Error processing OpenSea collection counter bid for task: ${slug} ` + RESET, error);
  }
}

async function magicedenCollectionCounterBid(data: IMagicedenCollectionBidData) {
  const { _id, bidCount, address, contractAddress, quantity, offerPrice, expiration, privateKey, slug } = data
  const currentTask = activeTasks.get(_id)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) return

  try {
    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, offerPrice.toString(), privateKey, slug);
  } catch (error) {
    console.error(RED + `❌ Error processing MagicEden collection counter bid for task: ${slug} ` + RESET, error);
  }
}

interface IMagicedenTraitBidData {
  taskId: string
  bidCount: string
  address: string
  contractAddress: string
  quantity: number
  offerPrice: number
  expiration: string
  privateKey: string
  slug: string
  trait: string
}

async function magicedenTraitCounterBid(data: IMagicedenTraitBidData) {

  const { taskId, bidCount, address, contractAddress, quantity, offerPrice, expiration, privateKey, slug, trait } = data
  const currentTask = activeTasks.get(taskId)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) return
  const triatData = JSON.parse(trait)
  const bidTrait = {
    attributeKey: triatData?.attributeKey,
    attributeValue: triatData?.attributeValue,
  }
  try {
    await bidOnMagiceden(taskId, bidCount, address, contractAddress, quantity, offerPrice.toString(), privateKey, slug, bidTrait);
  } catch (error) {
    console.error(RED + `❌ Error processing MagicEden trait counter bid for task: ${slug} ` + RESET, error);
  }
}

async function blurCollectionCounterBid(data: IBlurBidData) {
  const { taskId, bidCount, address, privateKey, contractAddress, offerPrice, slug, expiry } = data
  const currentTask = activeTasks.get(taskId)

  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur")) {
    return
  }

  try {

    const orderKeyPattern = `{${taskId}:*: blur: order:${slug} }:default `
    let remainingOffers = await redis.keys(orderKeyPattern)

    await bidOnBlur(taskId, bidCount, address, privateKey, contractAddress, BigInt(offerPrice), slug, expiry);
    const redisKey = `blur:${slug} `;
    const offerKey = `{${bidCount}:${redisKey} }: collection`

    remainingOffers = remainingOffers.filter((offer) => offer !== undefined)
    if (remainingOffers.length > 0) {
      const cancelData = remainingOffers.map((orderKey) => {
        return {
          name: CANCEL_BLUR_BID,
          data: { privateKey, orderKey: orderKey, taskId }
        }
      })


      await processBulkJobs(cancelData)
    }

  } catch (error) {
    console.error(RED + `❌ Error processing Blur collection counter bid for task: ${slug} ` + RESET, error);
  }
}

async function blurTraitCounterBid(data: BlurTraitCounterBid) {
  const { taskId, bidCount, address, privateKey, contractAddress, offerPrice, slug, expiry, trait } = data
  const currentTask = activeTasks.get(taskId)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur")) {
    return
  }
  try {
    await bidOnBlur(taskId, bidCount, address, privateKey, contractAddress, BigInt(offerPrice), slug, expiry, trait);
  } catch (error) {
    console.error(RED + `❌ Error processing Blur trait counter bid for task: ${data?.slug} ` + RESET, error);
  }

}

interface BlurTraitCounterBid {
  taskId: string;
  bidCount: string;
  address: string;
  privateKey: string;
  contractAddress: string;
  offerPrice: number;
  slug: string;
  expiry: number;
  trait: string;
}

async function openseaTokenCounterBid(data: IProcessOpenseaTokenBidData) {
  const {
    _id,
    address,
    privateKey,
    slug,
    offerPrice,
    creatorFees,
    enforceCreatorFee,
    expiry,
    asset,

  } = data

  const currentTask = activeTasks.get(_id)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) return

  const bidCount = getIncrementedBidCount(OPENSEA, slug, _id)
  try {
    await bidOnOpensea(
      _id,
      bidCount,
      address,
      privateKey,
      slug,
      Number(offerPrice),
      creatorFees,
      enforceCreatorFee,
      expiry,
      undefined,
      asset
    )
  } catch (error) {
    console.error(RED + `❌ Error processing OpenSea token counter bid for task: ${data?.slug} ` + RESET, error);
  }
}

async function openseaTraitCounterBid(data: IProcessOpenseaTraitBidData) {
  try {
    const { _id, bidCount, address, privateKey, slug, offerPrice, creatorFees, enforceCreatorFee, expiry, trait } = data
    if (!trait) return
    bidOnOpensea(
      _id,
      bidCount,
      address,
      privateKey,
      slug,
      Number(offerPrice),
      creatorFees,
      enforceCreatorFee,
      expiry,
      trait
    )
  } catch (error) {
    console.error(RED + `❌ Error processing OpenSea trait counter bid for task: ${data?.slug} ` + RESET, error);
  }
}

async function magicedenTokenCounterBid(data: IMagicedenTokenBidData) {
  try {
    const { _id, address, contractAddress, quantity, offerPrice, privateKey, slug, tokenId } = data
    const currentTask = activeTasks.get(_id)
    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) return
    const bidCount = getIncrementedBidCount(MAGICEDEN, slug, _id)
    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, offerPrice.toString(), privateKey, slug, undefined, tokenId)
  } catch (error) {
    console.error('Error in magicedenTokenCounterBid:', error);
  }
}


async function processBlurTraitBid(data: {
  _id: string;
  address: string;
  privateKey: string;
  contractAddress: string;
  offerPrice: string;
  slug: string;
  trait: string;
  expiry: number;
  outbidOptions: {
    outbid: boolean;
    blurOutbidMargin: number | null;
    openseaOutbidMargin: number | null;
    magicedenOutbidMargin: number | null;
    counterbid: boolean;
  };
  maxBidPriceEth: number
}) {
  const { address, privateKey, contractAddress, offerPrice, slug, trait, expiry, outbidOptions, maxBidPriceEth, _id } = data;
  let collectionOffer = BigInt(Math.round(Number(offerPrice) / 1e16) * 1e16);
  const traitsObj = JSON.parse(trait);
  const [traitType, traitValue] = Object.entries(traitsObj)[0];
  const identifier = `${traitType}:${traitValue}`
  const orderTrackingKey = `{${_id}}:blur:orders`;
  const currentTask = activeTasks.get(_id)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur")) return

  try {
    const orderKeys = await getPatternKeys(orderTrackingKey, identifier) || []
    const [orderKey] = orderKeys
    const ttl = await redis.ttl(orderKey)
    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION) return;
    }
    else {
      const outbidMargin = outbidOptions.blurOutbidMargin || 0.01;
      const bids = await fetchBlurBid(slug, "TRAIT", JSON.parse(trait));
      const highestBids = bids?.priceLevels?.length ? bids.priceLevels.sort((a, b) => +b.price - +a.price)[0].price : 0;

      const orderTrackingKey = `{${_id}}:blur:orders`;
      const orderKeys = await getPatternKeys(orderTrackingKey, identifier) || []
      const orderData = orderKeys.length ? await redis.mget(orderKeys) : []
      const offers = orderData.map((order: any) => {
        if (order.offer) return
        const parsed = JSON.parse(order)
        return parsed.offer
      })

      const currentBidPrice = !offers.length ? 0 : Math.max(...offers.map((offer) => Number(offer)))
      if (Number(highestBids) * 1e18 <= currentBidPrice) {
        if (ttl >= MIN_BID_DURATION) return;
      }
      const bidPrice = Number(highestBids) + outbidMargin;
      collectionOffer = BigInt(Math.ceil(bidPrice * 1e18));
    }
    const offerPriceEth = Number(collectionOffer) / 1e18;
    if (outbidOptions.outbid && maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
      if (!skipStats[_id]) {
        skipStats[_id] = {
          opensea: 0,
          magiceden: 0,
          blur: 0
        };
      }
      skipStats[_id]['blur']++;
      console.log(RED + '--------------------------------------------------------------------------------------------------' + RESET);
      console.log(RED + `Offer price ${offerPriceEth} ETH for ${slug} trait ${trait} exceeds max bid price ${maxBidPriceEth} ETH.Skipping ...`.toUpperCase() + RESET);
      console.log(RED + '--------------------------------------------------------------------------------------------------' + RESET);
      return;
    }
    const bidCount = getIncrementedBidCount(BLUR, slug, _id)
    if (orderKeys.length > 0) {
      const orderData = await redis.mget(orderKeys);
      const cancelData = orderData.map((orderKey, index) => {
        if (!orderKey) return
        const order = JSON.parse(orderKey);
        const payload = order.payload

        return {
          name: CANCEL_BLUR_BID,
          data: { privateKey, payload, taskId: _id, orderKey: orderKeys[index] }
        }
      })
      await processBulkJobs(cancelData)
    }
    await bidOnBlur(_id, bidCount, address, privateKey, contractAddress, collectionOffer, slug, expiry, trait);
  } catch (error) {
    console.error(RED + `Error processing Blur trait bid for task: ${data?.slug} ` + RESET, error);
  }
}

async function processMagicedenScheduledBid(task: ITask) {
  try {
    if (!task.running || !task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("magiceden")) return
    const filteredTasks = Object.fromEntries(
      Object.entries(task?.selectedTraits || {}).map(([category, traits]) => [
        category,
        traits.filter(trait => trait.availableInMarketplaces.includes("magiceden"))
      ]).filter(([_, traits]) => traits.length > 0)
    );

    const WALLET_ADDRESS: string = task.wallet.address
    const WALLET_PRIVATE_KEY: string = task.wallet.privateKey
    const selectedTraits = transformNewTask(filteredTasks)
    const traitBid = selectedTraits && Object.keys(selectedTraits).length > 0

    const contractAddress = task.contract.contractAddress
    const expiry = getExpiry(task.bidDuration)
    const duration = expiry / 60 || 15;
    const currentTime = new Date().getTime();
    const expiration = Math.floor((currentTime + (duration * 60 * 1000)) / 1000);

    let floor_price: number = 0

    if (task.bidPrice.minType == "eth" || task.openseaBidPrice.minType === "eth") {
      floor_price = 0
    } else {
      floor_price = Number(await fetchMagicEdenCollectionStats(task.contract.contractAddress))
    }

    const autoIds = task.tokenIds
      .filter(id => id.toString().toLowerCase().startsWith('bot'))
      .map(id => {
        const matches = id.toString().match(/\d+/);
        return matches ? parseInt(matches[0]) : null;
      })
      .filter(id => id !== null);

    const amount = autoIds[0]
    const bottlomListing = amount ? await fetchMagicEdenTokens(task.contract.contractAddress, amount) : []
    const taskTokenIds = task.tokenIds
    const tokenIds = bottlomListing ? [...bottlomListing, ...taskTokenIds] : [...taskTokenIds]
    const tokenBid = task.bidType === "token" && tokenIds.length > 0

    const { offerPriceEth, maxBidPriceEth } = calculateBidPrice(task, floor_price as number, "magiceden")

    const approved = await approveMarketplace(WETH_CONTRACT_ADDRESS, MAGICEDEN_MARKETPLACE, task, maxBidPriceEth);

    if (!approved) return
    let offerPrice = Math.ceil(offerPriceEth * 1e18)

    if (tokenBid) {
      const jobs = tokenIds
        .filter(token => !isNaN(Number(token)))
        .map((token) => ({
          name: MAGICEDEN_TOKEN_BID,
          data: {
            _id: task._id,
            address: WALLET_ADDRESS,
            contractAddress,
            quantity: 1,
            offerPrice,
            expiration,
            privateKey: WALLET_PRIVATE_KEY,
            slug: task.contract.slug,
            tokenId: token,
            outbidOptions: task.outbidOptions,
            maxBidPriceEth
          },
          opts: { priority: TOKEN_BID_PRIORITY.MAGICEDEN }
        }));

      await processBulkJobs(jobs, true);

      console.log(`ADDED ${jobs.length} ${task.contract.slug} MAGICEDEN TOKEN BID JOBS TO QUEUE`);
    }
    else if (traitBid) {
      const traits = Object.entries(selectedTraits).flatMap(([key, values]) =>
        values.map(value => ({ attributeKey: key, attributeValue: value })))
      const traitJobs = traits.map((trait) => ({
        name: MAGICEDEN_TRAIT_BID,
        data: {
          _id: task._id,
          address: WALLET_ADDRESS,
          contractAddress,
          quantity: 1,
          offerPrice,
          expiration,
          privateKey: WALLET_PRIVATE_KEY,
          slug: task.contract.slug,
          trait,
          outbidOptions: task.outbidOptions,
          maxBidPriceEth
        },
        opts: { priority: TRAIT_BID_PRIORITY.MAGICEDEN }
      }));

      await processBulkJobs(traitJobs, true);

      console.log(`ADDED ${traitJobs.length} ${task.contract.slug} MAGICEDEN TRAIT BID JOBS TO QUEUE`);
    }
    else if (task.bidType.toLowerCase() === "collection" && !traitBid) {
      const currentTask = activeTasks.get(task._id)
      if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) return;

      let colletionOffer = BigInt(offerPrice)
      const orderTrackingKey = `{${task._id}}:magiceden:orders`;
      const orderKeys = await getPatternKeys(orderTrackingKey, 'collection') || []
      const [orderKey] = orderKeys
      const ttl = await redis.ttl(orderKey)

      const marketDataPromise = task.outbidOptions.outbid ?
        fetchMagicEdenOffer("COLLECTION", task.wallet.address, task.contract.contractAddress) :
        null;

      if (!task.outbidOptions.outbid) {
        if (ttl >= MIN_BID_DURATION) {
          return;
        }
      }

      else {
        const highestBid = await marketDataPromise;
        const highestBidAmount = typeof highestBid === 'object' && highestBid ? Number(highestBid.amount) : Number(highestBid);
        const owner = highestBid && typeof highestBid === 'object' ? highestBid.owner?.toLowerCase() : '';
        const isOwnBid = walletsArr
          .map(addr => addr.toLowerCase())
          .includes(owner);
        if (isOwnBid) {
          if (ttl > MIN_BID_DURATION) {
            return
          }
        }
        if (!isOwnBid) {
          const outbidMargin = (task.outbidOptions.magicedenOutbidMargin || 0.0001) * 1e18
          colletionOffer = BigInt(highestBidAmount + outbidMargin)

          const offerPriceEth = Number(colletionOffer) / 1e18;
          if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
            if (!skipStats[task._id]) {
              skipStats[task._id] = {
                opensea: 0,
                magiceden: 0,
                blur: 0
              };
            }
            skipStats[task._id]['magiceden']++;
            console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
            console.log(RED + `❌ Offer price ${offerPriceEth} WETH for ${task.contract.slug} collection bid exceeds max bid price ${maxBidPriceEth} WETH FOR MAGICEDEN.Skipping ...`.toUpperCase() + RESET);
            console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
            return;
          }

        }
      }

      const bidCount = getIncrementedBidCount(MAGICEDEN, task.contract.slug, task._id)
      await bidOnMagiceden(task._id, bidCount, WALLET_ADDRESS, contractAddress, 1, colletionOffer.toString(), WALLET_PRIVATE_KEY, task.contract.slug);

      if (orderKeys.length > 0) {
        const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
        await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: task.wallet.privateKey, orderKeys: orderKeys, taskId: task._id });
      }
    }
  } catch (error) {
    console.error(RED + `Error processing MagicEden scheduled bid for task: ${task._id} ` + RESET, error);
  }
}


async function processMagicedenTokenBid(data: IMagicedenTokenBidData) {
  try {
    const { contractAddress, address, quantity, offerPrice, expiration, privateKey, slug, tokenId, outbidOptions, maxBidPriceEth, _id } = data

    const currentTask = activeTasks.get(_id)
    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) return;
    const orderTrackingKey = `{${_id}}:magiceden:orders`;
    const orderKeys = await getPatternKeys(orderTrackingKey, tokenId.toString()) || []
    const [orderKey] = orderKeys
    const ttl = await redis.ttl(orderKey)

    const marketDataPromise = outbidOptions?.outbid ? fetchMagicEdenOffer("TOKEN", address, contractAddress, tokenId.toString()) : null

    let tokenOffer = Number(offerPrice)

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION) {
        return;
      }
    }

    if (outbidOptions?.outbid) {
      const highestBid = await marketDataPromise;
      const owner = highestBid && typeof highestBid === 'object' ? highestBid.owner?.toLowerCase() : '';
      const isOwnBid = walletsArr
        .map(addr => addr.toLowerCase())
        .includes(owner);

      if (isOwnBid) {
        if (ttl > MIN_BID_DURATION) {
          return
        }
      }

      if (!isOwnBid) {
        const highestBidAmount = typeof highestBid === 'object' && highestBid ? Number(highestBid.amount) : Number(highestBid);

        const outbidMargin = (outbidOptions.openseaOutbidMargin || 0.0001) * 1e18
        tokenOffer = highestBidAmount + outbidMargin
        const offerPriceEth = Number(tokenOffer) / 1e18;

        if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
          if (!skipStats[_id]) {
            skipStats[_id] = {
              opensea: 0,
              magiceden: 0,
              blur: 0
            };
          }
          skipStats[_id]['magiceden']++;
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `❌ Offer price ${offerPriceEth} ETH for ${slug} token ${tokenId} exceeds max bid price ${maxBidPriceEth} ETH FOR MAGICEDEN.Skipping ...`.toUpperCase() + RESET);
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          return;
        }
      }
    }
    const bidCount = getIncrementedBidCount(MAGICEDEN, slug, _id)
    if (orderKeys.length > 0) {
      const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
      await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys: orderKeys, taskId: _id });
    }
    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, tokenOffer.toString(), privateKey, slug, undefined, tokenId)
  } catch (error) {
    console.error(RED + `Error processing MagicEden token bid for task: ${data?.slug} ` + RESET, error);
  }
}

async function getPatternKeys(orderTrackingKey: string, identifier: string) {
  try {
    const orderKeys = await redis.smembers(orderTrackingKey);
    if (!orderKeys.length) return [];

    return orderKeys.filter(key => {
      const keyParts = key.split(':');
      const lastParts = keyParts.slice(-2);

      if (identifier.includes(':')) {
        return lastParts.join(':') === identifier;
      } else {
        return keyParts[keyParts.length - 1] === identifier;
      }
    });

  } catch (error) {
    console.log(error);
    return [];
  }
}

async function processMagicedenTraitBid(data: {
  _id: string;
  address: string;
  contractAddress: string;
  quantity: number;
  offerPrice: string;
  expiration: string;
  privateKey: string,
  slug: string;
  trait: {
    attributeKey: string;
    attributeValue: string;
  },
  outbidOptions: {
    outbid: boolean;
    blurOutbidMargin: number | null;
    openseaOutbidMargin: number | null;
    magicedenOutbidMargin: number | null;
    counterbid: boolean;
  }
  maxBidPriceEth: number;
}) {
  try {
    const
      { contractAddress, address, quantity, offerPrice, expiration, privateKey, slug, trait, _id, outbidOptions, maxBidPriceEth } = data

    const currentTask = activeTasks.get(_id)
    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) return

    const orderTrackingKey = `{${_id}}:magiceden:orders`;
    const orderKeys = await getPatternKeys(orderTrackingKey, `${trait.attributeKey}:${trait.attributeValue}`) || []

    const [orderKey] = orderKeys
    const ttl = await redis.ttl(orderKey)
    const marketDataPromise = outbidOptions.outbid ?
      fetchMagicEdenOffer('TRAIT', address, contractAddress, trait) :
      null;

    let traitOffer = Number(offerPrice)

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION) {
        return;
      }
    }

    else {
      const highestBid = await marketDataPromise;
      const highestBidAmount = typeof highestBid === 'object' && highestBid ? Number(highestBid.amount) : Number(highestBid);
      const owner = highestBid && typeof highestBid === 'object' ? highestBid.owner?.toLowerCase() : '';
      const isOwnBid = walletsArr
        .map(addr => addr.toLowerCase())
        .includes(owner);

      if (isOwnBid) {
        if (ttl > MIN_BID_DURATION) {
          return
        }
      }
      if (!isOwnBid) {
        const outbidMargin = (outbidOptions.openseaOutbidMargin || 0.0001) * 1e18
        traitOffer = highestBidAmount + outbidMargin
        const offerPriceEth = Number(traitOffer) / 1e18;


        console.log({ offerPriceEth, maxBidPriceEth });

        if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
          if (!skipStats[_id]) {
            skipStats[_id] = {
              opensea: 0,
              magiceden: 0,
              blur: 0
            };
          }
          skipStats[_id]['magiceden']++;
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `❌ Offer price ${offerPriceEth} WETH for ${slug} ${JSON.stringify(trait)} exceeds max bid price ${maxBidPriceEth} WETH FOR MAGICEDEN.Skipping ...`.toUpperCase() + RESET);
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          return
        }
      }
    }

    const bidCount = getIncrementedBidCount(MAGICEDEN, slug, _id)
    if (orderKeys.length > 0) {
      const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)
      await queue.add(CANCEL_MAGICEDEN_BID, {
        orderIds: extractedOrderIds,
        privateKey,
        orderKeys: orderKeys,
        taskId: _id
      });
    }
    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, traitOffer.toString(), privateKey, slug, trait)
  } catch (error) {
    console.error(RED + `Error processing MagicEden trait bid for task: ${data?.slug} ` + RESET, error);
  }
}

async function bulkCancelOpenseaBid(data: { privateKey: string, orderId: string, taskId: string, orderKey: string }) {
  const { privateKey, orderId, taskId, orderKey } = data
  try {
    await Promise.all([
      cancelOrder(orderId, OPENSEA_PROTOCOL_ADDRESS, privateKey, taskId),
      redis.srem(`{${taskId}}:opensea:orders`, orderKey),
      redis.del(orderKey)
    ])
  } catch (error) {
    console.error(RED + `Error processing Opensea bid cancellation for task: ${taskId} ` + RESET, error);
  }
}

async function cleanupMagicedenKeys(keys: string[]) {
  try {
    const offerKeys = keys.map(key => key.replace(':order:', ':offer:'));
    const allKeys = [...keys, ...offerKeys];

    const deleteResults = await Promise.allSettled(
      allKeys.map(key => redis.del(key))
    );

    deleteResults.filter(
      result => result.status === 'fulfilled'
    ).length;

  } catch (error) {
    console.error('Error cleaning up Magiceden keys across cluster:', error);
  }
}

async function bulkCancelMagicedenBid(data: { orderIds: string[], privateKey: string, orderKeys: string[], taskId: string }) {
  const { orderIds, privateKey, orderKeys, taskId } = data
  try {
    if (orderIds.length > 0) {
      await Promise.all([
        cancelMagicEdenBid(orderIds, privateKey, taskId),
        ...orderKeys.map((key) => redis.srem(`{${taskId}}:magiceden:orders`, key)),
        ...orderKeys.map((key) => redis.del(key))
      ]);
    }
  } catch (error) {
    console.error(RED + `Error processing Magiceden bid cancellation for task: ${taskId} ` + RESET, error);
  }
}

async function validateBidCount() {
  for (const task of currentTasks) {
    try {
      const [openseaOrders, magicedenOrders, blurOrders] = await Promise.all([
        redis.smembers(`{${task._id}}:opensea:orders`),
        redis.smembers(`{${task._id}}:magiceden:orders`),
        redis.smembers(`{${task._id}}:blur:orders`)
      ])

      await Promise.all([
        ...blurOrders.map(async (key) => {
          const ttl = await redis.ttl(key)
          if (ttl <= 0) {
            await redis.srem(`{${task._id}}:blur:orders`, key)
            blurOrders.filter((item) => item !== key)
          }
        }),
        ...openseaOrders.map(async (key) => {
          const ttl = await redis.ttl(key)
          if (ttl <= 0) {
            await redis.srem(`{${task._id}}:opensea:orders`, key)
            openseaOrders.filter((item) => item !== key)
          }
        }),
        ...magicedenOrders.map(async (key) => {
          const ttl = await redis.ttl(key)
          if (ttl <= 0) {
            await redis.srem(`{${task._id}}:magiceden:orders`, key)
            magicedenOrders.filter((item) => item !== key)
          }
        })
      ])

      bidStats[task._id] = {
        opensea: openseaOrders.length,
        magiceden: magicedenOrders.length,
        blur: blurOrders.length
      }
    } catch (error) {
      console.error(RED + `Error validating bid count for task: ${task._id}` + RESET, error);
    }
  }
}

setInterval(validateBidCount, 5000);

async function blukCancelBlurBid(data: BlurCancelPayload) {
  const { orderKey, privateKey, taskId, payload } = data
  try {
    if (!data) return
    await Promise.all([
      cancelBlurBid({ payload, privateKey, taskId }),
      redis.srem(`{${taskId}}:blur:orders`, orderKey),
      redis.del(orderKey),
    ])
  } catch (error) {
    console.error(RED + `Error processing Blur bid cancellation for task: ${taskId} ` + RESET, error);
  }
}

app.get("/", (req, res) => {
  res.json({ message: "Welcome to the NFTTools bidding bot server! Let's make magic happen! 🚀🚀🚀" });
});

function getExpiry(bidDuration: { value: number; unit: string }) {
  const expiry = bidDuration.unit === 'minutes'
    ? bidDuration.value * 60
    : bidDuration.unit === 'hours'
      ? bidDuration.value * 3600
      : bidDuration.unit === 'days'
        ? bidDuration.value * 86400
        : 900;

  return expiry
}

function calculateBidPrice(task: ITask, floorPrice: number, marketplaceName: "opensea" | "magiceden" | "blur"): { offerPriceEth: number; maxBidPriceEth: number } {
  const isGeneralBidPrice = task.bidPriceType === "GENERAL_BID_PRICE";

  const marketplaceBidPrice = marketplaceName.toLowerCase() === "blur" ? task.blurBidPrice
    : marketplaceName.toLowerCase() === "opensea" ? task.openseaBidPrice
      : marketplaceName.toLowerCase() === "magiceden" ? task.magicEdenBidPrice
        : task.bidPrice

  let bidPrice = isGeneralBidPrice ? task.bidPrice : marketplaceBidPrice;

  if (task.bidPriceType === "MARKETPLACE_BID_PRICE" && !marketplaceBidPrice) {
    if (!task.bidPrice) throw new Error("No bid price found");
    bidPrice = task.bidPrice;
  }

  let offerPriceEth: number;
  if (bidPrice.minType === "percentage") {
    offerPriceEth = Number((floorPrice * bidPrice.min / 100).toFixed(4));
  } else {
    offerPriceEth = bidPrice.min;
  }

  let maxBidPriceEth: number;
  if (bidPrice.maxType === "percentage") {
    maxBidPriceEth = Number((floorPrice * (bidPrice.max || task.bidPrice.max) / 100).toFixed(4));
  } else {
    maxBidPriceEth = bidPrice.max || task.bidPrice.max;
  }

  return { offerPriceEth, maxBidPriceEth };
}

function transformBlurTraits(selectedTraits: Record<string, string[]>): { [key: string]: string }[] {
  const result: { [key: string]: string }[] = [];
  for (const [traitType, values] of Object.entries(selectedTraits)) {
    for (const value of values) {
      if (value.includes(',')) {
        const subValues = value.split(',');
        for (const subValue of subValues) {
          result.push({ [traitType]: subValue.trim() });
        }
      } else {
        result.push({ [traitType]: value.trim() });
      }
    }
  }
  return result;
}

// Store bid counts in memory with a Map
const bidCounts = new Map<string, number>();

function getIncrementedBidCount(marketplace: string, slug: string, taskId: string): string {
  const countKey = `${marketplace}:${slug} `;
  const currentCount = bidCounts.get(countKey) || 0;
  const newCount = currentCount + 1;
  bidCounts.set(countKey, newCount);
  return `${taskId}:${newCount}`;
}

function transformOpenseaTraits(selectedTraits: Record<string, string[]>): { type: string; value: string }[] {
  const result: { type: string; value: string }[] = [];
  for (const [traitType, values] of Object.entries(selectedTraits)) {
    for (const value of values) {
      if (value.includes(',')) {
        const subValues = value.split(',');
        for (const subValue of subValues) {
          result.push({ type: traitType, value: subValue.trim() });
        }
      } else {
        result.push({ type: traitType, value: value.trim() });
      }
    }
  }
  return result;
}


let marketplaceIntervals: { [key: string]: NodeJS.Timeout } = {};
function subscribeToCollections(tasks: ITask[]) {
  try {
    tasks.forEach(async (task) => {
      // Create a unique subscription key for this collection
      const subscriptionKey = `${task.contract.slug} `;

      const clientId = task.user.toString()

      if (ws.readyState === WebSocket.OPEN) {
        ws.send(
          JSON.stringify({
            event: "ping",
            clientId
          })
        );
      }


      let retries = 0;
      const maxRetries = 5;
      const retryDelay = 1000; // 1 second

      while ((!ws || ws.readyState !== WebSocket.OPEN) && retries < maxRetries) {
        console.error(RED + `WebSocket is not open for subscribing to collections: ${task.contract.slug}. Retrying...` + RESET);
        await new Promise(resolve => setTimeout(resolve, retryDelay));
        retries++;
      }

      if (!ws || ws.readyState !== WebSocket.OPEN) {
        console.error(RED + `Failed to open WebSocket after ${maxRetries} retries for: ${task.contract.slug} ` + RESET);
        return;
      }

      const connectToOpensea = task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("opensea");
      const connectToBlur = task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("blur");
      const connectToMagiceden = task.selectedMarketplaces.map((marketplace) => marketplace.toLowerCase()).includes("magiceden");

      if (connectToOpensea && task.outbidOptions.counterbid && task.running) {
        const openseaSubscriptionMessage = {
          "slug": task.contract.slug,
          "event": "join_the_party",
          "topic": task.contract.slug,
          "contractAddress": task.contract.contractAddress,
          "clientId": task.user.toString(),
          "marketplace": OPENSEA
        };

        ws.send(JSON.stringify(openseaSubscriptionMessage));
        activeSubscriptions.add(subscriptionKey);
        console.log('----------------------------------------------------------------------');
        console.log(`SUBSCRIBED TO COLLECTION: ${task.contract.slug} OPENSEA`);
        console.log('----------------------------------------------------------------------');

        const intervalKey = `opensea:${task.contract.slug} `;
        if (!marketplaceIntervals[intervalKey]) {
          marketplaceIntervals[intervalKey] = setInterval(() => {
            if (ws?.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify({
                event: "ping",
                clientId: task.user.toString(),
              }));
            } else {
              clearInterval(marketplaceIntervals[intervalKey]);
              delete marketplaceIntervals[intervalKey];
            }
          }, 30000);
        }
      }

      if (connectToMagiceden && task.outbidOptions.counterbid && task.running) {
        const magicedenSubscriptionMessage = {
          "topic": task.contract.slug,
          "slug": task.contract.slug,
          "contractAddress": task.contract.contractAddress,
          "event": "join_the_party",
          "clientId": task.user.toString(),
          "payload": {},
          "ref": 0,
          "marketplace": MAGICEDEN
        }

        ws.send(JSON.stringify(magicedenSubscriptionMessage));
        activeSubscriptions.add(subscriptionKey);
        console.log('----------------------------------------------------------------------');
        console.log(`SUBSCRIBED TO COLLECTION: ${task.contract.slug} MAGICEDEN`);
        console.log('----------------------------------------------------------------------');

        const intervalKey = `magiceden:${task.contract.slug} `;
        if (!marketplaceIntervals[intervalKey]) {
          marketplaceIntervals[intervalKey] = setInterval(() => {
            if (ws?.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify({
                event: "ping",
                clientId: task.user.toString(),
              }));
            } else {
              clearInterval(marketplaceIntervals[intervalKey]);
              delete marketplaceIntervals[intervalKey];
            }
          }, 30000);
        }
      }

      if (connectToBlur && task.outbidOptions.counterbid && task.running) {
        const blurSubscriptionMessage = {
          "topic": task.contract.slug,
          "slug": task.contract.slug,
          "contractAddress": task.contract.contractAddress,
          "event": "join_the_party",
          "clientId": task.user.toString(),
          "payload": {},
          "ref": 0,
          "marketplace": BLUR
        }

        ws.send(JSON.stringify(blurSubscriptionMessage));
        activeSubscriptions.add(subscriptionKey);
        console.log('----------------------------------------------------------------------');
        console.log(`SUBSCRIBED TO COLLECTION: ${task.contract.slug} BLUR`);
        console.log('----------------------------------------------------------------------');

        const intervalKey = `blur:${task.contract.slug} `;
        if (!marketplaceIntervals[intervalKey]) {
          marketplaceIntervals[intervalKey] = setInterval(() => {
            if (ws?.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify({
                event: "ping",
                clientId: task.user.toString(),
              }));
            } else {
              clearInterval(marketplaceIntervals[intervalKey]);
              delete marketplaceIntervals[intervalKey];
            }
          }, 30000);
        }
      }
    })
  } catch (error) {
    console.error(RED + 'Error subscribing to collections' + RESET, error);
  }
}

function checkOpenseaTrait(selectedTraits: any, trait_criteria: any) {
  if (selectedTraits.hasOwnProperty(trait_criteria.trait_type)) {
    return selectedTraits[trait_criteria.trait_type].includes(trait_criteria.trait_name);
  }
  return false;
}

function checkMagicEdenTrait(selectedTraits: Record<string, string[]>, trait: { attributeKey: string; attributeValue: string }) {
  if (selectedTraits.hasOwnProperty(trait.attributeKey)) {
    return selectedTraits[trait.attributeKey].includes(trait.attributeValue)
  }
  return false;
}

export function decrementBidCount(marketplace: 'opensea' | 'magiceden' | 'blur', taskId: string) {
  if (!bidStats[taskId]) {
    return;
  }
  if (bidStats[taskId][marketplace] > 0) {
    bidStats[taskId][marketplace]--;
  }
}

interface BlurCancelPayload {
  payload: {
    contractAddress: string;
    criteriaPrices: Array<{
      price: string;
      criteria?: {
        type: string;
        value: Record<string, string>;
      }
    }>;
  };
  privateKey: string;
  orderKey: string;
  taskId: string;
}

function checkBlurTraits(incomingBids: any, traits: any) {
  const traitKeys = traits.flatMap((trait: any) => Object.entries(trait).map(([key, value]) => ({ key, value })));
  return incomingBids.filter((bid: any) => {
    return traitKeys.some((trait: any) => {
      return bid.criteriaValue[trait.key] === trait.value;
    });
  });
}


async function approveMarketplace(currency: string, marketplace: string, task: ITask, maxBidPriceEth: number): Promise<boolean> {
  const lockKey = `approve:${task.wallet.address.toLowerCase()}:${marketplace.toLowerCase()} `;

  return await lockManager.withLock(lockKey, async () => {
    try {
      const provider = new ethers.providers.AlchemyProvider('mainnet', ALCHEMY_API_KEY);
      const signer = new Web3Wallet(task.wallet.privateKey, provider);
      const wethContract = new Contract(currency, WETH_MIN_ABI, signer);

      let allowance = Number(await wethContract.allowance(task.wallet.address, marketplace)) / 1e18;
      if (allowance > maxBidPriceEth) return true;

      if (!task?.wallet.openseaApproval) {
        console.log(`Approving WETH ${marketplace} as a spender for wallet ${task.wallet.address} with amount: ${constants.MaxUint256.toString()}...`.toUpperCase());
        const tx = await wethContract.approve(marketplace, constants.MaxUint256);
        await tx.wait();

        const updateData = marketplace.toLowerCase() === SEAPORT.toLowerCase()
          ? { openseaApproval: true }
          : { magicedenApproval: true };

        await Wallet.updateOne({ address: { $regex: new RegExp(task.wallet.address, 'i') } }, updateData);
        await Task.updateOne({ _id: task._id }, { $set: updateData });
      }

      return true;
    } catch (error: any) {
      const name = marketplace.toLowerCase() === "0x0000000000000068f116a894984e2db1123eb395".toLowerCase() ? OPENSEA : MAGICEDEN;

      if (error.code === 'INSUFFICIENT_FUNDS') {
        console.error(RED + `Error: Wallet ${task.wallet.address} could not approve ${name} as a spender.Please ensure your wallet has enough ETH to cover the gas fees and permissions are properly set.`.toUpperCase() + RESET);
      } else {
        console.error(RED + `Error details: `, error);
        console.error(`Error message: ${error.message} `);
        console.error(`Error code: ${error.code} `);
        console.error(RED + `Error: Wallet ${task.wallet.address} could not approve the ${name} as a spender.Task has been stopped.`.toUpperCase() + RESET);
      }
      return false;
    }
  }) ?? false;
}

// In the hasActivePrioritizedJobs function:
async function hasActivePrioritizedJobs(task: ITask): Promise<boolean> {

  const jobs = await queue.getJobs(['prioritized', 'active']);
  const baseKey = `${task._id} -${task.contract?.slug} `;

  const hasJobs = jobs.some(job => {
    const jobId = job?.id || '';


    return jobId.startsWith(baseKey);
  });


  return hasJobs;
}


interface SelectedTraits {
  [key: string]: {
    name: string;
    availableInMarketplaces: string[];
  }[];
}

export interface ITask {
  _id: string;
  user: string;
  contract: {
    slug: string;
    contractAddress: string;
  };
  wallet: {
    address: string;
    privateKey: string;
    openseaApproval: boolean;
    blurApproval: boolean;
    magicedenApproval: boolean
  };
  selectedMarketplaces: string[];
  running: boolean;
  tags: { name: string; color: string }[];
  selectedTraits: SelectedTraits;
  traits: {
    categories: Record<string, string>;
    counts: Record<
      string,
      Record<string, { count: number; availableInMarketplaces: string[] }>
    >;
  };
  outbidOptions: {
    outbid: boolean;
    blurOutbidMargin: number | null;
    openseaOutbidMargin: number | null;
    magicedenOutbidMargin: number | null;
    counterbid: boolean;
  };
  bidPrice: {
    min: number;
    max: number;
    minType: "percentage" | "eth";
    maxType: "percentage" | "eth";
  };

  openseaBidPrice: {
    min: number;
    max: number;
    minType: "percentage" | "eth";
    maxType: "percentage" | "eth";
  };

  blurBidPrice: {
    min: number;
    max: number | null;
    minType: "percentage" | "eth";
    maxType: "percentage" | "eth";
  };

  magicEdenBidPrice: {
    min: number;
    max: number;
    minType: "percentage" | "eth";
    maxType: "percentage" | "eth";
  };

  stopOptions: {
    minFloorPrice: number | null;
    maxFloorPrice: number | null;
    minTraitPrice: number | null;
    maxTraitPrice: number | null;
    maxPurchase: number | null;
    pauseAllBids: boolean;
    stopAllBids: boolean;
    cancelAllBids: boolean;
    triggerStopOptions: boolean;
  };
  bidDuration: { value: number; unit: string };
  tokenIds: (number | string)[];
  bidType: string;
  loopInterval: { value: number; unit: string };
  bidPriceType: "GENERAL_BID_PRICE" | "MARKETPLACE_BID_PRICE";

}

interface BidLevel {
  contractAddress: string;
  updates: Update[];
}

interface Update {
  criteriaType: string;
  criteriaValue: Record<string, unknown>;
  price: string;
  executableSize: number;
  bidderCount: number;
  singleBidder: string | null;
}

interface BidStat {
  criteriaType: string;
  criteriaValue: { [key: string]: string };
  bestPrice: string;
  totalValue: string;
}

interface BidStats {
  contractAddress: string;
  stats: BidStat[];
}
interface CombinedBid extends BidStats, BidLevel { }


function transformNewTask(newTask: Record<string, Array<{ name: string }>>): Record<string, string[]> {
  const transformedTask: Record<string, string[]> = {};

  for (const key in newTask) {
    if (newTask.hasOwnProperty(key)) {
      transformedTask[key] = newTask[key].map(item => item.name);
    }
  }
  return transformedTask;
}

const abortedTasks: string[] = [];

function markTaskAsAborted(taskId: string) {
  if (!abortedTasks.includes(taskId)) {
    abortedTasks.push(taskId);
  }
}

function removeTaskFromAborted(taskId: string) {
  const index = abortedTasks.indexOf(taskId);
  if (index > -1) {
    abortedTasks.splice(index, 1);
  }
}

function isTaskAborted(taskId: string): boolean {
  return abortedTasks.includes(taskId);
}

interface IProcessOpenseaTokenBidData {
  _id: string;
  address: string;
  privateKey: string;
  slug: string;
  offerPrice: number;
  creatorFees: IFee;
  enforceCreatorFee: boolean;
  asset: {
    contractAddress: string;
    tokenId: number;
  };
  expiry: number;
  outbidOptions: {
    outbid: boolean;
    blurOutbidMargin: number | null;
    openseaOutbidMargin: number | null;
    magicedenOutbidMargin: number | null;
    counterbid: boolean;
  };
  maxBidPriceEth: number;
}

interface IMagicedenTokenBidData {
  _id: string;
  address: string;
  contractAddress: string;
  quantity: number;
  offerPrice: number;
  expiration: string;
  privateKey: string;
  slug: string;
  tokenId: number;
  outbidOptions: {
    outbid: boolean;
    blurOutbidMargin: number | null;
    openseaOutbidMargin: number | null;
    magicedenOutbidMargin: number | null;
    counterbid: boolean;
  };
  maxBidPriceEth: number;
}

// Create separate bid rate states for each marketplace
const bidRateState = {
  opensea: {
    bids: [] as { timestamp: number, count: number }[]
  },
  magiceden: {
    bids: [] as { timestamp: number, count: number }[]
  },
  blur: {
    bids: [] as { timestamp: number, count: number }[]
  }
};

const BID_WINDOW_SECONDS = 60;

export function getAllBidRates(): MarketplaceBidRates {

  const tasks = Array.from(activeTasks.values());
  const running = tasks.some(task => task.running);
  if (!running) {
    return {
      opensea: { bidsPerSecond: 0, totalBids: 0, windowPeriod: BID_WINDOW_SECONDS },
      blur: { bidsPerSecond: 0, totalBids: 0, windowPeriod: BID_WINDOW_SECONDS },
      magiceden: { bidsPerSecond: 0, totalBids: 0, windowPeriod: BID_WINDOW_SECONDS }
    };
  }

  return globalBidRates;
}

export const globalBidRates: MarketplaceBidRates = {
  opensea: { bidsPerSecond: 0, totalBids: 0, windowPeriod: BID_WINDOW_SECONDS },
  blur: { bidsPerSecond: 0, totalBids: 0, windowPeriod: BID_WINDOW_SECONDS },
  magiceden: { bidsPerSecond: 0, totalBids: 0, windowPeriod: BID_WINDOW_SECONDS }
};

// Add this interface near other interfaces
interface BidCounts {
  [key: string]: {
    opensea: number;
    magiceden: number;
    blur: number;
  };
}


export function trackBidRate(marketplace: 'opensea' | 'magiceden' | 'blur', taskId: string) {
  const now = Math.floor(Date.now() / 1000);
  const cutoff = now - BID_WINDOW_SECONDS;

  bidRateState[marketplace].bids.push({
    timestamp: now,
    count: 1
  });

  bidRateState[marketplace].bids = bidRateState[marketplace].bids.filter(bid =>
    bid.timestamp > cutoff
  );

  const totalBids = bidRateState[marketplace].bids.reduce((sum, bid) => sum + bid.count, 0);

  const rate = totalBids / Math.min(BID_WINDOW_SECONDS, now - (bidRateState[marketplace].bids[0]?.timestamp || now));

  globalBidRates[marketplace] = {
    bidsPerSecond: rate,
    totalBids: totalBids,
    windowPeriod: BID_WINDOW_SECONDS
  };

  if (!bidStats[taskId]) {
    bidStats[taskId] = {
      opensea: 0,
      magiceden: 0,
      blur: 0
    };
  }
  bidStats[taskId][marketplace]++;
  return { globalBidRates, bidStats };
}

export function getBidStats(taskId?: string): BidCounts | { [key: string]: number } {
  if (taskId) {
    return bidStats[taskId] || {
      opensea: 0,
      magiceden: 0,
      blur: 0
    };
  }
  return bidStats;
}

interface MarketplaceBidRates {
  opensea: BidRateStats;
  blur: BidRateStats;
  magiceden: BidRateStats;
}

interface BidRateStats {
  bidsPerSecond: number;
  totalBids: number;
  windowPeriod: number;
}

interface IOpenseaBidParams {
  taskId: string;
  bidCount: string;
  walletAddress: string;
  walletPrivateKey: string;
  slug: string;
  offerPrice: number;
  creatorFees: IFee;
  enforceCreatorFee: boolean;
  expiry: number;
}

interface IMagicedenCollectionBidData {
  _id: string;
  bidCount: string;
  address: string;
  contractAddress: string;
  quantity: number;
  offerPrice: number;
  expiration: string;
  privateKey: string;
  slug: string;
}
interface IBlurBidData {
  taskId: string;
  bidCount: string;
  address: string;
  privateKey: string;
  contractAddress: string;
  offerPrice: number;
  slug: string;
  expiry: number;
}

interface IProcessOpenseaTraitBidData {
  _id: string;
  bidCount: string;
  address: string;
  privateKey: string;
  slug: string;
  offerPrice: number;
  creatorFees: IFee;
  enforceCreatorFee: boolean;
  expiry: number;
  trait: string;
}

// Add near other constants
const MEMORY_THRESHOLD = 80; // 80% memory usage threshold
const QUEUE_SIZE_THRESHOLD = 50000; // Maximum number of jobs in queue

async function manageQueueHealth() {
  const used = process.memoryUsage();
  const memoryUsagePercent = (used.heapUsed / os.totalmem()) * 100;

  const queueSize = await queue.getJobCounts();
  const totalJobs = Object.values(queueSize).reduce((a, b) => a + b, 0);

  if (memoryUsagePercent > MEMORY_THRESHOLD || totalJobs > QUEUE_SIZE_THRESHOLD) {
    console.log(RED + `⚠️ Queue health check triggered - Memory: ${memoryUsagePercent.toFixed(2)}%, Jobs: ${totalJobs} ` + RESET);

    // Pause queue and trigger cleanup
    await queue.pause();

    // Force garbage collection if available
    if (global.gc) {
      global.gc();
    }

    // Wait for some jobs to complete
    await new Promise(resolve => setTimeout(resolve, 5000));
    // Resume queue
    await queue.resume();
  }
}
setInterval(manageQueueHealth, 10000);