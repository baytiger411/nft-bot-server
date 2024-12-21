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
import { Queue, Worker, QueueEvents, Job, QueueOptions, JobType } from "bullmq";
import Wallet from "./models/wallet.model";
import redisClient from "./utils/redis";
import { WETH_CONTRACT_ADDRESS, WETH_MIN_ABI } from "./constants";
import { BigNumber, constants, Contract, ethers, utils, Wallet as Web3Wallet } from "ethers";
import { DistributedLockManager } from "./utils/lock";
import { Cluster, Redis } from "ioredis";


const RATE_LIMIT = Number(process.env.RATE_LIMIT)

const redis = redisClient.getClient()

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
  lockPrefix: 'marketplace:',
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
const QUEUE_NAME = '{bull}BIDDING_BOT';

// Add near the top with other constants
const CPU_COUNT = os.cpus().length;
const WORKER_COUNT = Math.max(2, Math.min(CPU_COUNT - 1, 4)); // Use 2-4 workers based on CPU cores

// Add near the top with other constants
const JOB_TIMEOUT = 30000; // 30 seconds
const BATCH_TIMEOUT = 60000; // 60 seconds
const MAX_CONCURRENT_JOBS = Math.ceil(RATE_LIMIT);

// Replace the worker configuration with optimized settings
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
      return result;
    } catch (error) {
      console.error(RED + `Error processing job ${job.id}:`, error, RESET);
      throw error;
    }
  },
  {
    connection: redis,
    prefix: '{bull}',
    // Optimize concurrency and rate limiting
    concurrency: Math.ceil(MAX_CONCURRENT_JOBS),

    lockDuration: 30000,
    stalledInterval: 30000,
    maxStalledCount: 1,

    name: `worker-${index + 1}`
  }
));

// Add queue events monitoring
const queueEvents = new QueueEvents(QUEUE_NAME, {
  connection: redis
});


queueEvents.on('failed', ({ jobId, failedReason }) => {
  console.error(RED + `Job ${jobId} failed: ${failedReason}` + RESET);
});

queueEvents.on('stalled', ({ jobId }) => {
  console.warn(YELLOW + `Job ${jobId} stalled` + RESET);
});

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

// Add worker monitoring
workers.forEach((worker, index) => {


  worker.on('failed', (job, err) => {
    console.error(RED + `✗ Worker ${index + 1} failed job ${job?.id}: ${err.message}` + RESET);
  });

  worker.on('error', error => {
    console.error(RED + `⚠ Worker ${index + 1} encountered error: ${error.message}` + RESET);
  });
});

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
const ALCHEMY_API_KEY = "HGWgCONolXMB2op5UjPH1YreDCwmSbvx"



const OPENSEA_PROTOCOL_ADDRESS = "0x0000000000000068F116a894984e2DB1123eB395"


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


const walletsArr: string[] = []

const MIN_BID_DURATION = 60
// Add near the top with other imports

// Add these functions
function cleanupMemory() {
  if (global.gc) {
    global.gc();
  }
}


const PRIORITIZED_THRESHOLD = RATE_LIMIT * WORKER_COUNT;

async function monitorHealth() {
  try {
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
      // cleanupMemory();

      setTimeout(async () => {
        await queue.resume();
        console.log(GREEN + '✨ Memory cleanup complete, resuming queue... ✨' + RESET);
      }, 5000);
    }

    if (counts.active > PRIORITIZED_THRESHOLD) {
      console.log(YELLOW + '⏸️  PAUSING QUEUE TEMPORARILY DUE TO HIGH LOAD... ⏸️' + RESET);
      await queue.pause();
      setTimeout(async () => {
        await queue.resume();
        console.log(GREEN + '▶️  RESUMING QUEUE AFTER PAUSE... ▶️' + RESET);
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


const HEALTH_CHECK_INTERVAL = 2000;
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
    await initialize();
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
          await queue.clean(0, 0, 'active');
          await queue.clean(0, 0, 'prioritized');
          await queue.clean(0, 0, 'delayed');
          await queue.clean(0, 0, 'failed');
          await queue.clean(0, 0, 'completed');
          await queue.clean(0, 0, 'wait');
          await queue.clean(0, 0, 'paused');
        }
      }

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
  })
// .then(() => {
//   console.log('Hello world!');

//   connectWebSocket().catch(error => {
//     console.error('Failed to connect to WebSocket:', error);
//   })
// })
// .catch(error => {
//   console.error('Failed to start server:', error);
// });

wss.on('connection', async (ws) => {

  // 
  console.log(GREEN + 'New WebSocket connection' + RESET);
  // update task
  ws.onmessage = async (event: WebSocket.MessageEvent) => {
    try {
      const message = JSON.parse(event.data as string);
      switch (message.endpoint) {
        case 'new-task':
          await processNewTask(message.data);
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
        default:
          console.warn(YELLOW + `Unknown endpoint: ${message.endpoint}` + RESET);
      }
    } catch (error) {
      console.error(RED + 'Error handling WebSocket message:' + RESET, error);
    }
  };

  ws.onclose = () => {
    console.log(YELLOW + 'WebSocket connection closed' + RESET);
  };
});


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
  while (activeTasks.size > 0) {
    try {
      // Process each active task
      for (const [taskId, task] of activeTasks.entries()) {
        if (!task.running) continue;

        // Get or initialize lock info
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

// Add near the top with other constants
const BATCH_SEMAPHORE = new Set();

// Add these constants near the top with other constants
const INTER_BATCH_DELAY = 100; // ms between batch processing

// Add these constants near the top with other constants
const BATCH_DELAY = 500; // 500ms between batches
const MAX_QUEUE_SIZE = Math.ceil(RATE_LIMIT * 1.5);


const MAX_ACTIVE_JOBS = WORKER_COUNT * RATE_LIMIT;
const QUEUE_HIGH_WATERMARK = MAX_ACTIVE_JOBS * 1.5; // 150% of max desired active jobs
const QUEUE_LOW_WATERMARK = MAX_ACTIVE_JOBS * 0.75; // 75% of max desired active jobs

// Add these constants near the top with other constants
const MEGA_BATCH_SIZE = 10000; // Process 10k jobs at a time
const BATCH_SIZE = 1000; // Size of each smaller batch within a mega batch
const BATCH_INTERVAL = 1000; // 1 second between batches
const MEGA_BATCH_INTERVAL = 5000; // 5 seconds between mega batches
const MAX_CONCURRENT_BATCHES = 5; // Maximum number of concurrent batch operations

// Add these constants
const MAX_MEMORY_USAGE = 75; // 75% of total memory
const MAX_PRIORITIZED_JOBS = 1000;
const DRAIN_THRESHOLD = 100;
const RATE_LIMIT_PER_SECOND = 2;

async function processBulkJobs(jobs: any[], createKey = false) {
  if (!jobs?.length) return;

  // Initialize rate limiter
  const rateLimiter = new RateLimiter(RATE_LIMIT_PER_SECOND);

  // Process in smaller chunks to prevent memory issues
  const chunks = chunk(jobs, 100);

  for (const [chunkIndex, currentChunk] of chunks.entries()) {
    try {
      // Check memory usage
      const memUsage = process.memoryUsage();
      const memoryUsagePercent = (memUsage.heapUsed / os.totalmem()) * 100;

      if (memoryUsagePercent > MAX_MEMORY_USAGE) {
        console.log(YELLOW + `Memory usage high (${memoryUsagePercent.toFixed(1)}%). Waiting for GC...` + RESET);
        await new Promise(resolve => setTimeout(resolve, 5000));
        global.gc?.();
        continue;
      }

      // Check queue health
      const counts = await queue.getJobCounts();
      const prioritizedCount = counts.prioritized || 0;

      if (prioritizedCount > MAX_PRIORITIZED_JOBS) {
        console.log(YELLOW + `Too many prioritized jobs (${prioritizedCount}). Waiting for queue to drain...` + RESET);
        await waitForQueueDrain();
        continue;
      }

      // Process chunk with rate limiting
      await Promise.all(
        currentChunk.map(async (job) => {
          await rateLimiter.acquire();
          return queue.add(
            job.name,
            job.data,
            {
              ...(createKey && { jobId: createJobKey(job) }),
              ...job.opts,
              removeOnComplete: true,
              removeOnFail: true,
              timeout: JOB_TIMEOUT,
              attempts: 3,
              backoff: {
                type: 'exponential',
                delay: 2000
              }
            }
          );
        })
      );

      console.log(GREEN + `Processed chunk ${chunkIndex + 1}/${chunks.length} (${currentChunk.length} jobs)` + RESET);

      // Small delay between chunks
      await new Promise(resolve => setTimeout(resolve, 1000));

    } catch (error) {
      console.error(RED + `Error processing chunk ${chunkIndex + 1}:`, error, RESET);
    }
  }
}

// Rate limiter class
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


async function processNewTask(task: ITask) {
  try {
    console.log(BLUE + `\n=== Processing New Task ===` + RESET);
    console.log(`Collection: ${task.contract.slug}`);
    console.log(`Task ID: ${task._id}`);

    // Add to activeTasks Map
    activeTasks.set(task._id, task);

    currentTasks.push(task);

    console.log(GREEN + `Added task to currentTasks (Total: ${currentTasks.length})` + RESET);
    const jobs = [
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("opensea") ? [{ name: OPENSEA_SCHEDULE, data: task }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("blur") ? [{ name: BLUR_SCHEDULE, data: task }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("magiceden") ? [{ name: MAGICEDEN_SCHEDULE, data: task }] : []),
    ];

    if (jobs.length > 0) {
      await queue.addBulk(jobs)
      console.log(`Successfully added ${jobs.length} jobs to the queue.`);
    }
    subscribeToCollections([task]);
    console.log(GREEN + `=== New Task Processing Complete ===\n` + RESET);
  } catch (error) {
    console.error(RED + `Error processing new task: ${task.contract.slug}` + RESET, error);
  }
}

async function processUpdatedTask(task: ITask) {
  try {
    const existingTaskIndex = currentTasks.findIndex(t => t._id === task._id);
    if (existingTaskIndex !== -1) {
      currentTasks.splice(existingTaskIndex, 1, task);
      activeTasks.set(task._id, task);
      console.log(YELLOW + `Updated existing task: ${task.contract.slug}`.toUpperCase() + RESET);

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
        jobs.forEach((job) => {
          if (!job?.id) return
          if (blurJobIds.includes(job.id)) {
            job.remove()
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
        jobs.forEach((job) => {
          if (!job?.id) return
          if (openseaJobIds.includes(job.id)) {
            job.remove()
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
        jobs.forEach((job) => {
          if (!job?.id) return
          if (magicedenJobIds.includes(job.id)) {
            job.remove()
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
      console.log(RED + `Attempted to update non-existent task: ${task.contract.slug}` + RESET);
    }
  } catch (error) {
    console.error(RED + `Error processing updated task: ${task.contract.slug}` + RESET, error);
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
    console.error(RED + `Error updating magiceden token jobs: ${task.contract.slug}` + RESET, error);
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
    console.error(RED + `Error updating opensea token jobs: ${task.contract.slug}` + RESET, error);
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
    console.error(RED + `Error updating magiceden trait jobs: ${task.contract.slug}` + RESET, error);
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
    console.error(RED + `Error updating opensea trait jobs: ${task.contract.slug}` + RESET, error);
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
    console.error(RED + `Error updating blur trait jobs: ${task.contract.slug}` + RESET, error);
  }
}

async function startTask(task: ITask, start: boolean) {
  const taskId = task._id.toString();
  try {
    await removeTaskFromAborted(taskId);
    const updatedTask = { ...task, running: true };
    activeTasks.set(taskId, updatedTask);
    const taskIndex = currentTasks.findIndex(t => t._id === task._id);
    if (taskIndex !== -1) {
      currentTasks[taskIndex].running = true;
    }
    console.log(GREEN + `Updated task ${task.contract.slug} running status to: ${start}`.toUpperCase() + RESET);
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
        console.error(RED + `Error subscribing to collection ${task.contract.slug}:` + RESET, error);
      }
    }

    const jobs = [
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("opensea") ? [{ name: OPENSEA_SCHEDULE, data: { ...task, running: start } }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("blur") ? [{ name: BLUR_SCHEDULE, data: { ...task, running: start } }] : []),
      ...(task.selectedMarketplaces.map(m => m.toLowerCase()).includes("magiceden") ? [{ name: MAGICEDEN_SCHEDULE, data: { ...task, running: start } }] : []),
    ];
    await queue.addBulk(jobs);


  } catch (error) {
    console.error(RED + `Error starting task ${taskId}:` + RESET, error);
  }
}


async function stopTask(task: ITask, start: boolean, marketplace?: string) {
  const taskId = task._id.toString();
  try {
    if (!marketplace || task.selectedMarketplaces.length === 0) {
      markTaskAsAborted(taskId);

      // Update activeTasks Map
      const updatedTask = { ...task, running: false };
      activeTasks.set(taskId, updatedTask);
    }

    await updateTaskStatus(task, start, marketplace);

    await cancelAllRelatedBids(task, marketplace)
    await removePendingAndWaitingBids(task, marketplace)
    await waitForRunningJobsToComplete(task, marketplace)

    if (task.outbidOptions.counterbid && !marketplace) {
      try {
        await unsubscribeFromCollection(task);
      } catch (error) {
        console.error(RED + `Error unsubscribing from collection for task ${task.contract.slug}:` + RESET, error);
      }
    }

    const residualBids = await checkForResidualBids(task, marketplace);
    if (residualBids.length > 0) {
      console.warn(YELLOW + `Found ${residualBids.length} residual bids after stopping task` + RESET);
      try {
        await Promise.race([
          cancelAllRelatedBids(task, marketplace),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('Final cleanup timeout')), 30000)
          )
        ]);
      } catch (error) {
        console.error(RED + `Error during final cleanup for task ${task.contract.slug}:` + RESET, error);
      }
    }

    await cancelAllRelatedBids(task, marketplace)

  } catch (error) {
    console.error(RED + `Error stopping task ${task.contract.slug}:` + RESET, error);
    throw error;
  } finally {
  }
}

async function checkForResidualBids(task: ITask, marketplace?: string): Promise<string[]> {
  const patterns = [];
  const taskId = task._id;

  if (!marketplace || marketplace.toLowerCase() === 'opensea') {
    patterns.push(`*:${taskId}:opensea:order:${task.contract.slug}:*`);
  }
  if (!marketplace || marketplace.toLowerCase() === 'blur') {
    patterns.push(`*:${taskId}:blur:order:${task.contract.slug}:*`);
  }
  if (!marketplace || marketplace.toLowerCase() === 'magiceden') {
    patterns.push(`*:${taskId}:magiceden:order:${task.contract.slug}:*`);
  }

  const results = await Promise.all(patterns.map(pattern => redis.keys(pattern)));
  return results.flat();
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

    const BATCH_SIZE = 5000;
    const CONCURRENT_BATCHES = 3;

    let totalJobsRemoved = 0;
    try {
      let start = 0;

      while (true) {
        const batchPromises = Array.from({ length: CONCURRENT_BATCHES }, async (_, i) => {
          const batchStart = start + (i * BATCH_SIZE);
          const jobs: Job[] = await queue.getJobs();

          if (jobs.length === 0) return null;
          Promise.all(jobs.map((job) => {
            if (!job || !job.id) return
            if (jobIds.includes(job?.id)) {
              job.remove()
            }
          }))
          if (jobs.length > 0) return 0;
          return null;
        });

        const batchResults = await Promise.all(batchPromises);
        if (batchResults.every(result => result === null)) break;

        const batchTotal = batchResults.reduce((sum: number, count) =>
          sum + (count === null ? 0 : count), 0
        );
        totalJobsRemoved += batchTotal;

        start += (BATCH_SIZE * CONCURRENT_BATCHES);
      }
    } catch (error) {
      console.error(RED + `Error removing jobs in batch: ${error}` + RESET);
    }
    console.log('=== End Summary ====' + RESET);
    console.log(GREEN + 'Queue resumed after removing pending bids' + RESET);

  } catch (error) {
    console.error(RED + `Error removing pending bids: ${error}` + RESET);
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
      const activeJobs = await queue.getJobs(['active']);
      const relatedJobs = activeJobs?.filter(job => {
        const matchedId = job?.data?._id === task._id
        if (jobnames && jobnames?.length > 0) {
          return matchedId && jobnames?.includes(job?.name);
        }
        return matchedId;
      });

      if (relatedJobs?.length === 0) {
        break;
      }
      await new Promise(resolve => setTimeout(resolve, checkInterval));
    }

    console.log(GREEN + 'All active bids completed...'.toUpperCase() + RESET);

  } catch (error: any) {
    console.error(RED + `Error waiting for running jobs to complete for task ${task.contract.slug}: ${error.message}` + RESET);
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
  console.log(running ? GREEN : RED + `${running ? 'Started' : 'Stopped'} processing task ${task.contract.slug}`.toUpperCase() + RESET);
}

async function cancelAllRelatedBids(task: ITask, marketplace?: string) {
  const { openseaBids, magicedenBids, blurBids } = await getAllRelatedBids(task);
  console.log(YELLOW + `Found bids to cancel for ${task.contract.slug}:`.toUpperCase() + RESET);
  if (openseaBids.length) console.log(`- OpenSea: ${openseaBids.length} bids`.toUpperCase());
  if (magicedenBids.length) console.log(`- MagicEden: ${magicedenBids.length} bids`.toUpperCase());
  if (blurBids.length) console.log(`- Blur: ${blurBids.length} bids`.toUpperCase());

  switch (marketplace?.toLowerCase()) {
    case OPENSEA.toLowerCase():
      await cancelOpenseaBids(openseaBids, task.wallet.privateKey, task.contract.slug, task);
      break;

    case MAGICEDEN.toLowerCase():
      await cancelMagicedenBids(magicedenBids, task.wallet.privateKey, task.contract.slug, task);
      break;

    case BLUR.toLowerCase():
      await cancelBlurBids(blurBids, task.wallet.privateKey);
      break;

    default:
      await cancelOpenseaBids(openseaBids, task.wallet.privateKey, task.contract.slug, task);
      await cancelMagicedenBids(magicedenBids, task.wallet.privateKey, task.contract.slug, task);
      await cancelBlurBids(blurBids, task.wallet.privateKey);
  }
}

async function getAllRelatedBids(task: ITask) {
  let openseaBids: string[] = [];
  let magicedenBids: string[] = [];
  let blurBids: string[] = [];
  const taskId = task._id

  const selectedTraits = transformNewTask(task.selectedTraits);

  if (task.bidType === "token") {
    openseaBids = await redis.keys(`*:${taskId}:opensea:order:${task.contract.slug}:[0-9]*`);
    magicedenBids = await redis.keys(`*:${taskId}:magiceden:order:${task.contract.slug}:[0-9]*`);
    blurBids = await redis.keys(`*:${taskId}:blur:order:${task.contract.slug}:[0-9]*`)
  } else if (task.bidType === "collection" && (!selectedTraits || (selectedTraits && Object.keys(selectedTraits).length === 0))) {
    openseaBids = await redis.keys(`*:${taskId}:opensea:order:${task.contract.slug}:default`);
    magicedenBids = await redis.keys(`*:${taskId}:magiceden:order:${task.contract.slug}:default`);
    blurBids = await redis.keys(`*:${taskId}:blur:order:${task.contract.slug}:default`)
  } else {
    openseaBids = await redis.keys(`*:${taskId}:opensea:order:${task.contract.slug}:*`);
    magicedenBids = await redis.keys(`*:${taskId}:magiceden:order:${task.contract.slug}:*`);
    blurBids = await redis.keys(`*:${taskId}:blur:order:${task.contract.slug}:*`)
  }

  return { openseaBids, magicedenBids, blurBids };
}

async function cancelOpenseaBids(bids: string[], privateKey: string, slug: string, task: ITask) {
  if (bids.length) { console.log(RED + `Found ${bids.length} OpenSea bids to cancel for ${slug}`.toUpperCase() + RESET) }
  const cancelData = bids.map(orderKey => ({
    name: CANCEL_OPENSEA_BID,
    data: { privateKey, orderKey },
    opts: {
      priority: 1
    }
  }));

  await queue.addBulk(cancelData);
}

async function extractMagicedenOrderHash(orderKeys: string[]): Promise<string[]> {
  const bidData = await Promise.all(orderKeys.map(key => redis.get(key)));

  const extractedOrderIds = bidData
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

async function cancelMagicedenBids(orderKeys: string[], privateKey: string, slug: string, task: ITask) {
  if (!orderKeys.length) {
    return;
  }

  for (let i = 0; i < orderKeys.length; i += 1000) {
    const extractedOrderIds = await extractMagicedenOrderHash(orderKeys.slice(i, i + 1000))
    const batch = extractedOrderIds.slice(i, i + 1000);
    console.log(RED + `DELETING  batch ${Math.floor(i / 1000) + 1} of ${Math.ceil(extractedOrderIds.length / 1000)} (${batch.length} MAGICEDEN BIDS)`.toUpperCase() + RESET);
    await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: batch, privateKey, orderKeys: orderKeys.slice(i, i + 1000) }, { priority: 1 });
  }
}

async function cancelBlurBids(bids: string[], privateKey: string) {
  const cancelData = bids.map((orderKey) => {
    return {
      name: CANCEL_BLUR_BID,
      data: { privateKey, orderKey: orderKey },
      opts: {
        priority: 1
      }
    }
  })

  await queue.addBulk(cancelData);
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
    console.error(RED + `Error updating status for task: ${task._id}` + RESET, error);
  }
}

async function unsubscribeFromCollection(task: ITask) {
  try {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      throw new Error(`WebSocket is not open for unsubscribing from collection: ${task.contract.slug}`);
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
        console.log(`Unsubscribed from collection: ${task.contract.slug}`);
        resolve();
      } catch (err) {
        reject(err);
      }
    });
  } catch (error) {
    console.error(RED + `Error in unsubscribeFromCollection:` + RESET, error);
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
      console.log(RED + `Removing marketplace: ${outgoing.toUpperCase()} for collection: ${task.contract.slug}`.toUpperCase() + RESET);
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

      console.log(color + `Adding marketplace: ${incoming.toUpperCase()} for collection: ${task.contract.slug}`.toUpperCase() + RESET);

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
    console.error(RED + `Error updating marketplace for task: ${task._id}` + RESET, error);
  }
}

async function handleOutgoingMarketplace(marketplace: string, task: ITask) {
  try {
    const countKey = `${marketplace}:${task._id}:count`;
    await redis.del(countKey);
    const taskId = task._id;
    await stopTask(task, false, marketplace);

    await new Promise(resolve => setTimeout(resolve, 2000));
    const residualBids = await checkForResidualBids(task, marketplace);

    if (residualBids.length > 0) {
      console.warn(YELLOW + `Found ${residualBids.length} residual bids after marketplace removal, attempting final cleanup...` + RESET);
      await stopTask(task, false, marketplace);
    }

  } catch (error) {
    console.error(RED + `Failed to handle outgoing marketplace ${marketplace} for task ${task.contract.slug}:` + RESET, error);
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
          console.error(RED + `Error starting task ${task.contract.slug}:` + RESET, error);
        }
      }));
    } else {
      await Promise.all(tasks.map(async (task) => {
        try {
          await stopTask(task, false);
        } catch (error) {
          console.error(RED + `Error stopping task ${task.contract.slug}:` + RESET, error);
        }
      }));
    }
  } catch (error) {
    console.error(RED + 'Error updating multiple tasks status:' + RESET, error);
  }
}

async function connectWebSocket(): Promise<void> {
  ws = new WebSocket(MARKETPLACE_WS_URL);

  ws.addEventListener("open", async function open() {
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
        console.log(error);
      }
    });
  });

  ws.addEventListener("close", function close() {
    console.log(RED + "DISCONNECTED FROM MARKETPLACE EVENTS WEBSCKET" + RESET);
    if (heartbeatIntervalId !== null) {
      clearInterval(heartbeatIntervalId);
      heartbeatIntervalId = null;
    }
    attemptReconnect();
  });

  ws.addEventListener("error", function error(err) {
    attemptReconnect()
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
      console.log(`Unknown marketplace: ${message.marketplace}`);
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

      const redisKeyPattern = `*:${task._id}:magiceden:${task.contract.slug}:${tokenId}`;
      const redisKeys = await redis.keys(redisKeyPattern)
      const currentOffers = await Promise.all(redisKeys.map((key) => redis.get(key)))
      const currentBidPrice = !redisKeys.length ? 0 : Math.max(...currentOffers.map((offer) => Number(offer)))


      if (incomingBidAmount < currentBidPrice) return

      offerPrice = Math.ceil((magicedenOutbidMargin * 1e18) + incomingBidAmount)
      if (maxBidPriceEth > 0 && Number(offerPrice / 1e18) > maxBidPriceEth) {
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + `magiceden counter offer ${offerPrice / 1e18} WETH for ${task.contract.slug} ${tokenId}  exceeds max bid price ${maxBidPriceEth} WETH ON MAGICEDEN. Skipping ...`.toUpperCase() + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
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

      const jobId = `${task._id}-${task.contract.slug}-${MAGICEDEN.toLowerCase()}-${tokenId}-counterbid-`
      const job: Job = await queue.getJob(jobId)

      if (job) {
        if (incomingBidAmount <= Number(job.data.offerPrice)) {
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${incomingBidAmount / 1e18} WETH is less than or equal to existing bid ${Number(job.data.offerPrice) / 1e18} WETH Domain: ${domain}` + RESET);
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

      const redisKeyPattern = `*:${task._id}:magiceden:${task.contract.slug}:${JSON.stringify(trait)}`;
      const redisKeys = await redis.keys(redisKeyPattern)
      const currentOffers = await Promise.all(redisKeys.map((key) => redis.get(key)))
      const currentBidPrice = !redisKeys.length ? 0 : Math.max(...currentOffers.map((offer) => Number(offer)))

      if (incomingBidAmount < currentBidPrice) return

      offerPrice = Math.ceil((magicedenOutbidMargin * 1e18) + incomingBidAmount)
      if (maxBidPriceEth > 0 && Number(offerPrice / 1e18) > maxBidPriceEth) {
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + `counter Offer ${offerPrice / 1e18} WETH for ${task.contract.slug} ${JSON.stringify(trait)} exceeds max bid price ${maxBidPriceEth} WETH ON MAGICEDEN. Skipping ...`.toUpperCase() + RESET);
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
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
      const jobId = `counterbid-${task._id}-${task.contract.slug}-${MAGICEDEN.toLowerCase()}-${JSON.stringify(trait)}`
      const job: Job = await queue.getJob(jobId)


      if (job) {
        if (incomingBidAmount <= job.data.offerPrice) {
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${incomingBidAmount / 1e18} WETH is less than or equal to existing bid ${Number(job.data.offerPrice) / 1e18} WETH Domain: ${domain}` + RESET);
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


      const redisKeyPattern = `*:${task._id}:magiceden:${task.contract.slug}:collection`;
      const redisKeys = await redis.keys(redisKeyPattern)
      const currentOffers = await Promise.all(redisKeys.map((key) => redis.get(key)))
      const currentBidPrice = !redisKeys.length ? 0 : Math.max(...currentOffers.map((offer) => Number(offer)))

      if (incomingBidAmount < currentBidPrice) return

      offerPrice = Math.ceil((magicedenOutbidMargin * 1e18) + incomingBidAmount)

      if (maxBidPriceEth > 0 && Number(offerPrice) / 1e18 > maxBidPriceEth) {
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + `counter Offer ${offerPrice / 1e18} WETH for ${task.contract.slug}  exceeds max bid price ${maxBidPriceEth} WETH. Skipping ...`.toUpperCase() + RESET);
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

      const jobId = `counterbid-${task._id}-${task.contract.slug}-${MAGICEDEN.toLowerCase()}-collection`
      const job: Job = await queue.getJob(jobId)

      if (job) {
        if (incomingBidAmount <= job.data.offerPrice) {
          console.log(RED + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `Skipping counterbid since incoming bid ${incomingBidAmount / 1e18} WETH is less than or equal to existing bid ${Number(job.data.offerPrice) / 1e18} WETH Domain: ${domain}` + RESET);
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
    console.error(RED + `Error handling MAGICEDEN counterbid: ${JSON.stringify(error)}` + RESET);
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

      const redisKeyPattern = `*:${task._id}:opensea:${task.contract.slug}:${tokenId}`;
      const redisKeys = await redis.keys(redisKeyPattern)
      const currentOffers = await Promise.all(redisKeys.map((key) => redis.get(key)))
      const currentBidPrice = !redisKeys.length ? 0 : Math.max(...currentOffers.map((offer) => Number(offer)))

      if (incomingBidAmount < currentBidPrice) return

      offerPrice = Math.ceil((openseaOutbidMargin * 1e18) + incomingBidAmount)

      if (maxBidPriceEth > 0 && Number(offerPrice / 1e18) > maxBidPriceEth) {
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + `counter offer ${offerPrice / 1e18} WETH for ${task.contract.slug}:${tokenId}  exceeds max bid price ${maxBidPriceEth} WETH ON OPENSEA. Skipping ...`.toUpperCase() + RESET);
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
      const trait = JSON.stringify({
        type: data.payload.payload.trait_criteria.trait_type,
        value: data.payload.payload.trait_criteria.trait_name
      })

      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);
      console.log(BLUE + `incoming offer for ${task.contract.slug}:${trait} for ${incomingBidAmount / 1e18} WETH on opensea`.toUpperCase() + RESET);
      console.log(BLUE + '---------------------------------------------------------------------------------' + RESET);

      const redisKeyPattern = `*:${task._id}:opensea:${task.contract.slug}:${trait}`;
      const redisKeys = await redis.keys(redisKeyPattern)
      const currentOffers = await Promise.all(redisKeys.map((key) => redis.get(key)))


      const currentBidPrice = !redisKeys.length ? 0 : Math.max(...currentOffers.map((offer) => Number(offer)))

      if (incomingBidAmount < currentBidPrice) return

      offerPrice = Math.ceil((openseaOutbidMargin * 1e18) + incomingBidAmount)


      if (maxBidPriceEth > 0 && Number(offerPrice / 1e18) > maxBidPriceEth) {
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + `counter Offer ${offerPrice / 1e18} WETH for ${task.contract.slug} ${trait}  exceeds max bid price ${maxBidPriceEth} WETH ON OPENSEA. Skipping ...`.toUpperCase() + RESET);
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
        trait
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
      console.log(GREEN + `Counterbidding incoming opensea offer of ${Number(incomingBidAmount) / 1e18} WETH for ${task.contract.slug} ${trait} for ${Number(colletionOffer) / 1e18} WETH ON OPENSEA`.toUpperCase() + RESET);
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

      const redisKeyPattern = `*:${task._id}:opensea:${task.contract.slug}:collection`;
      const redisKeys = await redis.keys(redisKeyPattern)
      const currentOffers = await Promise.all(redisKeys.map((key) => redis.get(key)))
      const currentBidPrice = !redisKeys.length ? 0 : Math.max(...currentOffers.map((offer) => Number(offer)))
      if (incomingBidAmount < currentBidPrice) return
      offerPrice = Math.ceil((openseaOutbidMargin * 1e18) + incomingBidAmount)

      if (maxBidPriceEth > 0 && Number(offerPrice / 1e18) > maxBidPriceEth) {
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + `counter Offer price ${offerPrice / 1e18} WETH for ${task.contract.slug} exceeds max bid price ${maxBidPriceEth} WETH ON OPENSEA. Skipping ...`.toUpperCase() + RESET);
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
        const redisKeyPattern = `*:${task._id}:blur:${task.contract.slug}:${trait}`;
        const redisKeys = await redis.keys(redisKeyPattern)
        const currentOffers = await Promise.all(redisKeys.map((key) => redis.get(key)))
        const currentBidPrice = !redisKeys.length ? 0 : Math.max(...currentOffers.map((offer) => Number(offer)))

        if (incomingPrice <= currentBidPrice) return

        let offerPrice: any = Number((blurOutbidMargin + Number(incomingPrice)).toFixed(2))
        offerPrice = BigNumber.from(utils.parseEther(offerPrice.toString()).toString())

        if (offerPrice > maxBidPriceEth) {
          console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `counter Offer price ${Number(offerPrice) / 1e18} BETH exceeds max bid price ${maxBidPriceEth} BETH ON BLUR. Skipping ...`.toUpperCase() + RESET);
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
        console.log(GREEN + `Counterbidding incoming blur offer of ${Number(incomingPrice)} WETH for ${task.contract.slug} ${trait} for ${Number(offerPrice) / 1e18} WETH ON BLUR`.toUpperCase() + RESET);
        console.log(GREEN + '-------------------------------------------------------------------------------------------------------------------------' + RESET);
        // BLUR_TRAIT_BID_COUNTERBID
      }

    } else {
      const collectionBid = incomingBid?.stats?.filter(item => item.criteriaType.toLowerCase() === "collection") || incomingBid?.updates?.filter(item => item.criteriaType.toLowerCase() === "collection")

      if (!collectionBid.length) return
      const bestPrice = collectionBid?.sort((a, b) => +b.bestPrice - +a.bestPrice)[0].bestPrice || "0"
      const incomingPrice = Number(bestPrice) * 1e18
      const redisKey = `blur:${task.contract.slug}:collection`;

      const redisKeyPattern = `*:${task._id}:blur:${task.contract.slug}:collection`;
      const redisKeys = await redis.keys(redisKeyPattern)
      const currentOffers = await Promise.all(redisKeys.map((key) => redis.get(key)))
      const currentBidPrice = !redisKeys.length ? 0 : Math.max(...currentOffers.map((offer) => Number(offer)))

      if (incomingPrice <= currentBidPrice) return

      const rawPrice = (blurOutbidMargin * 1e18) + Number(incomingPrice)
      const offerPrice = BigInt(Math.round(rawPrice / 1e16) * 1e16)

      if (maxBidPriceEth > 0 && Number(offerPrice) / 1e18 > maxBidPriceEth) {
        console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
        console.log(RED + `counter Offer price ${Number(offerPrice) / 1e18} BETH exceeds max bid price ${maxBidPriceEth} BETH ON BLUR. Skipping ...`.toUpperCase() + RESET);
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

      const offerKey = `${bidCount}:${redisKey}`
      await redis.setex(offerKey, expiry, offerPrice.toString());
    }

  } catch (error) {
    console.error(RED + `Error handling Blur counterbid: ${JSON.stringify(error)}` + RESET);
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
    console.error(`Failed to parse Blur message:`, error);
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

    console.log(BLUE + `Current OPENSEA floor price for ${task.contract.slug}: ${floor_price} ETH`.toUpperCase() + RESET);

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
      const redisKey = `opensea:${task.contract.slug}:collection`
      const currentOfferKeyPattern = `*:${task._id}:opensea:order:${task.contract.slug}:default`

      const marketDataPromise = task.outbidOptions.outbid ?
        fetchOpenseaOffers(
          "COLLECTION",
          task.contract.slug,
          task.contract.contractAddress,
          {}
        ) :
        null;


      const [orderKey, ...remainingOffers] = await redis.keys(currentOfferKeyPattern)
      const ttl = await redis.ttl(orderKey)

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
            console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
            console.log(RED + `❌ Offer price ${offerPriceEth} WETH for ${task.contract.slug} collection bid exceeds max bid price ${maxBidPriceEth} WETH FOR OPENSEA. Skipping ...`.toUpperCase() + RESET);
            console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
            return;
          }

        }
      }
      const bidCount = getIncrementedBidCount(OPENSEA, task.contract.slug, task._id)

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
      await redis.setex(`${bidCount}:${redisKey}`, expiry, offerPrice.toString())
      const totalOffers = [orderKey, ...remainingOffers]
      if (totalOffers.length > 0) {
        const cancelData = remainingOffers.map(orderKey => ({
          name: CANCEL_OPENSEA_BID,
          data: { privateKey: task.wallet.privateKey, orderKey },
          opts: { priority: CANCEL_PRIORITY.OPENSEA }
        }));
        await queue.addBulk(cancelData);
      }
      console.log(GREEN + `✅ Successfully placed bid of ${Number(offerPrice) / 1e18} ETH for ${task.contract.slug}` + RESET);
    }

  } catch (error) {
    console.error(RED + `Error processing OpenSea scheduled bid for task: ${task._id}` + RESET, error);
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
    const outbidMargin = task.outbidOptions.blurOutbidMargin || 0.01
    let floor_price: number = 0

    if (task.bidPrice.minType == "eth" || task.openseaBidPrice.minType === "eth") {
      floor_price = 0
    } else {
      const stats = await fetchBlurCollectionStats(task.contract.slug);
      floor_price = stats.total.floor_price;
    }


    console.log(GOLD + `Current BLUR floor price for ${task.contract.slug}: ${floor_price} ETH`.toUpperCase() + RESET);

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
      const currentOfferKeyPattern = `*:${task._id}:blur:order:${task.contract.slug}:default`

      const marketDataPromise = task.outbidOptions.outbid ?
        fetchBlurBid(task.contract.slug, "COLLECTION", {}) :
        null;

      const [orderKey, ...remainingOffers] = await redis.keys(currentOfferKeyPattern)

      const [ttl, offer] = await Promise.all([
        redis.ttl(orderKey),
        redis.get(orderKey)
      ]);



      if (!task.outbidOptions.outbid) {
        if (ttl >= MIN_BID_DURATION) {
          return;
        }
      }
      else {
        const highestBid = await marketDataPromise;

        const highestBidAmount = Number(highestBid?.priceLevels[0].price) * 1e18

        const redisKeyPattern = `*:${task._id}:blur:${task.contract.slug}:collection`;
        const redisKeys = await redis.keys(redisKeyPattern)
        const currentOffers = await Promise.all(redisKeys.map((key) => redis.get(key)))
        const currentBidPrice = !redisKeys.length ? 0 : Math.max(...currentOffers.map((offer) => Number(offer)))

        if (highestBidAmount > currentBidPrice) {

          if (highestBidAmount) {
            const outbidMargin = (task.outbidOptions.openseaOutbidMargin || 0.0001) * 1e18
            colletionOffer = BigInt(highestBidAmount + outbidMargin)

            const offerPriceEth = Number(colletionOffer) / 1e18;
            if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
              console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
              console.log(RED + `❌ Offer price ${offerPriceEth} BETH for ${task.contract.slug} collection bid exceeds max bid price ${maxBidPriceEth} BETH FOR BLUR. Skipping ...`.toUpperCase() + RESET);
              console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
              return;
            }
          }
        }
      }

      const bidCount = getIncrementedBidCount(BLUR, task.contract.slug, task._id)
      await bidOnBlur(task._id, bidCount, WALLET_ADDRESS, WALLET_PRIVATE_KEY, contractAddress, colletionOffer, task.contract.slug, expiry);

      const totalOffers = [orderKey, ...remainingOffers]

      if (totalOffers.length > 0) {
        const cancelData = totalOffers.map((orderKey) => {
          return {
            name: CANCEL_BLUR_BID,
            data: { privateKey: task.wallet.privateKey, orderKey: orderKey },
            opts: { priority: CANCEL_PRIORITY.BLUR }
          }
        })

        await queue.addBulk(cancelData);
      }
      const redisKey = `blur:${task.contract.slug}:collection`;
      const offerKey = `${bidCount}:${redisKey}`
      await redis.setex(offerKey, expiry, offerPrice.toString());
    }


  } catch (error) {
    console.error(RED + `Error processing Blur scheduled bid for task: ${task._id}` + RESET, error);
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

    // Set up Redis keys for tracking offers
    const redisKey = `opensea:${slug}:${trait}`;
    const currentOfferKeyPattern = `*:${_id}:opensea:order:${slug}:trait:${type}:${value}`

    // Start fetching market data early if outbid is enabled
    const marketDataPromise = outbidOptions.outbid ?
      fetchOpenseaOffers('TRAIT', slug, contractAddress, JSON.parse(trait)) :
      null;

    const [orderKey, ...remainingOffers] = await redis.keys(currentOfferKeyPattern)
    const ttl = await redis.ttl(orderKey)

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION) {
        console.log(GREEN + '✅ Current bid still valid, no action needed' + RESET);
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
        colletionOffer = BigInt(highestBidAmount + outbidMargin)
        const offerPriceEth = Number(colletionOffer) / 1e18;
        if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `❌ Offer price ${offerPriceEth} WETH for ${slug} ${trait} exceeds max bid price ${maxBidPriceEth} WETH FOR OPENSEA. Skipping ...`.toUpperCase() + RESET);
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          return
        }
      }
    }

    const bidCount = getIncrementedBidCount(OPENSEA, slug, _id)
    const totalOffers = [...remainingOffers, orderKey].filter((offer) => offer !== undefined)
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
    await redis.setex(`${bidCount}:${redisKey}`, expiry, offerPrice.toString())
    if (totalOffers.length > 0) {
      const cancelData = totalOffers.map(orderKey => ({
        name: CANCEL_OPENSEA_BID,
        data: { privateKey, orderKey },
        opts: { priority: CANCEL_PRIORITY.OPENSEA }
      }));
      await queue.addBulk(cancelData)
    }
  } catch (error) {
    console.error(RED + `❌ Error processing OpenSea trait bid for task: ${data?.slug}` + RESET, error);
  }
}

async function processOpenseaTokenBid(data: IProcessOpenseaTokenBidData) {
  try {
    const { address, privateKey, slug, offerPrice, creatorFees, enforceCreatorFee, asset, expiry, outbidOptions, maxBidPriceEth, _id } = data
    const currentTask = activeTasks.get(_id)
    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) return;

    let colletionOffer = BigInt(offerPrice)

    const redisKey = `opensea:${slug}:${asset.tokenId}`;
    const currentOfferKeyPattern = `*:${_id}:opensea:order:${slug}:${asset.tokenId}`

    // Start fetching market data early if outbid is enabled
    const marketDataPromise = outbidOptions.outbid ?
      fetchOpenseaOffers("TOKEN", slug, asset.contractAddress, asset.tokenId.toString()) :
      null;

    // Fetch all existing offers from Redis in parallel
    const [orderKey, ...remainingOffers] = await redis.keys(currentOfferKeyPattern)
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
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `❌ Offer price ${offerPriceEth} WETH for ${slug} token ${asset.tokenId} exceeds max bid price ${maxBidPriceEth} WETH FOR OPENSEA. Skipping ...`.toUpperCase() + RESET);
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          return;
        }
      }
    }

    const bidCount = getIncrementedBidCount(OPENSEA, slug, _id)

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
    const cancelData = remainingOffers.map(orderKey => ({
      name: CANCEL_OPENSEA_BID,
      data: { privateKey, orderKey },
      opts: { priority: CANCEL_PRIORITY.OPENSEA }
    }));
    await queue.addBulk(cancelData);
    await redis.setex(`${bidCount}:${redisKey}`, expiry, colletionOffer.toString())
  } catch (error) {
    console.error(RED + `❌ Error processing OpenSea token bid for task: ${data?.slug}` + RESET, error);
  }
}

async function openseaCollectionCounterBid(data: IOpenseaBidParams) {
  const { taskId, bidCount, walletAddress, walletPrivateKey, slug, offerPrice, creatorFees, enforceCreatorFee, expiry } = data

  const currentTask = activeTasks.get(taskId)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) {
    return
  }
  const orderKeyPattern = `*:${taskId}:opensea:order:${slug}:default`
  const orderKeys = await redis.keys(orderKeyPattern)

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

    const cancelData = orderKeys.map(orderKey => ({
      name: CANCEL_OPENSEA_BID,
      data: { privateKey: walletPrivateKey, orderKey },
      opts: { priority: CANCEL_PRIORITY.OPENSEA }
    }));
    await queue.addBulk(cancelData);
    const redisKey = `opensea:${slug}:collection`;
    const offerKey = `${bidCount}:${redisKey}`
    await redis.setex(offerKey, expiry, offerPrice.toString());
  } catch (error) {
    console.error(RED + `❌ Error processing OpenSea collection counter bid for task: ${slug}` + RESET, error);
  }
}

async function magicedenCollectionCounterBid(data: IMagicedenCollectionBidData) {
  const { _id, bidCount, address, contractAddress, quantity, offerPrice, expiration, privateKey, slug } = data
  const currentTask = activeTasks.get(_id)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) {
    return
  }
  try {
    const currentOfferKeyPattern = `*:${_id}:magiceden:order:${slug}:default`
    const remainingOffers = await redis.keys(currentOfferKeyPattern)

    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, offerPrice.toString(), privateKey, slug);

    if (remainingOffers.length > 0) {
      console.log(YELLOW + `⏰ Found ${remainingOffers.length} stale offers, cleaning up...` + RESET);
      const extractedOrderIds = await extractMagicedenOrderHash(remainingOffers)
      await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: privateKey, orderKeys: remainingOffers }, { priority: CANCEL_PRIORITY.MAGICEDEN });
    }
  } catch (error) {
    console.error(RED + `❌ Error processing MagicEden collection counter bid for task: ${slug}` + RESET, error);
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

  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) {
    return
  }

  const triatData = JSON.parse(trait)

  const bidTrait = {
    attributeKey: triatData?.attributeKey,
    attributeValue: triatData?.attributeValue,
  }
  try {
    const currentOfferKeyPattern = `*:${taskId}:magiceden:order:${slug}:${JSON.stringify(trait)}`
    const totalOffers = await redis.keys(currentOfferKeyPattern)

    await bidOnMagiceden(taskId, bidCount, address, contractAddress, quantity, offerPrice.toString(), privateKey, slug, bidTrait);

    if (totalOffers.length > 0) {
      const extractedOrderIds = await extractMagicedenOrderHash(totalOffers)
      if (totalOffers.length > 0) {
        await queue.add(CANCEL_MAGICEDEN_BID, {
          orderIds: extractedOrderIds,
          privateKey,
          orderKeys: totalOffers
        }, {
          priority: CANCEL_PRIORITY.MAGICEDEN
        });
      }
    }

  } catch (error) {
    console.error(RED + `❌ Error processing MagicEden trait counter bid for task: ${slug}` + RESET, error);
  }
}

async function blurCollectionCounterBid(data: IBlurBidData) {
  const { taskId, bidCount, address, privateKey, contractAddress, offerPrice, slug, expiry } = data
  const currentTask = activeTasks.get(taskId)

  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur")) {
    return
  }

  try {
    const orderKeyPattern = `*:${taskId}:blur:order:${slug}:default`
    let remainingOffers = await redis.keys(orderKeyPattern)

    await bidOnBlur(taskId, bidCount, address, privateKey, contractAddress, BigInt(offerPrice), slug, expiry);
    const redisKey = `blur:${slug}:collection`;
    const offerKey = `${bidCount}:${redisKey}`
    await redis.setex(offerKey, expiry, offerPrice.toString());

    remainingOffers = remainingOffers.filter((offer) => offer !== undefined)
    if (remainingOffers.length > 0) {
      const cancelData = remainingOffers.map((orderKey) => {
        return {
          name: CANCEL_BLUR_BID,
          data: { privateKey, orderKey: orderKey },
          opts: { priority: CANCEL_PRIORITY.BLUR }
        }
      })


      await queue.addBulk(cancelData)
    }

  } catch (error) {
    console.error(RED + `❌ Error processing Blur collection counter bid for task: ${slug}` + RESET, error);
  }


}


async function blurTraitCounterBid(data: BlurTraitCounterBid) {
  const { taskId, bidCount, address, privateKey, contractAddress, offerPrice, slug, expiry, trait } = data

  const currentTask = activeTasks.get(taskId)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur")) {
    return
  }
  try {

    const currentOfferKeyPattern = `*:${taskId}:blur:order:${slug}:${trait}`
    let remainingOffers = await redis.keys(currentOfferKeyPattern)
    await bidOnBlur(taskId, bidCount, address, privateKey, contractAddress, BigInt(offerPrice), slug, expiry, trait);

    const redisKey = `blur:${slug}:${trait}`;
    const offerKey = `${bidCount}:${redisKey}`
    await redis.setex(offerKey, expiry, offerPrice.toString());

    remainingOffers = remainingOffers.filter((offer) => offer !== undefined)

    if (remainingOffers.length > 0) {
      const cancelData = remainingOffers.map((orderKey) => {
        return {
          name: CANCEL_BLUR_BID,
          data: { privateKey, orderKey: orderKey },
          opts: { priority: CANCEL_PRIORITY.BLUR }
        }
      })

      await queue.addBulk(cancelData)
    }
  } catch (error) {
    console.error(RED + `❌ Error processing Blur trait counter bid for task: ${data?.slug}` + RESET, error);
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
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("opensea")) {

    return
  }

  const bidCount = getIncrementedBidCount(OPENSEA, slug, _id)
  try {
    const orderKeyPattern = `*:${_id}:opensea:order:${slug}:${asset?.tokenId}`
    const orderKeys = await redis.keys(orderKeyPattern)

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

    const cancelData = orderKeys.map(orderKey => ({
      name: CANCEL_OPENSEA_BID,
      data: { privateKey, orderKey },
      opts: { priority: CANCEL_PRIORITY.OPENSEA }
    }));
    await queue.addBulk(cancelData);

    const redisKey = `opensea:${slug}:${asset?.tokenId}`;
    const offerKey = `${bidCount}:${redisKey}`
    await redis.setex(offerKey, expiry, offerPrice.toString());
  } catch (error) {
    console.log(error);
  }
}

async function openseaTraitCounterBid(data: IProcessOpenseaTraitBidData) {
  try {
    const { _id, bidCount, address, privateKey, slug, offerPrice, creatorFees, enforceCreatorFee, expiry, trait } = data
    const redisKey = `${slug}:${OPENSEA.toLowerCase()}:${trait}`
    const offerKey = `${bidCount}:${redisKey}`

    if (!trait) return

    const traitData = JSON.parse(trait);
    const traitType = traitData.type;
    const traitValue = traitData.value;

    const orderKeyPattern = `*:${_id}:opensea:order:${slug}:trait:${traitType}:${traitValue}`
    const orderKeys = await redis.keys(orderKeyPattern)

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
    await redis.setex(offerKey, expiry, offerPrice.toString())

    const cancelData = orderKeys.map(orderKey => ({
      name: CANCEL_OPENSEA_BID,
      data: { privateKey, orderKey },
      opts: { priority: CANCEL_PRIORITY.OPENSEA }
    }));

    await queue.addBulk(cancelData);
  } catch (error) {
    console.error(RED + `❌ Error processing OpenSea trait counter bid for task: ${data?.slug}` + RESET, error);
  }
}

async function magicedenTokenCounterBid(data: IMagicedenTokenBidData) {
  try {
    const { _id, address, contractAddress, quantity, offerPrice, privateKey, slug, tokenId } = data

    // Validate task is running and marketplace is enabled
    const currentTask = activeTasks.get(_id)

    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) =>
      market.toLowerCase()).includes("magiceden")) {
      return
    }

    // Place new bid
    const bidCount = getIncrementedBidCount(MAGICEDEN, slug, _id)

    const currentOfferKeyPattern = `*:${_id}:magiceden:order:${slug}:${tokenId}`
    const orderKeys = await redis.keys(currentOfferKeyPattern)
    await bidOnMagiceden(
      _id,
      bidCount,
      address,
      contractAddress,
      quantity,
      offerPrice.toString(),
      privateKey,
      slug,
      undefined,
      tokenId
    )
    const extractedOrderIds = await extractMagicedenOrderHash(orderKeys)

    if (orderKeys.length > 0) {
      await queue.add(CANCEL_MAGICEDEN_BID, {
        orderIds: extractedOrderIds,
        privateKey,
        orderKeys
      }, {
        priority: CANCEL_PRIORITY.MAGICEDEN
      });
    }
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

  const currentTask = activeTasks.get(_id)
  if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("blur")) return

  try {
    const currentOfferKeyPattern = `*:${_id}:blur:order:${slug}:${trait}`
    const [orderKey, ...remainingOffers] = await redis.keys(currentOfferKeyPattern)
    const ttl = await redis.ttl(orderKey)

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION) {
        console.log(GREEN + '✅ Current bid still valid, no action needed' + RESET);
        return;
      }
    }
    else {
      const outbidMargin = outbidOptions.blurOutbidMargin || 0.01;
      const bids = await fetchBlurBid(slug, "TRAIT", JSON.parse(trait));
      const highestBids = bids?.priceLevels?.length ? bids.priceLevels.sort((a, b) => +b.price - +a.price)[0].price : 0;
      const bidPrice = Number(highestBids) + outbidMargin;
      collectionOffer = BigInt(Math.ceil(bidPrice * 1e18));
    }

    const offerPriceEth = Number(collectionOffer) / 1e18;

    if (outbidOptions.outbid && maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
      console.log(RED + '--------------------------------------------------------------------------------------------------' + RESET);
      console.log(RED + `Offer price ${offerPriceEth} ETH for ${slug} trait ${trait} exceeds max bid price ${maxBidPriceEth} ETH. Skipping ...`.toUpperCase() + RESET);
      console.log(RED + '--------------------------------------------------------------------------------------------------' + RESET);
      return;
    }
    const bidCount = getIncrementedBidCount(BLUR, slug, _id)

    await bidOnBlur(_id, bidCount, address, privateKey, contractAddress, collectionOffer, slug, expiry, trait);
    const redisKey = `blur:${slug}:${trait}`;
    const offerKey = `${bidCount}:${redisKey}`
    await redis.setex(offerKey, expiry, collectionOffer.toString());

    const totalOffers = [orderKey, ...remainingOffers].filter((item) => item !== undefined)

    if (totalOffers.length > 0) {
      const cancelData = totalOffers.map((orderKey) => {
        return {
          name: CANCEL_BLUR_BID,
          data: { privateKey, orderKey: orderKey },
          opts: { priority: CANCEL_PRIORITY.BLUR }
        }
      })

      await queue.addBulk(cancelData)
    }


  } catch (error) {
    console.error(RED + `Error processing Blur trait bid for task: ${data?.slug}` + RESET, error);
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


    console.log(MAGENTA + `Current magiceden floor price for ${task.contract.slug}: ${floor_price} ETH`.toUpperCase() + RESET);

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
      const currentOfferKeyPattern = `*:${task._id}:magiceden:order:${task.contract.slug}:default`

      const marketDataPromise = task.outbidOptions.outbid ?
        fetchMagicEdenOffer("COLLECTION", task.wallet.address, task.contract.contractAddress) :
        null;

      const [orderKey, ...remainingOffers] = await redis.keys(currentOfferKeyPattern)
      const ttl = await redis.ttl(orderKey)


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


          const outbidMargin = (task.outbidOptions.magicedenOutbidMargin || 0.0001) * 1e18
          colletionOffer = BigInt(highestBidAmount + outbidMargin)

          const offerPriceEth = Number(colletionOffer) / 1e18;
          if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
            console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
            console.log(RED + `❌ Offer price ${offerPriceEth} WETH for ${task.contract.slug} collection bid exceeds max bid price ${maxBidPriceEth} WETH FOR MAGICEDEN. Skipping ...`.toUpperCase() + RESET);
            console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
            return;
          }

        }
      }

      const bidCount = getIncrementedBidCount(MAGICEDEN, task.contract.slug, task._id)
      await bidOnMagiceden(task._id, bidCount, WALLET_ADDRESS, contractAddress, 1, colletionOffer.toString(), WALLET_PRIVATE_KEY, task.contract.slug);

      const totalOffers = [orderKey, ...remainingOffers]
      if (totalOffers.length > 0) {
        const extractedOrderIds = await extractMagicedenOrderHash(totalOffers)
        await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey: task.wallet.privateKey, orderKeys: remainingOffers }, { priority: CANCEL_PRIORITY.MAGICEDEN });
      }
    }
  } catch (error) {
    console.error(RED + `Error processing MagicEden scheduled bid for task: ${task._id}` + RESET, error);
  }
}


async function processMagicedenTokenBid(data: IMagicedenTokenBidData) {
  try {
    const
      {
        contractAddress,
        address,
        quantity,
        offerPrice,
        expiration,
        privateKey,
        slug,
        tokenId,
        outbidOptions,
        maxBidPriceEth,
        _id
      } = data

    const currentTask = activeTasks.get(_id)
    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) return;

    const currentOfferKeyPattern = `*:${_id}:magiceden:order:${slug}:${tokenId}`
    const marketDataPromise = outbidOptions?.outbid ? fetchMagicEdenOffer("TOKEN", address, contractAddress, tokenId.toString()) : null

    const [orderKey, ...remainingOffers] = await redis.keys(currentOfferKeyPattern)
    const ttl = await redis.ttl(orderKey)

    let collectionOffer = Number(offerPrice)

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
        collectionOffer = highestBidAmount + outbidMargin
        const offerPriceEth = Number(collectionOffer) / 1e18;

        if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `❌ Offer price ${offerPriceEth} ETH for ${slug} token ${tokenId} exceeds max bid price ${maxBidPriceEth} ETH FOR MAGICEDEN. Skipping ...`.toUpperCase() + RESET);
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          return;
        }
      }
    }
    const bidCount = getIncrementedBidCount(MAGICEDEN, slug, _id)
    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, collectionOffer.toString(), privateKey, slug, undefined, tokenId)

    const extractedOrderIds = await extractMagicedenOrderHash(remainingOffers)
    await queue.add(CANCEL_MAGICEDEN_BID, { orderIds: extractedOrderIds, privateKey, orderKeys: remainingOffers }, { priority: CANCEL_PRIORITY.MAGICEDEN });
  } catch (error) {
    console.error(RED + `Error processing MagicEden token bid for task: ${data?.slug}` + RESET, error);
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
      { contractAddress,
        address, quantity, offerPrice, expiration, privateKey, slug, trait, _id, outbidOptions, maxBidPriceEth } = data
    const currentTask = activeTasks.get(_id)

    if (!currentTask?.running || !currentTask.selectedMarketplaces.map((market) => market.toLowerCase()).includes("magiceden")) return

    const currentOfferKeyPattern = `*:${_id}:magiceden:order:${slug}:${JSON.stringify(trait)}`

    const marketDataPromise = outbidOptions.outbid ?
      fetchMagicEdenOffer('TRAIT', address, contractAddress, trait) :
      null;

    const [orderKey, ...remainingOffers] = await redis.keys(currentOfferKeyPattern)
    const ttl = await redis.ttl(orderKey)
    let traitOffer = Number(offerPrice)

    if (!outbidOptions.outbid) {
      if (ttl >= MIN_BID_DURATION) {
        console.log(GREEN + '✅ Current bid still valid, no action needed' + RESET);
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

        if (maxBidPriceEth > 0 && offerPriceEth > maxBidPriceEth) {
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          console.log(RED + `❌ Offer price ${offerPriceEth} WETH for ${slug} ${JSON.stringify(trait)} exceeds max bid price ${maxBidPriceEth} WETH FOR MAGICEDEN. Skipping ...`.toUpperCase() + RESET);
          console.log(RED + '-----------------------------------------------------------------------------------------------------------------------------------------' + RESET);
          return
        }
      }
    }

    const bidCount = getIncrementedBidCount(MAGICEDEN, slug, _id)
    await bidOnMagiceden(_id, bidCount, address, contractAddress, quantity, traitOffer.toString(), privateKey, slug, trait)

    const totalOffers = [orderKey, ...remainingOffers].filter((offer) => offer !== undefined)
    if (totalOffers.length > 0) {
      const extractedOrderIds = await extractMagicedenOrderHash(totalOffers)
      if (totalOffers.length > 0) {
        await queue.add(CANCEL_MAGICEDEN_BID, {
          orderIds: extractedOrderIds,
          privateKey,
          orderKeys: totalOffers
        }, {
          priority: CANCEL_PRIORITY.MAGICEDEN
        });
      }
    }
  } catch (error) {
    console.error(RED + `Error processing MagicEden trait bid for task: ${data?.slug}` + RESET, error);
  }
}

async function bulkCancelOpenseaBid(data: { privateKey: string, orderKey: string }) {
  try {
    const { privateKey, orderKey } = data

    const res = await cleanupOSKeys(orderKey)
    if (!res) return

    const { orderHash, offerKey, countKey } = res

    if (orderHash) {
      await Promise.all([
        cancelOrder(orderHash, OPENSEA_PROTOCOL_ADDRESS, privateKey),
        redis.del(offerKey),
        redis.del(orderKey),
        redis.decr(countKey)
      ]);
    } else {
      await Promise.all([
        redis.del(offerKey),
        redis.del(orderKey)
      ]);
    }

  } catch (error) {
    console.log(error);
  }
}

async function cleanupOSKeys(key: string) {
  try {
    let offerKey: string = '';
    let countKey = ''
    const length = key.split(":").length
    if (length === 6) {
      const [bidCount, taskId, marketplace, orderType, slug, identifier] = key.split(':')
      countKey = `opensea:${taskId}:count`
      const uniqueKey = identifier === "default" ? "collection" : identifier;
      offerKey = `${bidCount}:${taskId}:opensea:${slug}:${uniqueKey}`

    } else if (length === 8) {
      const [bidCount, taskId, marketplace, orderType, slug, bidType, type, value] = key.split(':')
      const trait = JSON.stringify({ type: type, value: value })
      offerKey = `${bidCount}:${taskId}:opensea:${slug}:${trait}`
      countKey = `opensea:${taskId}:count`
    }
    const orderHash = await redis.get(key)
    return { orderHash, offerKey, countKey }

  } catch (error) {
    console.log(error);
  }
}

async function cleanupMagicedenKeys(keys: string[]) {
  try {
    await Promise.all(keys.map(async key => {
      let offerKey: string = '';
      let countKey = ''
      const length = key.split(":").length
      if (length === 6) {
        const [bidCount, taskId, marketplace, orderType, slug, identifier] = key.split(':')
        countKey = `magiceden:${taskId}:count`
        const uniqueKey = identifier === "default" ? "collection" : identifier;
        offerKey = `${bidCount}:${taskId}:opensea:${slug}:${uniqueKey}`
        redis.del(offerKey)
        redis.del(key)
        redis.decr(countKey)
      }
    }))

  } catch (error) {
    console.log(error);
  }
}

async function bulkCancelMagicedenBid(data: { orderIds: string[], privateKey: string, orderKeys: string[] }) {
  try {
    const { orderIds, privateKey, orderKeys } = data
    const parsedOrderIds = orderIds.map(orderId => {
      try {
        const parsed = JSON.parse(orderId);
        return parsed.orderId || null;
      } catch {
        return orderId;
      }
    }).filter(id => id !== null);
    if (parsedOrderIds.length > 0) {
      await Promise.all([
        cancelMagicEdenBid(parsedOrderIds, privateKey),
        cleanupMagicedenKeys(orderKeys)
      ])
    }
  } catch (error) {
    console.log(error);
  }
}

async function validateBidCount() {
  try {
    for (const task of currentTasks) {
      const keys = {
        counts: {
          opensea: `opensea:${task._id}:count`,
          blur: `blur:${task._id}:count`,
          magiceden: `magiceden:${task._id}:count`
        },
        patterns: {
          opensea: `*:${task._id}:opensea:order:${task.contract.slug}:*`,
          magiceden: `*:${task._id}:magiceden:order:${task.contract.slug}:*`,
          blur: `*:${task._id}:blur:order:${task.contract.slug}:*`
        }
      };

      const [openseaBids, magicedenBids, blurBids] = await Promise.all([
        redis.keys(keys.patterns.opensea),
        redis.keys(keys.patterns.magiceden),
        redis.keys(keys.patterns.blur)
      ]);

      await Promise.all([
        redis.set(keys.counts.opensea, openseaBids.length),
        redis.set(keys.counts.magiceden, magicedenBids.length),
        redis.set(keys.counts.blur, blurBids.length)
      ]);
    }
  } catch (error) {
    console.log(error);
  }
}

setInterval(validateBidCount, 5000);

async function cleanupBlurKeys(key: string) {
  try {
    let offerKey: string = '';
    let countKey = ''
    const length = key.split(":").length
    if (length === 6) {
      const [bidCount, taskId, marketplace, orderType, slug, identifier] = key.split(':')
      countKey = `blur:${taskId}:count`
      const uniqueKey = identifier === "default" ? "collection" : identifier;
      offerKey = `${bidCount}:${taskId}:blur:${slug}:${uniqueKey}`

    } else if (length === 8) {
      const [bidCount, taskId, marketplace, orderType, slug, bidType, type, value] = key.split(':')
      const trait = JSON.stringify({ type: type, value: value })
      offerKey = `${bidCount}:${taskId}:blur:${slug}:${trait}`
      countKey = `blur:${taskId}:count`
    }
    const cancelPayload = await redis.get(key)
    return { cancelPayload, offerKey, countKey }

  } catch (error) {
    console.log(error);
  }
}

async function blukCancelBlurBid(data: BlurCancelPayload) {
  try {
    const { orderKey, privateKey } = data
    if (!data) return

    const res = await cleanupBlurKeys(orderKey)
    if (!res) return

    const { cancelPayload, offerKey, countKey } = res
    const cancelData = JSON.parse(cancelPayload as string)

    if (cancelData) {
      await Promise.all([
        cancelBlurBid({ payload: cancelData, privateKey }),
        redis.del(offerKey),
        redis.del(orderKey),
        redis.decr(countKey)
      ]);
    } else {
      await Promise.all([
        redis.del(offerKey),
        redis.del(orderKey)
      ]);
    }
  } catch (error) {
    console.log(error);
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
  const countKey = `${marketplace}:${slug}`;
  const currentCount = bidCounts.get(countKey) || 0;
  const newCount = currentCount + 1;
  bidCounts.set(countKey, newCount);
  return `${newCount}:${taskId}`;
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
      const subscriptionKey = `${task.contract.slug}`;

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
        console.error(RED + `Failed to open WebSocket after ${maxRetries} retries for: ${task.contract.slug}` + RESET);
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

        const intervalKey = `opensea:${task.contract.slug}`;
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

        const intervalKey = `magiceden:${task.contract.slug}`;
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

        const intervalKey = `blur:${task.contract.slug}`;
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
  const lockKey = `approve:${task.wallet.address.toLowerCase()}:${marketplace.toLowerCase()}`;

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
        console.error(RED + `Error: Wallet ${task.wallet.address} could not approve ${name} as a spender. Please ensure your wallet has enough ETH to cover the gas fees and permissions are properly set.`.toUpperCase() + RESET);
      } else {
        console.error(RED + `Error details:`, error);
        console.error(`Error message: ${error.message}`);
        console.error(`Error code: ${error.code}`);
        console.error(RED + `Error: Wallet ${task.wallet.address} could not approve the ${name} as a spender. Task has been stopped.`.toUpperCase() + RESET);
      }
      return false;
    }
  }) ?? false;
}

// In the hasActivePrioritizedJobs function:
async function hasActivePrioritizedJobs(task: ITask): Promise<boolean> {

  const jobs = await queue.getJobs(['prioritized', 'active']);
  const baseKey = `${task._id}-${task.contract?.slug}`;

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

// Add near the top with other constants
// Add these helper functions
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

interface OpenseaCounterBidJobData {
  _id: string;
  bidCount: string;
  address: string;
  privateKey: string;
  slug: string;
  colletionOffer: number;
  creatorFees: IFee;
  enforceCreatorFee: any;
  expiry: number;
  asset?: {
    contractAddress: string;
    tokenId: number;
  };
  trait?: string;
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

const BID_WINDOW_SECONDS = 60; // Track bids over a 60 second window

export function trackBidRate(marketplace: 'opensea' | 'magiceden' | 'blur') {
  const now = Math.floor(Date.now() / 1000);
  const cutoff = now - BID_WINDOW_SECONDS;

  // Add new bid to the appropriate marketplace
  bidRateState[marketplace].bids.push({
    timestamp: now,
    count: 1
  });

  // Remove entries older than our window
  bidRateState[marketplace].bids = bidRateState[marketplace].bids.filter(bid =>
    bid.timestamp > cutoff
  );

  // Calculate total bids in window
  const totalBids = bidRateState[marketplace].bids.reduce((sum, bid) => sum + bid.count, 0);

  // Calculate rate (bids per second) over the window period
  const rate = totalBids / Math.min(BID_WINDOW_SECONDS, now - (bidRateState[marketplace].bids[0]?.timestamp || now));

  console.log(`Current ${marketplace} bid rate: ${rate.toFixed(2)} bids/second (${totalBids} bids in last ${BID_WINDOW_SECONDS}s)`);
  return rate;
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
    console.log(RED + `⚠️ Queue health check triggered - Memory: ${memoryUsagePercent.toFixed(2)}%, Jobs: ${totalJobs}` + RESET);

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