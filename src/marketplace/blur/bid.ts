import { BigNumber, ethers, utils, Wallet } from "ethers";
import { axiosInstance, limiter } from "../../init";
import { BLUR_SCHEDULE, BLUR_TRAIT_BID, currentTasks, decrementBidCount, errorStats, queue, redis, RESET, trackBidRate } from "../..";
import { config } from "dotenv";
import { createBalanceChecker } from "../../utils/balance";
import { Job } from "bullmq";
import { DistributedLockManager } from "../../utils/lock";
const RED = '\x1b[31m';

config()
const API_KEY = process.env.API_KEY as string;
const BLUR_API_URL = 'https://api.nfttools.website/blur';
const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY as string;

const deps = {
  redis: redis,
  provider: new ethers.providers.AlchemyProvider("mainnet", ALCHEMY_API_KEY),
};
const balanceChecker = createBalanceChecker(deps);
const provider = new ethers.providers.AlchemyProvider('mainnet', ALCHEMY_API_KEY);

const lockManager = new DistributedLockManager({
  lockPrefix: '{blur}:lock:',
  defaultTTLSeconds: 60 // 1 minute lock timeout
});

/**
 * Creates an offer on Blur.
 * @param walletAddress - The wallet address of the offerer.
 * @param privateKey - The private key of the offerer's wallet.
 * @param contractAddress - The contract address of the collection.
 * @param offerPrice - The price of the offer in wei.
 * @param traits - Optional traits for the offer.
 */
export async function bidOnBlur(
  taskId: string,
  bidCount: string,
  wallet_address: string,
  private_key: string,
  contractAddress: string,
  offer_price: BigNumber | bigint,
  slug: string,
  expiry = 900,
  traits?: string
) {
  const bethBalance = await balanceChecker.getBethBalance(wallet_address);
  let offerPriceEth: string | number = (Number(offer_price) / 1e18)

  if (offerPriceEth > bethBalance) {
    console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
    console.log(RED + `Offer price: ${offerPriceEth} BETH  is greater than available WETH balance: ${bethBalance} BETH. SKIPPING ...`.toUpperCase() + RESET);
    console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
    return
  }

  const offerPrice = BigNumber.from(offer_price.toString());
  const accessToken = await getAccessToken(BLUR_API_URL, private_key);

  offerPriceEth = (Math.floor(Number(utils.formatUnits(offerPrice)) * 100) / 100).toFixed(2);

  if (Number(offerPriceEth) === 0) {
    console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
    console.log(RED + `Offer price is less than the minimum Blur offer price. SKIPPING ...`.toUpperCase() + RESET);
    console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
    return
  }

  const wallet = new Wallet(private_key, provider);
  const basePayload = {
    price: {
      unit: 'BETH',
      amount: offerPriceEth,
    },
    quantity: 1,
    expirationTime: new Date(Date.now() + (expiry * 1000)).toISOString(),
    contractAddress: contractAddress,
  };

  const buildPayload = traits ? {
    ...basePayload,
    criteria: {
      type: "TRAIT",
      value: JSON.parse(traits)
    }
  } : basePayload;

  let build: any;
  try {
    if (!accessToken) {
      throw new Error('Access token is undefined');
    }
    build = await formatBidOnBlur(BLUR_API_URL, accessToken, wallet_address, buildPayload);

  } catch (error: any) {
    if (!errorStats[taskId]) {
      errorStats[taskId] = {
        magiceden: 0,
        opensea: 0,
        blur: 0
      }
    }
    errorStats[taskId]['blur']++
    return
  }

  let data = build?.signatures?.[0];
  if (!data) {
    build = await formatBidOnBlur(BLUR_API_URL, accessToken, wallet_address, buildPayload);
    data = build?.signatures?.[0];
  }
  if (!data) {
    return;
  }

  const signObj = await wallet._signTypedData(
    data?.signData?.domain,
    data?.signData?.types,
    data?.signData?.value
  );

  const submitPayload = {
    signature: signObj,
    marketplaceData: data?.marketplaceData,
  };

  try {
    const cancelPayload = {
      contractAddress,
      criteriaPrices: [
        {
          price: offerPriceEth,
          criteria: {
            "type": traits ? "TRAIT" : "COLLECTION",
            value: traits ? JSON.parse(traits) : {}
          }
        }
      ]
    }

    await submitBidToBlur(taskId, bidCount, offer_price, BLUR_API_URL, accessToken, wallet_address, submitPayload, slug, cancelPayload, expiry, traits);

  } catch (error: any) {
    console.log("blur post offer error: ", error?.response?.data || error?.message);
    if (!errorStats[taskId]) {
      errorStats[taskId] = {
        magiceden: 0,
        opensea: 0,
        blur: 0
      }
    }
  }
};

/**
 * Gets an access token.
 * @param url - The URL to get the access token from.
 * @param privateKey - The private key of the wallet.
 * @returns The access token.
 */
async function getAccessToken(url: string, private_key: string): Promise<string | undefined> {
  const wallet = new Wallet(private_key, provider);
  const lockKey = `auth:${wallet.address}`;

  return await lockManager.withLock(lockKey, async () => {
    const options = { walletAddress: wallet.address };
    const headers = {
      'content-type': 'application/json',
      'X-NFT-API-Key': API_KEY
    };

    try {
      const key = `{blurAccessToken}:${wallet.address}`
      const cachedToken = await redis.get(key);
      if (cachedToken) {
        return cachedToken;
      }

      let response: any = await limiter.schedule(() => axiosInstance
        .post(`${url}/auth/challenge`, options, { headers }));
      const message = response.data.message;
      const signature = await wallet.signMessage(message);
      const data = {
        message: message,
        walletAddress: wallet.address,
        expiresOn: response.data.expiresOn,
        hmac: response.data.hmac,
        signature: signature
      };
      response = await limiter.schedule(() => axiosInstance
        .post(`${url}/auth/login`, data, { headers }));
      const accessToken = response.data.accessToken;
      await redis.set(key, accessToken, 'EX', 5 * 60);

      return accessToken;
    } catch (error: any) {
      throw error
    }
  });
};

/**
 * Sends a request to format a bid on Blur.
 * @param url - The URL to send the request to.
 * @param accessToken - The access token for authentication.
 * @param walletAddress - The wallet address of the offerer.
 * @param buildPayload - The payload for the bid.
 * @returns The formatted bid data.
 */
async function formatBidOnBlur(
  url: string,
  accessToken: string,
  walletAddress: string,
  buildPayload: any
) {
  try {
    const { data } = await limiter.schedule(() =>
      axiosInstance.request<BlurBidResponse>({
        method: 'POST',
        url: `${url}/v1/collection-bids/format`,
        headers: {
          'content-type': 'application/json',
          authToken: accessToken,
          walletAddress: walletAddress.toLowerCase(),
          'X-NFT-API-Key': API_KEY,
        },
        data: JSON.stringify(buildPayload),
      })
    );
    return data;
  } catch (error: any) {
    if (error.response?.data?.message === 'Balance over-utilized' || error.message?.message === 'Balance over-utilized') {
      console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
      console.log(RED + 'BALANCE OVER-UTILIZED: BETH balance is being used in too many active orders' + RESET);
      console.log(RED + '-----------------------------------------------------------------------------------------------------------' + RESET);
      return;
    }
    throw error
  }
}

/**
 * Submits a bid to Blur.
 * @param url - The URL to send the request to.
 * @param accessToken - The access token for authentication.
 * @param walletAddress - The wallet address of the offerer.
 * @param submitPayload - The payload for the bid submission.
 * @param slug - The slug of the collection.
 */
async function submitBidToBlur(
  taskId: string,
  bidCount: string,
  offer_price: BigNumber | bigint,
  url: string,
  accessToken: string,
  walletAddress: string,
  submitPayload: SubmitPayload,
  slug: string,
  cancelPayload: any,
  expiry = 900,
  traits?: string
) {
  try {
    let running = currentTasks.find((task) => task.contract.slug.toLowerCase() === slug.toLowerCase())
    if (!running) return
    const { data: offers } = await limiter.schedule(() =>
      axiosInstance.request({
        method: 'POST',
        url: `${url}/v1/collection-bids/submit`,
        headers: {
          'content-type': 'application/json',
          authToken: accessToken,
          walletAddress: walletAddress.toLowerCase(),
          'X-NFT-API-Key': API_KEY,
        },
        data: JSON.stringify(submitPayload),
      })
    );


    const successMessage = traits ? `🎉 TRAIT OFFER POSTED TO BLUR SUCCESSFULLY FOR: ${slug.toUpperCase()} 🎉 TRAIT: ${traits}` : `🎉 OFFER POSTED TO BLUR SUCCESSFULLY FOR: ${slug.toUpperCase()} 🎉`

    if (offers.errors) {
      console.error('Error:', JSON.stringify(offers.errors));
    } else {
      console.log("\x1b[33m", successMessage, RESET);

      let identifier = ''
      if (traits) {
        const traitsObj = JSON.parse(traits);
        const [traitType, traitValue] = Object.entries(traitsObj)[0];
        identifier = `${traitType}:${traitValue}`
      } else {
        identifier = "collection"
      }
      const [taskId, count] = bidCount.split(":")
      const orderTrackingKey = `{${taskId}}:blur:orders`;
      const orderKey = `{${taskId}}:${count}:blur:order:${slug}:${identifier}`;

      const order = JSON.stringify({
        offer: offer_price.toString(),
        payload: cancelPayload
      })

      await Promise.all([
        redis.setex(orderKey, expiry, order),
        redis.sadd(orderTrackingKey, orderKey),
        redis.expire(orderTrackingKey, expiry)
      ]);

      trackBidRate("blur", taskId)
    }
  } catch (error: any) {
    if (error.response?.data?.message?.message === 'Balance over-utilized' || error.message.message === 'Balance over-utilized') {
      const jobs: Job[] = await queue.getJobs(['prioritized']);
      const blurJobs = jobs.filter(job =>
        [BLUR_SCHEDULE, BLUR_TRAIT_BID].includes(job.name)
      );

      if (blurJobs.length > 0) {
        await queue.pause()
        await Promise.all(blurJobs.map(job => job.remove()));
        console.log(RED + `REMOVING ${blurJobs.length} BLUR JOB(S) DUE TO INSUFFICIENT BETH BALANCE` + RESET);
        await queue.resume()
      }
    } else {
      console.error("Error submitting bid:", error.response?.data || error.message);
    }
    throw error
  }
}

export async function cancelBlurBid(data: BlurCancelPayload) {
  try {
    if (!data || !data.payload || !data.privateKey) return
    const { payload, privateKey, taskId } = data
    const wallet = new Wallet(privateKey, provider);
    const walletAddress = wallet.address
    const accessToken = await getAccessToken(BLUR_API_URL, privateKey);
    const endpoint = `${BLUR_API_URL}/v1/collection-bids/cancel`
    const { data: cancelResponse } = await limiter.schedule(() => axiosInstance.post(endpoint, payload, {
      headers: {
        'content-type': 'application/json',
        authToken: accessToken,
        walletAddress: walletAddress.toLowerCase(),
        'X-NFT-API-Key': API_KEY,
      }
    }))
    decrementBidCount('blur', taskId);
    console.log(JSON.stringify(cancelResponse));

  } catch (error: any) {
    if (error.response?.data?.message?.message !== 'No bids found') {
      console.log("cancelBlurBid: ", error?.response?.data || error);
    }
    console.log(error.response.data || error.message);
  }
}

export async function fetchBlurBid(collection: string, criteriaType: 'TRAIT' | 'COLLECTION', criteriaValue: Record<string, string>) {
  const url = `https://api.nfttools.website/blur/v1/collections/${collection}/executable-bids`;
  try {
    const { data } = await limiter.schedule(() => axiosInstance.get<BlurBidResponse>(url, {
      params: {
        filters: JSON.stringify({
          criteria: {
            type: criteriaType,
            value: criteriaValue
          }
        })
      },
      headers: {
        'content-type': 'application/json',
        'X-NFT-API-Key': API_KEY,
      }
    }));

    return data;
  } catch (error: any) {
    console.error("Error fetching executable bids:", error.response?.data || error.message);
  }
}

export async function fetchBlurCollectionStats(slug: string) {
  const lockKey = `blur:stats:${slug}`;

  return await lockManager.withLock(lockKey, async () => {
    const URL = `https://api.nfttools.website/blur/v1/collections/${slug}`;
    try {
      const { data } = await limiter.schedule(() => axiosInstance.get(URL, {
        headers: {
          'content-type': 'application/json',
          'X-NFT-API-Key': API_KEY,
        }
      }));
      return data?.collection?.floorPrice?.amount || 0
    } catch (error: any) {
      console.error("Error fetching collection data:", error.response?.data || error.message);
      return 0
    }
  });
}


interface PriceLevel {
  criteriaType: string;
  criteriaValue: Record<string, unknown>;
  price: string;
  executableSize: number;
  numberBidders: number;
  bidderAddressesSample: any[];
}

interface BlurBidResponse {
  success: boolean;
  priceLevels: PriceLevel[];
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
  taskId: string;
}

interface BlurBidResponse {
  success: boolean;
  signatures: Signature[];
  [key: string]: any;
}

interface Signature {
  type: string;
  signData: SignData;
  marketplace: string;
  marketplaceData: string;
  tokens: any[];
  [key: string]: any;

}

interface SignData {
  domain: Domain;
  types: Types;
  value: Value;
  [key: string]: any;

}

interface Domain {
  name: string;
  version: string;
  chainId: string;
  verifyingContract: string;
  [key: string]: any;
}

interface Types {
  Order: OrderType[];
  FeeRate: FeeRateType[];
  [key: string]: any;

}
interface OrderType {
  name: string;
  type: string;
  [key: string]: any;

}

interface FeeRateType {
  name: string;
  type: string;
  [key: string]: any;

}

interface Value {
  trader: string;
  collection: string;
  listingsRoot: string;
  numberOfListings: number;
  expirationTime: string;
  assetType: number;
  makerFee: MakerFee;
  salt: string;
  orderType: number;
  nonce: Nonce;
  [key: string]: any;

}

interface MakerFee {
  recipient: string;
  rate: number;
  [key: string]: any;
}

interface Nonce {
  type: string;
  hex: string;
  [key: string]: any;

}

interface SubmitPayload {
  signature: string;
  marketplaceData: string[];
  [key: string]: any;
}
