import Bottleneck from "bottleneck";
import axios, { AxiosInstance } from "axios";
import axiosRetry, { IAxiosRetryConfig } from "axios-retry";
import { config } from "dotenv";

config()

let limiter: Bottleneck;
let axiosInstance: AxiosInstance;

async function fetchRateLimitFromDatabase() {
  return { rateLimit: Number(process.env.RATE_LIMIT), apiKey: process.env.API_KEY as string };
}

const retryConfig: IAxiosRetryConfig = {
  retries: 3,
  retryDelay: (retryCount, error) => {
    limiter.schedule(() => Promise.resolve());
    if (error.response && (error.response.status >= 400 && error.response.status < 600)) {
      return 1000;
    }
    return axiosRetry.exponentialDelay(retryCount);
  },
  retryCondition: async (error: any) => {
    if (error.response && (error.response.status >= 400 && error.response.status < 600)) {
      return true;
    }
    if (axiosRetry.isNetworkError(error)) {
      return true;
    }
    return false;
  },
};

async function initialize(rateLimit: number) {
  axiosInstance = axios.create({
    timeout: 300000,
  });
  limiter = new Bottleneck({
    minTime: 1000 / rateLimit,
  });
  axiosRetry(axiosInstance, retryConfig);

  console.log(`Limiter initialized with rate limit: ${rateLimit} requests per second`);
}

export { limiter, initialize, axiosInstance };