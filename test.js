const { Worker, Queue } = require('bullmq');

const queue = new Queue('nft-bidding');

const worker = new Worker('nft-bidding', async job => {
    // Process the job
    console.log(`Processing job ${job.id}`);
}, {
    concurrency: 10 // Adjust concurrency based on your needs
});