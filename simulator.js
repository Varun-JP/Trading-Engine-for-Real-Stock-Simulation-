import { Worker } from 'worker_threads';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// 4 workers × 125 bots = 500 intelligent bots
// Each worker has all 4 personalities: market makers, noise traders, whales, contrarians
const WORKER_COUNT = 1; //4 for more bots
console.log(`[SIMULATOR] Each worker runs: market makers, noise traders, whales, contrarians`);
console.log(`[SIMULATOR] Direct Kafka publish — circuit breakers + capital limits active\n`);

for (let i = 0; i < WORKER_COUNT; i++) {
    const worker = new Worker(path.join(__dirname, 'botWorker.js'), {
        workerData: {
            workerId:     i + 1,
            kafkaBrokers: process.env.KAFKA_BROKERS,
            kafkaUsername: process.env.KAFKA_USERNAME,
            kafkaPassword: process.env.KAFKA_PASSWORD,
            kafkaCaCert:  process.env.KAFKA_CA_CERT,
        }
    });

    worker.on('message', (msg) => {
        if (msg.type === 'log') console.log(msg.data);
    });

    worker.on('error', (err) => {
        console.error(`Worker ${i + 1} error:`, err.message);
    });

    worker.on('exit', (code) => {
        if (code !== 0) console.error(` Worker ${i + 1} exited with code ${code}`);
    });
}

