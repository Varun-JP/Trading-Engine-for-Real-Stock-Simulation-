import mongoose from 'mongoose';
import { createClient } from 'redis';
import dotenv from 'dotenv';
import { Candle, HistoricalTrade } from './model.js';
import connectDB from './db.js';

dotenv.config();

const resetEnvironment = async () => {
    try {
        // 1. Clear Mongo
        await connectDB();
        await Candle.deleteMany({});
        await HistoricalTrade.deleteMany({});
        console.log('💥 MongoDB data successfully purged.');

        // 2. Clear Redis
        const redisClient = createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
        await redisClient.connect();
        await redisClient.flushAll();
        console.log('💥 Redis cache fully flushed.');

        process.exit(0);
    } catch (err) {
        console.error('Error during purge:', err);
        process.exit(1);
    }
};

resetEnvironment();