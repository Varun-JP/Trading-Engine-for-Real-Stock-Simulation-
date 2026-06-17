import { Kafka } from 'kafkajs';
import { createClient } from 'redis';
import { HistoricalTrade } from './model.js';
import connectDB from './db.js';
import dotenv from 'dotenv';

dotenv.config();

// ── KAFKA ─────────────────────────────────────────────────────────────────────
const kafka = new Kafka({
    clientId: 'matching-engine',
    brokers: [process.env.KAFKA_BROKERS || 'localhost:9092']
});

// Engine consumes order-requests, publishes to completed-trades AND trade-fills
const consumer = kafka.consumer({ groupId: 'matching-engine-group' });
const producer  = kafka.producer();

// ── REDIS ─────────────────────────────────────────────────────────────────────
const redisClient = createClient({
    url: process.env.REDIS_URL || 'redis://localhost:6379'
});

redisClient.on('error', (err) => console.error('[ENGINE] Redis error:', err));

// ── BOOT ──────────────────────────────────────────────────────────────────────
const startEngine = async () => {
    try {
        await connectDB();
        await redisClient.connect();
        console.log('[ENGINE] ✅  Redis connected');

        await producer.connect();
        await consumer.connect();
        console.log('[ENGINE] ✅  Kafka producer + consumer connected');

        await consumer.subscribe({ topic: 'order-requests', fromBeginning: true });
        console.log('[ENGINE] 🚀  Matching engine live — consuming order-requests\n');

        await consumer.run({
            eachMessage: async ({ message }) => {
                const incomingOrder = JSON.parse(message.value.toString());
                const { orderId, userId, ticker, side, price, quantity } = incomingOrder;

                console.log(`\n📦 [ENGINE] ${side} ${ticker} | Qty ${quantity} @ ₹${price}`);

                const buyBookKey  = `orders:${ticker}:BUY`;
                const sellBookKey = `orders:${ticker}:SELL`;
                let remainingQty  = quantity;

                try {
                    if (side === 'BUY') {
                        while (remainingQty > 0) {
                            const bestSells = await redisClient.zRangeWithScores(sellBookKey, 0, 0);
                            if (bestSells.length === 0) break;

                            const cheapestSellOrder = JSON.parse(bestSells[0].value);
                            const cheapestSellPrice = bestSells[0].score;

                            if (price >= cheapestSellPrice) {
                                const matchQty = Math.min(remainingQty, cheapestSellOrder.quantity);
                                console.log(`   ✅  MATCH: ${matchQty} shares of ${ticker} @ ₹${cheapestSellPrice}`);

                                // 1. Notify chart engine
                                await producer.send({
                                    topic: 'completed-trades',
                                    messages: [{
                                        key: ticker,
                                        value: JSON.stringify({
                                            ticker,
                                            price:    parseFloat(cheapestSellPrice),
                                            quantity: parseInt(matchQty),
                                            timestamp: new Date().toISOString()
                                        })
                                    }]
                                });

                                // 2. Notify server.js → Socket.io via trade-fills topic
                                await producer.send({
                                    topic: 'trade-fills',
                                    messages: [{
                                        key: ticker,
                                        value: JSON.stringify({
                                            buyerUserId:  userId,
                                            sellerUserId: cheapestSellOrder.userId,
                                            ticker,
                                            price:    parseFloat(cheapestSellPrice),
                                            quantity: parseInt(matchQty),
                                            timestamp: new Date().toISOString()
                                        })
                                    }]
                                });

                                remainingQty -= matchQty;
                                cheapestSellOrder.quantity -= matchQty;

                                // 3. Persist to MongoDB
                                await Promise.all([
                                    new HistoricalTrade({ userId, ticker, price: cheapestSellPrice, quantity: matchQty, side: 'BUY' }).save(),
                                    new HistoricalTrade({ userId: cheapestSellOrder.userId, ticker, price: cheapestSellPrice, quantity: matchQty, side: 'SELL' }).save()
                                ]);

                                // 4. Update Redis order book
                                await redisClient.zRem(sellBookKey, bestSells[0].value);
                                if (cheapestSellOrder.quantity > 0) {
                                    await redisClient.zAdd(sellBookKey, { score: cheapestSellPrice, value: JSON.stringify(cheapestSellOrder) });
                                }
                            } else {
                                break;
                            }
                        }

                        if (remainingQty > 0) {
                            const partialOrder = { orderId, userId, ticker, side, price, quantity: remainingQty };
                            await redisClient.zAdd(buyBookKey, { score: price, value: JSON.stringify(partialOrder) });
                        }

                    } else if (side === 'SELL') {
                        while (remainingQty > 0) {
                            const bestBuys = await redisClient.zRangeWithScores(buyBookKey, -1, -1);
                            if (bestBuys.length === 0) break;

                            const highestBuyOrder = JSON.parse(bestBuys[0].value);
                            const highestBuyPrice = bestBuys[0].score;

                            if (price <= highestBuyPrice) {
                                const matchQty = Math.min(remainingQty, highestBuyOrder.quantity);
                                console.log(`   ✅  MATCH: ${matchQty} shares of ${ticker} @ ₹${highestBuyPrice}`);

                                await producer.send({
                                    topic: 'completed-trades',
                                    messages: [{
                                        key: ticker,
                                        value: JSON.stringify({
                                            ticker,
                                            price:    parseFloat(highestBuyPrice),
                                            quantity: parseInt(matchQty),
                                            timestamp: new Date().toISOString()
                                        })
                                    }]
                                });

                                await producer.send({
                                    topic: 'trade-fills',
                                    messages: [{
                                        key: ticker,
                                        value: JSON.stringify({
                                            buyerUserId:  highestBuyOrder.userId,
                                            sellerUserId: userId,
                                            ticker,
                                            price:    parseFloat(highestBuyPrice),
                                            quantity: parseInt(matchQty),
                                            timestamp: new Date().toISOString()
                                        })
                                    }]
                                });

                                remainingQty -= matchQty;
                                highestBuyOrder.quantity -= matchQty;

                                await Promise.all([
                                    new HistoricalTrade({ userId, ticker, price: highestBuyPrice, quantity: matchQty, side: 'SELL' }).save(),
                                    new HistoricalTrade({ userId: highestBuyOrder.userId, ticker, price: highestBuyPrice, quantity: matchQty, side: 'BUY' }).save()
                                ]);

                                await redisClient.zRem(buyBookKey, bestBuys[0].value);
                                if (highestBuyOrder.quantity > 0) {
                                    await redisClient.zAdd(buyBookKey, { score: highestBuyPrice, value: JSON.stringify(highestBuyOrder) });
                                }
                            } else {
                                break;
                            }
                        }

                        if (remainingQty > 0) {
                            const partialOrder = { orderId, userId, ticker, side, price, quantity: remainingQty };
                            await redisClient.zAdd(sellBookKey, { score: price, value: JSON.stringify(partialOrder) });
                        }
                    }

                } catch (dbError) {
                    console.error('[ENGINE] ❌  Matching loop fault:', dbError.message);
                }
            }
        });

    } catch (err) {
        console.error('[ENGINE] Critical startup failure:', err);
        process.exit(1);
    }
};

startEngine();