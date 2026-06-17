import express from 'express';
import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';
import http from 'http';
import { Server } from 'socket.io';
import { User, Candle } from './model.js';
import connectDB from './db.js';
import client from 'prom-client';


dotenv.config();

const app    = express();
const PORT   = process.env.PORT || 5000;
const server = http.createServer(app);

// ----- SOCKET.IO ---------------------------
const io = new Server(server, {
    cors: { origin: '*' }
});

//-----------PROMETHEUS-----------------------
const register = new client.Registry();
client.collectDefaultMetrics({ register });
export const circuitBreakerCounter = new client.Counter({
    name : 'nexus_circuit_breaker_trips_total',
    help : 'Number of times circuit breaker has tripped',
    labelNames : ['ticker', 'side'],
    registers: [register]
});
// Force initialize combinations to 0 so they appear in Prometheus on boot
const supportedTickers = ['INFY', 'RELIANCE', 'TCS', 'HDFCBANK'];
supportedTickers.forEach(ticker => {
    circuitBreakerCounter.labels({ ticker, side: 'UPPER' }).inc(0);
    circuitBreakerCounter.labels({ ticker, side: 'LOWER' }).inc(0);
});

io.use(async (socket, next) => {
    try {
        const apiKey = socket.handshake.auth.token;
        if (!apiKey) return next(new Error('API key missing'));

        const user = await User.findOne({ apiKey });
        if (!user)  return next(new Error('Invalid API key'));

        socket.userId = user._id.toString();
        socket.join(socket.userId);
        next();
    } catch (err) {
        console.error('Socket auth middleware error', err.message);
        next(new Error('Authentication error'));
    }
});

app.use(express.json());
app.use(express.static('public'));

// ── KAFKA ─────────────────────────────────────────────────────────────────────
const kafka = new Kafka({
    clientId: 'api-server',
    brokers: [process.env.KAFKA_BROKERS],
    ssl: {
        ca: [process.env.KAFKA_CA_CERT],
    },
    sasl: {
        mechanism: 'plain',
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD,
    }
});

const producer       = kafka.producer();

// Consumes trade-fills FROM engine.js → emits Socket.io events to users
const fillConsumer   = kafka.consumer({ groupId: 'fill-broadcast-group' });

// Consumes candle updates FROM chartEngine.js → streams to browsers
const candleConsumer = kafka.consumer({ groupId: 'candle-broadcast-group' });

// ── CIRCUIT BREAKER ───────────────────────────────────────────────────────────
const CIRCUIT_STATE = {
    INFY:     { dailyOpen: null, haltUntil: null },
    RELIANCE: { dailyOpen: null, haltUntil: null },
    TCS:      { dailyOpen: null, haltUntil: null },
    HDFCBANK: { dailyOpen: null, haltUntil: null },
};

const CIRCUIT_LIMIT_PCT = 0.15;
const HALT_DURATION_MS  = 10_000;

function enforceCircuitBreaker(ticker, price) {
    const state = CIRCUIT_STATE[ticker];
    if (!state) return null;

    if (state.dailyOpen === null) { state.dailyOpen = price; return null; }

    if (state.haltUntil && Date.now() < state.haltUntil) {
        const resumesIn = Math.ceil((state.haltUntil - Date.now()) / 1000);
        return `Circuit breaker active — ${ticker} trading halted for ${resumesIn}s`;
    }

    if (state.haltUntil && Date.now() >= state.haltUntil) {
        state.haltUntil = null;
        state.dailyOpen = price;
    }

    const move = Math.abs(price - state.dailyOpen) / state.dailyOpen;
    if (move >= CIRCUIT_LIMIT_PCT) {
        state.haltUntil = Date.now() + HALT_DURATION_MS;
        const dir = price > state.dailyOpen ? 'UPPER' : 'LOWER';
        console.warn(`[CIRCUIT] 🚨 ${dir} BREAKER — ${ticker} halted (${(move * 100).toFixed(1)}%)`);
        circuitBreakerCounter.inc({ticker , side : dir});
        return `Circuit breaker triggered — ${ticker} ${dir} limit breached. Trading halted 10s.`;
    }

    return null;
}

// ── START ─────────────────────────────────────────────────────────────────────
const startServices = async () => {
    try {
        await connectDB();
        console.log('[SERVER] ✅  MongoDB connected');

        await producer.connect();
        await fillConsumer.connect();
        await candleConsumer.connect();
        console.log('[SERVER] ✅  Kafka connected');

        // ── Listen for trade fills from engine.js → push to user sockets ──
        await fillConsumer.subscribe({ topic: 'trade-fills', fromBeginning: false });
        await fillConsumer.run({
            eachMessage: async ({ message }) => {
                try {
                    const { buyerUserId, sellerUserId, ticker, price,quantity, timestamp } = JSON.parse(message.value.toString());
           // FIXED (broadcast to everyone - sidebar will light up)
                        io.emit('live-ticker-update', { ticker, price, quantity, side: 'BUY',  timestamp });
                        io.emit('live-ticker-update', { ticker, price, quantity, side: 'SELL', timestamp });    
                } catch (err) {
                    console.error('[SERVER] Fill broadcast error:', err.message);
                }
            }
        });

        // ── Listen for candle updates from chartEngine.js → push to all browsers ──
        await candleConsumer.subscribe({ topic: 'candle-updates', fromBeginning: false });
        await candleConsumer.run({
            eachMessage: async ({ message }) => {
                try {
                    const candleData = JSON.parse(message.value.toString());
                    io.emit('live-candle-update', candleData);
                } catch (err) {
                    console.error('[SERVER] Candle broadcast error:', err.message);
                }
            }
        });

        server.listen(PORT, () => {
            
            console.log(`[SERVER] 🚀  API + UI live on http://localhost:${PORT}`);
        });

    } catch (err) {
        console.error('[SERVER] Critical startup failure:', err);
        process.exit(1);
    }
};

// ── ROUTES ────────────────────────────────────────────────────────────────────
app.get('/api/candles/:ticker', async (req, res) => {
    try {
        const ticker = req.params.ticker.toUpperCase();
        const supported = ['INFY', 'RELIANCE', 'TCS', 'HDFCBANK'];
        if (!supported.includes(ticker)) {
            return res.status(404).json({ success: false, error: `${ticker} is not tracked` });
        }

        const historicalCandles = await Candle.find({ ticker })
            .sort({ timestamp: 1 })
            .limit(200)
            .lean();

        return res.status(200).json({
            success: true, ticker,
            count: historicalCandles.length,
            data:  historicalCandles
        });
    } catch (err) {
        console.error('[SERVER] Candle fetch error:', err.message);
        return res.status(500).json({ success: false, error: 'Internal server error' });
    }
});

app.get('/metrics' , async(req , res) => {
    try{
        console.log("!!! METRICS ENDPOINT WAS HIT !!!"); // 👈 ADD THIS
        res.setHeader('Content-Type' , register.contentType);
        res.send(await register.metrics());
    }
    catch(err){
        res.status(500).send(err);
    }
});
app.post('/api/orders', async (req, res) => {
    try {
        const apiKey = req.headers['x-api-key'];
        if (!apiKey) return res.status(401).json({ success: false, error: 'API key missing' });

        const user = await User.findOne({ apiKey });
        if (!user)  return res.status(403).json({ success: false, error: 'Invalid API key' });

        const { ticker, price, quantity, side } = req.body;

        if (!ticker || !side || price === undefined || quantity === undefined) {
            return res.status(400).json({ success: false, error: 'Missing required fields' });
        }

        const parsedSide     = side.toUpperCase();
        const parsedPrice    = parseFloat(price);
        const parsedQuantity = parseInt(quantity);

        if (parsedSide !== 'BUY' && parsedSide !== 'SELL') {
            return res.status(400).json({ success: false, error: "Side must be 'BUY' or 'SELL'" });
        }
        if (isNaN(parsedPrice)    || parsedPrice    <= 0) return res.status(400).json({ success: false, error: 'Invalid price' });
        if (isNaN(parsedQuantity) || parsedQuantity <= 0) return res.status(400).json({ success: false, error: 'Invalid quantity' });

        const circuitError = enforceCircuitBreaker(ticker.toUpperCase(), parsedPrice);
        if (circuitError) return res.status(503).json({ success: false, error: circuitError });

        const orderId = `ord_${Math.random().toString(36).substring(2, 9)}`;

        await producer.send({
            topic: 'order-requests',
            messages: [{
                key:   ticker.toUpperCase(),
                value: JSON.stringify({
                    orderId,
                    userId:    user._id.toString(),
                    ticker:    ticker.toUpperCase(),
                    side:      parsedSide,
                    price:     parsedPrice,
                    quantity:  parsedQuantity,
                    timestamp: new Date().toISOString()
                })
            }]
        });

        return res.status(202).json({ success: true, message: 'Order dispatched to Kafka', orderId });

    } catch (err) {
        console.error('[SERVER] Order ingestion error:', err);
        return res.status(500).json({ success: false, error: err.message });
    }
});

startServices().catch(console.error);


/*
whats next actually ? i see it in 3 different places and each time it looks to have a different function
why are someplaces the number is given as 1000 and someplaces its 10_000 ? jsut conventions? 

ciel ? 

req.params.ticker ? 




*/