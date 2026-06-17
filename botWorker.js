import { parentPort, workerData } from 'worker_threads';
import { Kafka } from 'kafkajs';
import { randomUUID } from 'crypto';

const { workerId, kafkaBrokers, kafkaUsername, kafkaPassword } = workerData;


const kafka = new Kafka({
    clientId: `bot-worker-${workerId}`,
    brokers:  [kafkaBrokers],
    ssl: true,
    sasl: {
        mechanism: 'plain',
        username: kafkaUsername,
        password: kafkaPassword,
    },
    logLevel: 0
});

const producer = kafka.producer();
const TICKERS  = ['INFY', 'RELIANCE', 'TCS', 'HDFCBANK'];

// ── MARKET STATE (per worker — independent drift, converges via Redis) ────────
const MARKET_STATE = {
    INFY:     { lastPrice: 1500.00, dailyOpen: 1500.00, haltUntil: null },
    RELIANCE: { lastPrice: 2400.00, dailyOpen: 2400.00, haltUntil: null },
    TCS:      { lastPrice: 3200.00, dailyOpen: 3200.00, haltUntil: null },
    HDFCBANK: { lastPrice: 4000.00, dailyOpen: 4000.00, haltUntil: null },
};

const CIRCUIT_LIMIT    = 0.15;
const HALT_DURATION_MS = 10_000;

function isHalted(ticker) {
    const state = MARKET_STATE[ticker];
    if (!state.haltUntil) return false;
    if (Date.now() < state.haltUntil) return true;
    state.haltUntil = null;
    state.dailyOpen = state.lastPrice;
    log(`[CIRCUIT] ✅  ${ticker} halt lifted. New open: ₹${state.dailyOpen.toFixed(2)}`);
    return false;
}

function checkCircuit(ticker, price) {
    const { dailyOpen } = MARKET_STATE[ticker];
    const move = Math.abs(price - dailyOpen) / dailyOpen;
    if (move >= CIRCUIT_LIMIT) {
        MARKET_STATE[ticker].haltUntil = Date.now() + HALT_DURATION_MS;
        const dir = price > dailyOpen ? '🚨 UPPER' : '🚨 LOWER';
        log(`[CIRCUIT] ${dir} — ${ticker} halted 10s (${(move * 100).toFixed(1)}% move)`);
        return true;
    }
    return false;
}

// ── BOT FACTORY ───────────────────────────────────────────────────────────────
// Valid 24-char hex ObjectId so Mongoose doesn't throw cast errors
function fakeObjectId(workerId, botIndex) {
    const prefix = (workerId * 1000 + botIndex).toString(16).padStart(8, '0');
    const fill   = Math.floor(Math.random() * 0xffffffffffff).toString(16).padStart(16, '0');
    return (prefix + fill).substring(0, 24);
}

function createBot(localId, personality, ticker) {
    const baseCash = {
        market_maker: 500_000,
        noise_trader:  50_000,
        whale:       2_000_000,
        contrarian:   200_000,
    }[personality];

    const baseShares = {
        market_maker: 200,
        noise_trader:  30,
        whale:        500,
        contrarian:   100,
    }[personality];

    return {
        id:          `w${workerId}_${localId}`,
        userId:      fakeObjectId(workerId, localId),
        personality,
        ticker,
        cash:        baseCash + Math.random() * baseCash * 0.2,
        shares:      baseShares + Math.floor(Math.random() * 20),
    };
}

// ── BUILD BOT POOL: 125 bots per worker, all 4 personalities ─────────────────
// Per ticker (4 tickers): 10 market makers, 12 noise traders, 5 whales, 4 contrarians = ~31 per ticker
// 31 × 4 tickers = 124 bots + 1 extra noise trader = 125 total per worker
const BOTS = [];
let localId = 0;

for (const ticker of TICKERS) {
    for (let i = 0; i < 5; i++) BOTS.push(createBot(localId++, 'market_maker', ticker)); //10
    for (let i = 0; i < 4; i++) BOTS.push(createBot(localId++, 'noise_trader', ticker)); //12
    for (let i = 0; i <  2; i++) BOTS.push(createBot(localId++, 'whale',        ticker)); //5
    for (let i = 0; i <  2; i++) BOTS.push(createBot(localId++, 'contrarian',   ticker)); //4
}
// Add 1 extra noise trader to hit exactly 125
BOTS.push(createBot(localId++, 'noise_trader', TICKERS[0]));

// ── ORDER GENERATION ──────────────────────────────────────────────────────────
function generateOrder(bot) {
    const state = MARKET_STATE[bot.ticker];
    const last  = state.lastPrice;

    switch (bot.personality) {
        case 'market_maker': {
            const side   = Math.random() > 0.5 ? 'BUY' : 'SELL';
            const spread = last * 0.001;
            const price  = side === 'BUY'
                ? parseFloat((last - spread * (0.5 + Math.random())).toFixed(2))
                : parseFloat((last + spread * (0.5 + Math.random())).toFixed(2));
            const qty    = Math.floor(Math.random() * 30) + 5;
            if (side === 'BUY'  && bot.cash   < price * qty) return null;
            if (side === 'SELL' && bot.shares < qty)          return null;
            return { side, price, quantity: qty };
        }

        case 'noise_trader': {
            const side   = Math.random() > 0.5 ? 'BUY' : 'SELL';
            const jitter = (Math.random() * 2 - 1) * last * 0.003;
            const price  = parseFloat((last + jitter).toFixed(2));
            const qty    = Math.floor(Math.random() * 15) + 1;
            if (side === 'BUY'  && bot.cash   < price * qty) return null;
            if (side === 'SELL' && bot.shares < qty)          return null;
            return { side, price, quantity: qty };
        }

        case 'whale': {
            const side  = Math.random() > 0.45 ? 'BUY' : 'SELL';
            const aggr  = last * (0.002 + Math.random() * 0.004);
            const price = side === 'BUY'
                ? parseFloat((last + aggr).toFixed(2))
                : parseFloat((last - aggr).toFixed(2));
            const qty   = Math.floor(Math.random() * 200) + 100;
            if (side === 'BUY'  && bot.cash   < price * qty) return null;
            if (side === 'SELL' && bot.shares < qty)          return null;
            return { side, price, quantity: qty };
        }

        case 'contrarian': {
            const trendUp = last > state.dailyOpen;
            const side    = trendUp ? 'SELL' : 'BUY';
            const jitter  = (Math.random() * 0.002) * last;
            const price   = side === 'BUY'
                ? parseFloat((last - jitter).toFixed(2))
                : parseFloat((last + jitter).toFixed(2));
            const qty     = Math.floor(Math.random() * 50) + 10;
            if (side === 'BUY'  && bot.cash   < price * qty) return null;
            if (side === 'SELL' && bot.shares < qty)          return null;
            return { side, price, quantity: qty };
        }
    }
}

// ── CAPITAL BOOKKEEPING ───────────────────────────────────────────────────────
function deductCapital(bot, side, price, qty) {
    if (side === 'BUY') {
        bot.cash   -= price * qty;
        bot.shares += qty;
    } else {
        bot.cash   += price * qty;
        bot.shares  = Math.max(0, bot.shares - qty);
    }
    if (bot.cash   < 5_000) { bot.cash   += 50_000; }
    if (bot.shares < 5)     { bot.shares += 50;     }
}

// ── FIRE ORDER ────────────────────────────────────────────────────────────────
let tickCount = 0;

async function fireBotOrder(bot) {
    const ticker = bot.ticker;
    if (isHalted(ticker)) return;

    const order = generateOrder(bot);
    if (!order) return;

    const { side, price, quantity } = order;
    if (price <= 0) return;
    if (checkCircuit(ticker, price)) return;

    deductCapital(bot, side, price, quantity);
    MARKET_STATE[ticker].lastPrice = price;

    try {
        await producer.send({
            topic:    'order-requests',
            messages: [{ key: ticker, value: JSON.stringify({
                orderId: `sim_${randomUUID()}`, //so we can tell a bot apart from real human
                userId:    bot.userId,
                ticker,
                side,
                price,
                quantity,
                timestamp: new Date().toISOString(),
            })}],
        });
        tickCount++;
    } catch (err) {
        log(`⚠️  Bot ${bot.id} send error: ${err.message}`);
    }
}

// ── TICK INTERVALS PER PERSONALITY ───────────────────────────────────────────
const TICK_INTERVALS = {
    market_maker: 400,
    noise_trader: 250,
    whale:       3000,
    contrarian:   800,
};

// ── START ─────────────────────────────────────────────────────────────────────
function log(msg) {
    parentPort.postMessage({ type: 'log', data: `[W${workerId}] ${msg}` });
}

async function startSimulation() {
    await producer.connect();
    log(`✅ Kafka connected — ${BOTS.length} bots across ${TICKERS.length} tickers`);

    for (const bot of BOTS) {
        const interval   = TICK_INTERVALS[bot.personality];
        const startDelay = Math.random() * interval;
        setTimeout(() => {
            setInterval(() => fireBotOrder(bot), interval);
        }, startDelay);
    }

    // Per-worker snapshot every 15 seconds
    setInterval(() => {
        log(`📊 Ticks fired: ${tickCount} | Prices: ${
            TICKERS.map(t => `${t} ₹${MARKET_STATE[t].lastPrice.toFixed(0)}`).join(' | ')
        }`);
    }, 15_000);

    log(`🚀 All ${BOTS.length} bots live`);
}

startSimulation().catch(err => log(`🚨 ${err.message}`));

/*
kafkaBrokers ? why did it go from kafka to kafka brokers without the url now ? 
log level? 

differnece between const and function ? 
*/