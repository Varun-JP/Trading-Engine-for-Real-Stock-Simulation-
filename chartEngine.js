import { Kafka } from 'kafkajs';
import mongoose from 'mongoose';
import dotenv from'dotenv';
import { Candle } from './model.js';

dotenv.config();



//2. KAFKA INITIALIZATION (Belongs to its own distinct consumer group)

const kafka = new Kafka({
    clientId : 'chart-engine',
    brokers:[process.env.KAFKA_BROKERS || 'localhost:9092']
});

const consumer = kafka.consumer({groupId : 'charts-group'});
const producer  = kafka.producer();

//3 . IN_MEMORY STATE FOR THE ACTIVE MINUTE WINDOW
const CURRENT_WINDOW = {
    INFY: null,
    RELIANCE : null,
    TCS : null,
    HDFCBANK : null
};

//Helper to generate a fresh cnadle structure 
const createNewCandleBucket = (ticker , price , quantity , minuteSlot) => {
    return{
        ticker ,
        timestamp : minuteSlot,
        open:price,
        high:price,
        low:price,
        close:price,
        volume:quantity
    };
};

//4. THE CORE OHLC MATHEMATICS ENGINE
const processIncomingTrade = async(trade) => {
    const { ticker , price , quantity , timestamp} = trade;

    //normaize time to the exact start of the current minute )
    const tradeTime = new Date(timestamp);
    const minuteSlot = new Date(
        tradeTime.getFullYear(),
        tradeTime.getMonth(),
        tradeTime.getDate(),
        tradeTime.getHours(),
        tradeTime.getMinutes(),
        0
    );

    let currentCandle = CURRENT_WINDOW[ticker];

    //Case A : No candle exists yet , or the clock rolled over to a completely new minute block
    if(!currentCandle || currentCandle.timestamp.getTime() != minuteSlot.getTime()){

        //If an old candle was sitting in memory from the previous minute , save it to MongoDB before wiping it 
        if(currentCandle){
            try{
                await Candle.create(currentCandle);
                console.log(`[MONGO SAVE] Saved 1m Candle for ${currentCandle.ticker} C : Rs${currentCandle.close} High : Rs${currentCandle.high} Low : Rs${currentCandle.low} V : ${currentCandle.volume}`);
            }
            catch(err){
                console.error("Failed to save  old candle to database " , err.message);
            }
        }

        //Initialize the brand new minute candle block
        CURRENT_WINDOW[ticker] = createNewCandleBucket(ticker , price , quantity , minuteSlot);
        console.log(`[NEW CANDLE] Started 1m window for ${ticker} @ ${price}`);
    }

    // Case B: We are still inside the exact same minute window .Update the high , low close , and volume metrics.
    else{
        currentCandle.high = Math.max(currentCandle.high , price);
        currentCandle.low = Math.min(currentCandle.low , price);
        currentCandle.close = price; //The latest trade price becomes the rolling Close
        currentCandle.volume+=quantity; //Accumulate trade execution sizes


        console.log(`[OHLC UPDATE] ${ticker} | High:Rs${currentCandle.high} | Low:Rs${currentCandle.low} | Close:Rs${currentCandle.close} | Volume:${currentCandle.volume}  `);

        }

        try{
            await producer.send({
                topic: 'candle-updates',
                messages : [
                    {
                        key : ticker , value : JSON.stringify(CURRENT_WINDOW[ticker]),
                    }
                ]
            });
        }
        catch(kafkaErr){
            console.error("Chart Engine failed to broadcast candle update " , kafkaErr.message);
        }
};

const startChartEngine = async()=> {
    try{
        //Connect to MongoDB
        await mongoose.connect(process.env.MONGODB_URI || 'mongodb://localhost:27017/financial_db'); 
        console.log('MongoDB Connected successfully to Chart Engine Channel.');

        //Connect to Kafka

        await producer.connect();
        console.log('Kafka Chart Engine Producer Connected.');
        await consumer.connect();
        console.log('Kafka Chart Engine Consumer Connected.');

        //Subscrbe directly to the execcuted trades broadcast
        await consumer.subscribe({ topic : 'completed-trades' , fromBeginning : false});

        //spin up the consumer loop
        await consumer.run({
            eachMessage : async({ message }) => {
                const rawTradeData = JSON.parse(message.value.toString());
                await processIncomingTrade(rawTradeData);

            }
        });
    }catch(error){
            console.error('Critical Failure in Chart Engine service' , error);
            process.exit(1);

        }
    };

    startChartEngine();

