import mongoose from 'mongoose';

//1. USER SCHEMA

const userSchema = new mongoose.Schema({
    username : { type : String , required : true , unique : true},
    email : { type : String  , required : true , unique : true} ,
    apiKey : { type : String , unique : true} //for unique user authentication in real production scenario
}, { timestamps : true});

//2. PORTFOLIO SCHEMA (User-Differentiated Accumulator)
const portfolioSchema = new mongoose.Schema({
    
    userId : { type : mongoose.Schema.Types.ObjectId , ref : 'User', required : true },
    ticker : { type : String ,required: true },

    totalShares : { type : Number , default : 0 },
    averageBuyPrice : {type : Number , default : 0} ,
    totalInvested : { type : Number , default : 0 } 
} , {timestamps: true});


//Ensure a user can only have one unique portfolio document per ticker asset 
portfolioSchema.index({ userId : 1 , ticker : 1} , {unique : true});


//3. UPDATED TRADE HISTORICAL LOG SCHEMA 
const tradeSchema = new mongoose.Schema({
    userId : { type : mongoose.Schema.Types.ObjectId , ref : 'User' , required : true },
    ticker : { type : String, required : true },
    price :  { type : Number , required : true  },
    quantity : { type : Number , required : true },
    side : { type : String , enum : ['BUY' , 'SELL'] , required : true},
    timestamp : { type : Date , default : Date.now}

});

export const User = mongoose.model('User' , userSchema);
export const Portfolio = mongoose.model('Portfolio' , portfolioSchema);
export const HistoricalTrade = mongoose.model('HistoricalTrade' , tradeSchema);


const CandleSchema  = new mongoose.Schema({
    ticker : { type : String ,  required : true },
    timestamp : { type : Date , required : true },
    open : { type : Number , required : true },
    high : { type : Number , required : true },
    low : { type : Number , required : true },
    close : { type : Number , required : true },
    volume : { type : Number , required : true }
}, { timestamps : true});

CandleSchema.index({ticker : 1 , timestamp : 1 });

const Candle = mongoose .model('Candle ' , CandleSchema);
export { Candle };


/*
that looks like sql table declaration for const userSchema


why are we sepeareting userschema and protfolio schema without actually having a user database to fnction for seperate users ? isnt it just accumulating for the same person then?

index keyword ? first time seeing that and 
i see before it had only 1 curlly braces . this one has multiple in the same like in portfolio Schema . index
why is that ? 


*/