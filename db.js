import dotenv from 'dotenv';
import mongoose from 'mongoose';
import {User } from './model.js'; //Import our new models

dotenv.config();

const connectDB = async () => {
    try{
        //This pulls the exact string we jsut saved in your .env file
        const conn = await mongoose.connect(process.env.MONGODB_URI);
        console.log(`MongoDB connected successflly : ${conn.connection.host}`);

        //Seed a permanent test user if the collection is empty 
        const usersToSeed = [
            { username : 'varun_developer' , email : 'varun@example.com' , apiKey : 'varun_secret_key'},
            {username : 'xyz_trader' , email : 'xyz@example.com' , apiKey : 'xyz_secret_key'}
        ];
        for( const userData of usersToSeed){
            const userExists = await User.findOne({ username : userData.username });
            if(!userExists ) {
                const newUser = await User.create(userData);
                console.log(` Seeded User : ${newUser.username} | ID : ${newUser._id}`);
            }
            else{
                console.log(`Active User : ${userExists.username} | ID: ${userExists._id}`);
            }
            
        }
    }
    catch(error){
        console.error(`Database connection failed : ${error.message}`);
        process.exit(1);
    }
};

export default connectDB;


/*

whats findOne ? 
userData is just a iterator right ? no other significance ? 
*/