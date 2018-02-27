MongoClient = require('mongodb').MongoClient;


global.mDB_conn = "mongodb://"+process.env.MONGO_USER+":"+process.env.MONGO_PWD+"@"+process.env.MONGO_HOST+":"+process.env.MONGO_PORT+"/"+process.env.MONGO_DB