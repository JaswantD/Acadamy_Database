// MongoClient = require('mongodb').MongoClient;
// global.mDB_conn = process.env.MONGODB
// module.exports={
//     getData: function (table,cbData){
//         MongoClient.connect(mDB_conn, function(err,db){             //connecting to MongoDB
//             if (err)throw err;
//             console.log("MongoDB Connected")
//             var dbo = db.db(process.env.MONGO_DB)
//             dbo.collection(table).find({}).addCursorFlag('noCursorTimeout', true).toArray(function(err, tdata){    //fetching completelookups data into @tdata 
//                 cbData(null,tdata);
//                 });
//             })
//         }
//     }

    
MongoClient = require('mongoose');
global.mDB_conn = process.env.MONGODB;
console.log(mDB_conn)
MongoClient.connect(mDB_conn);
var connection = MongoClient.connection;
connection.on('error', console.error.bind(console, 'connection error:'));
connection.on('open',function(){
    console.log("mongodb is connected!!");
})
module.exports={
    getData: function (table,cbData){
        console.log("inside get data");
        connection.db.collection(table, function(err, collection){
            collection.find({}).toArray(function(err, tdata){
                cbData(null,tdata);
            })
        });
    
        }
    }

// var connection = MongoClient.connection;

// connection.on('error', console.error.bind(console, 'connection error:'));
// connection.once('open', function () {

//     connection.db.collection("earnedbadges", function(err, collection){
//         collection.find({}).toArray(function(err, data){
//             console.log(data); // it will print your collection data
//         })
//     });

// });





// var ObjectId = require('mongodb').ObjectID;
// const getData= function (table,cbData){
//     MongoClient.connect(mDB_conn, function(err,db){             //connecting to MongoDB
//         if (err)throw err;
//         console.log("MongoDB Connected")
//         var dbo = db.db(process.env.MONGO_DB)
//         dbo.collection(table).find({}).toArray(function(err, tdata){    //fetching completelookups data into @tdata 
//             cbData(null,tdata);
//             });
//         })
//     }

//     module.exports={
//         getData:getData
//     }

// module.exports.getData= function(table,cbData){
//     MongoClient.connect(mDB_conn, function(err,db){             //connecting to MongoDB
//         if (err)throw err;
//         console.log("MongoDB Connected")
//         var dbo = db.db(process.env.MONGO_DB)
//         dbo.collection(table).find({}).toArray(function(err, tdata){    //fetching completelookups data into @tdata 
//             cbData(null,tdata);
//             });
//         })
//     }



    

    

    