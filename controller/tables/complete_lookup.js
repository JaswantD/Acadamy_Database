require('dotenv').config({path: __dirname + './../../.env'});
require('./../../databases/PostgresSqlConnection.js');
require('../../databases/mongoConnection');
var moment = require('moment')
var async = require('async')
var ObjectId = require('mongodb').ObjectID;
MongoClient.connect(mDB_conn, function(err,db){
        if (err)throw err;
        console.log("MongoDB Connected")
        var dbo = db.db(process.env.MONGO_DB)
        dbo.collection('completelookups').find({}).toArray(function(err, tdata){
                if (err) throw err;
                //console.log(tdata.length)		
                console.log("inserting Complete_Lookup data");
                var createTb = "CREATE TABLE IF NOT EXISTS temp_completelookups(completelookups_obj_id varchar(100),userid varchar(100), userid_email varchar(100), itemid varchar(100), itemtype varchar(100), __v int)";
                pgClient.query(createTb, function(err){
                    if(err){console.log(err)}
                    
                }) 
                async.auto({
                        "first":function(cb1)
                            {
                                console.log(tdata.length)
                                for(let i=1; i<2;i++)
                                        {
                                            let id = tdata[i]['_id'];
                                            let userid_email = tdata[i]['userId'];
                                            let itemid = tdata[i]['itemId'];
                                            let itemtype = tdata[i]['itemType'];
                                            let v = tdata[i]['__v'];
                                            var query = "insert into temp_completelookups(completelookups_obj_id, userid_email, itemid, itemtype, __v) values($1,$2,$3,$4,$5);"
                                            pgClient.query(query,[id, userid_email, itemid, itemtype, v], function(err, result){
                                            if (err){console.error('could not connect to postgres', err);
                                            }
                                            else{
                                               console.log("..");
                                            }
                                            });
                                            
                                       }
                                        cb1(null,"done")
                            },
                            
                            "second":["first",function(aboveResult,cb2){
                                query ="INSERT INTO completelookups "+
                                        "Select completelookups_obj_id,itemid, itemtype, userid, userid_email, __v from temp_completelookups ON CONFLICT(completelookups_obj_id)"+
                                        "DO UPDATE Set (completelookups_obj_id,  itemid, itemtype, userid_email, __v)="+ 
                                        "(select completelookups_obj_id,  itemid, itemtype, userid_email, __v from temp_completelookups t2 where completelookups.completelookups_obj_id = t2.completelookups_obj_id),"+
                                        "userid =  (select userid FROM users b WHERE completelookups.userid_email = b.username);";
                                
                                pgClient.query(query, function(err, r)
                                {
                                    if(err){ console.log(err)
                                                return err;
                                            }
                                    console.log("Inserted") 
                                    pgClient.query("DROP TABLE temp_completelookups;");    
                                    cb2(null,"NewRecordsinserted")
                                });
                            }],                      
                            function(errr,ress){
                                    //cb(null,"done");
                                }
                })
                       
        });
        
           
});
        