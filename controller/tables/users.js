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
        dbo.collection('users').find({}).toArray(function(err, tbData){
                if (err) throw err;
                else{
                //console.log(tdata.length)		
                console.log("inserting users data");
                var createTb = "CREATE TABLE IF NOT EXISTS temp_users(userid varchar(100), username varchar(100), __v int, emails varchar(100), firstname varchar(100), lastname varchar(100),created_date date)";
                pgClient.query(createTb, function(err){
                    if(err){console.log(err)}
                    
                });
                async.auto({
                            "first":function(cb1){
                                console.log("First phase");
                                var fname;
                                var lname;
                                for(i=0;i<tbData.length;i++)
                                    {
                                        console.log(tbData[i]);
                                        let userid = tbData[i]['_id'];
                                        let username = tbData[i]['username'];
                                        if(tbData[i].hasOwnProperty('name')){
                                            fname = tbData[i]['name']['first']
                                            lname= tbData[i]['name']['last'];
                                        }
                                        else {
                                                fname = 'Null';
                                                lname= 'Null';
                                            }
                                        let emails = tbData[i]['emails'][0];
                                        if (tbData[i]['emails']>1){
                                            for(let em=1; em<tbData[i]['emails'].length;em++)
                                                emails = emails + "," +tbData[i]['emails'][em];
                                    
                                        }	
                                        let v = tbData[i]['__v']
                                        let createddate =  moment(ObjectId(userid).getTimestamp()).format('YYYY-MM-DD');
                                        var users_data = 'insert into temp_users(userid, username, __v, emails, firstname, lastname,created_date) values($1,$2,$3,$4,$5,$6,$7)';
                                        console.log(users_data);
                                        pgClient.query(users_data, [userid,username,v,emails,fname,lname,createddate], function(err, result){
                                            if (err){  console.error('could not connect to postgres', err);	}
                                            
                                        });
                                    }
                                    cb1(null,"TempCreated");
                            },
                            "second":["first",function(aboveResult,cb2){
                                console.log("second phase")
                                query ="INSERT INTO users(userid, username, __v, emails, firstname, lastname, created_date) Select userid, username, __v, emails, firstname, lastname, created_date from temp_users ON CONFLICT(userid) DO UPDATE Set (userid, username, __v, emails, firstname, lastname, created_date)"+
                                        "= (select userid, username, __v, emails, firstname, lastname, created_date from temp_users t2 where users.userid = t2.userid);"
                                console.log(query)
                                
                                pgClient.query(query, function(err, r)
                                {
                                    if(err){ console.log(err)
                                                return err;
                                            }
                                    console.log("Inserted") 
                                    pgClient.query("DROP TABLE temp_users;");  
                                    cb2(null,"NewRecordsinserted")
                                });
                                
                            }],
                                function(errr,ress){
                                //  cb(null,"done");
                            }
                                        
                });
            }
        })
    }) 

                                    
                             
                  
        
        
     