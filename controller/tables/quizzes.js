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
        dbo.collection('quizzes').find({}).toArray(function(err, tdata){
                if (err) throw err;
                //console.log(tdata.length)		
                console.log("inserting quizzes data");
                var createTb = "CREATE TABLE IF NOT EXISTS temp_quizzesquestions(quizzesschema_obj_id varchar(100), questions__sys__id varchar(100), questions__sys__contenttype varchar(100),questions__internaltitile varchar(100), questiontext varchar(100), internaltitle varchar(100), created_date date)";
                var createTb = "CREATE TABLE IF NOT EXISTS temp_quizzesanswerslist(quizzesschema_obj_id varchar(100), questions__sys__id varchar(100), sys__contenttype varchar(100),internaltitile varchar(100), questiontext varchar(100), internaltitle varchar(100), created_date date)";
                var createTb = "CREATE TABLE IF NOT EXISTS temp_earnedcertificates(created_date date)";
                pgClient.query(createTb, function(err){
                    if(err){console.log(err)}
                    
                }) 
                async.auto({
                        "first":function(cb1)
                            {
                                console.log(tdata.length)
                                for(let i=1; i<tdata.length;i++)
                                        {
                                            let id = tdata[i]['_id'];
                                                let userid_email = tdata[i]['userId'];
                                                let examid = tdata[i]['examId'];
                                            let examtype = tdata[i]['examType'];
                                                let certificateid = tdata[i]['certificateId'];
                                                let certificationid = tdata[i]['certificationId'];
                                            let certificationtype = tdata[i]['certificationType'];
                                            let dateearned = moment(tdata[i]['dateEarned']).format('YYYY-MM-DD HH:mm:ss');
                                                let dateexpires = moment(tdata[i]['dateExpires']).format('YYYY-MM-DD HH:mm:ss');
                                                let examresultsid = tdata[i]['examResultsId'];
                                            let v = tdata[i]['__v'];
                                            let certificationname =  tdata[i]['certificationname'];
                                            let createddate =  moment(ObjectId(id).getTimestamp()).format('YYYY-MM-DD');
                                            var query = "insert into temp_earnedcertificates(earnedcertificates_obj_id, userid_email, examid, examtype, certificateid, certificationid, certificationtype, dateearned, dateexpires, examresultsid, __v, certificationname, created_date)  values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13);"
                                            pgClient.query(query,[id,userid_email,examid, examtype,certificateid,certificationid,certificationtype, dateearned, dateexpires, examresultsid, v, certificationname, createddate], function(err, result){
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
                                query ="INSERT INTO earnedcertificates "+
                                        "Select earnedcertificates_obj_id, userid_email, examid, examtype, certificateid, certificationid, certificationtype, dateearned, dateexpires, examresultsid, __v, certificationname, created_date from temp_earnedcertificates ON CONFLICT(earnedcertificates_obj_id)"+
                                        "DO UPDATE Set (earnedcertificates_obj_id, userid_email, examid, examtype, certificateid, certificationid,"+ 
                                        "certificationtype, dateearned, dateexpires, examresultsid, __v, certificationname, created_date)"+
                                        "= (select earnedcertificates_obj_id, userid_email, examid, examtype, certificateid, certificationid,"+ 
                                        "certificationtype, dateearned, dateexpires, examresultsid, __v, certificationname, created_date "+
                                        "from temp_earnedcertificates t2 where earnedcertificates.earnedcertificates_obj_id = t2.earnedcertificates_obj_id);"
                                pgClient.query(query, function(err, r)
                                {
                                    if(err){ console.log(err)
                                                return err;
                                            }
                                    console.log("Inserted") 
                                    pgClient.query("DROP TABLE temp_earnedcertificates;");    
                                    cb2(null,"NewRecordsinserted")
                                });
                            }],                      
                        
                            function(errr,ress){
                                    //cb(null,lastFetched_uid);
                                }
                })
                       
        });
        
           
});
        