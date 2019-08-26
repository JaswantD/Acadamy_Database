/* This script is fetching earned_certificates data from MongoDB and storing to temp table first. After the data stored in temp table
   this script will compare the temp data and target table data, if there will be any change in temp table respectively to target
   table it will update to target table. After completing the upsertion to target table it will delete the temp table.
*/
var pg = require('./../../databases/PostgresSqlConnection.js');
var mongoDB = require('../../databases/mongoConnection');
var moment = require('moment')
var async = require('async')
var ObjectId = require('mongodb').ObjectID;
var sanitizer = require('sanitizer');
exports.eCertificatesData = function(finalCallback){
    async.auto({
        //Following funcion is creating temp table
        createTable:function(createdTable){
            var createTb = "earnedcertificates_obj_id varchar(100),userid varchar(100), userid_email varchar(100), examid varchar(100), examtype varchar(100), certificateid varchar(100), certificationid varchar(100), certificationtype varchar(100), dateearned DATE, dateexpires date, examresultsid varchar(100), __v int, certificationname varchar(100),created_date date";
            pg.create("earnedcertificates",createTb, function(err){
                if(err){console.log(err)}
                console.log("Temp Table created")
                }) 
            createdTable();
            },
        /* Following block is divided into 2 parts, first and second. 
            The first part is parsing the data and pushing into temp table.
            The second part is upserting the target table from temp table.
        */
        fetchData:["createTable",function(aboveResult,fetchedData){                
            mongoDB.getData('earnedcertificates',function(err,tdata){
                if (err){console.error('could not connect to postgres', err);}
                console.log("Total Records : ",tdata.length)
                fetchedData(null, tdata)
                })
            }],
        insertData:["fetchData",function(aboveResult,dataInserted){
            async.auto({
                "dataToTempTable":function(updatedTempTable){
                    var dataQueryArray = [];
                    var start = 0;
                    var tdata = aboveResult.fetchData;
                    if(tdata.length>500){
                        var last = 500;}
                    else{var last = tdata.length}                                                    
                    while(start<last){
                        dataToBeInserted = "";
                        for(i=start;i<last;i++){
                            let id = tdata[i]['_id'];
                            let userid_email = tdata[i]['userId'];
                            let examid = tdata[i]['examId'];
                            let examtype = tdata[i]['examType'];
                            let certificateid = tdata[i]['certificateId'];
                            let certificationid = tdata[i]['certificationId'];
                            let certificationtype = tdata[i]['certificationType'];
                            let dateearned = ""
                            if(tdata[i].hasOwnProperty('dateEarned'))
                                dateearned = moment(tdata[i]['dateEarned']).format('YYYY-MM-DD');
                            let dateexpires = ""
                            if(tdata[i].hasOwnProperty('dateExpires'))
                                dateexpires = moment(tdata[i]['dateExpires']).format('YYYY-MM-DD');
                            let examresultsid = tdata[i]['examResultsId'];
                            let v = tdata[i]['__v'];
                            let certificationname = ""
                            if(tdata[i].hasOwnProperty('certificationName'))
                                certificationname =  tdata[i]['certificationName'];
                            let createddate = moment(ObjectId(id).getTimestamp()).format('YYYY-MM-DD');
                            dataToBeInserted +=" ('"+id+"','"+userid_email+"','"+examid+"','"+examtype+"','"+certificateid+"','"+certificationid+"','"+certificationtype+"','"+dateearned+"','"+dateexpires+"','"+examresultsid+"','"+v+"','"+certificationname+"','"+createddate+"'), "
                            }
                        dataToBeInserted = dataToBeInserted.substring(0,dataToBeInserted.length-2)
                        dataQueryArray.push(dataToBeInserted)
                        start = last;
                        if(tdata.length-last>500){
                            last += 500;}
                        else{last = tdata.length;}  
                        }
                    console.log("No. of lists in the array = ",dataQueryArray.length)
                    insert(0)
                    function insert(z){
                        if(z < dataQueryArray.length){    
                            var query = "temp_earnedcertificates(earnedcertificates_obj_id, userid_email, examid, examtype, certificateid, certificationid, certificationtype, dateearned, dateexpires, examresultsid, __v, certificationname, created_date)  values ";
                            pg.insert(query,dataQueryArray[z], function(err, result){
                                if (err){console.error('could not connect to postgres', err);}
                                insert(z+1)
                                });
                            }
                        else{   pg.Truncate("earnedcertificates",function(err){
                                    updatedTempTable();
                                    })
                                }
                        }
                    
                    },
                "insertingToMainTable":["dataToTempTable",function(aboveResult,updatedMainTable){
                    query ="earnedcertificates Select * from temp_earnedcertificates"
                    pg.insert(query,"",function(err, r){
                        if(err){     console.log(err)
                                     return err;
                                }
                        updatedMainTable();
                        });
                    }],
                "updatingUserIds":["insertingToMainTable",function(aboveResult,updatedUserIds){
                    update = "earnedcertificates SET userid = (select userid FROM users b WHERE earnedcertificates.userid_email = b.username)";
                    pg.update(update,function(err,updated){
                        if (err){console.error('could not connect to postgres', err);} 
                        console.log("Updated userId")
                        updatedUserIds();
                        });
                    }],
                "droppingTempTable":["updatingUserIds",function(aboveResult,droppedTempTable){
                    pg.drop('earnedcertificates',function(err,dropped){
                        if(err){console.log(err)}
                        console.log("Dropped the table")
                        droppedTempTable();
                    }); 
                    }]  
                },function(errr,ress){                    //this function is to check the result of internal Async
                    dataInserted();
                });
            }]
        },function(error,response){                       //this function is to check the result of outer Async
            console.log("Earned Certificates Data Inserted")
            finalCallback();
        });
    }
