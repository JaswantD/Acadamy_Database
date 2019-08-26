/* This script is fetching complete_lookup data from MongoDB and will store to temp table first. After the data stored in temp table
   this script will compare the temp data and target table data, if there will be any change in temp table respectively to target
   table it will update to target table. After completing the upsertion to target table it will delete the temp table.
*/

var pg = require('./../../databases/PostgresSqlConnection.js');
var mongoDB = require('../../databases/mongoConnection');
var moment = require('moment')
var async = require('async')
var ObjectId = require('mongodb').ObjectID;
var sanitizer = require('sanitizer');
exports.cLookupData = function(finalCallback){
    async.auto({                                        //async is used to make synchronous work flow
        // Following @createTable function is used to create a temp table in postgres 
        createTable:function(cb){                       
            pg.drop("completelookups",function(err){
                if(err){console.log(err)}
                })
            var createTb = "completelookups_obj_id varchar(100),userid varchar(100), userid_email varchar(100), itemid varchar(100), itemtype varchar(100), __v int";
            pg.create("completelookups",createTb, function(err){
                if(err){console.log(err)}
                cb();
                }) 
            },
        fetchData:["createTable",function(aboveResult,fetchedData){                
            mongoDB.getData('completelookups',function(err,tdata){
                if (err){console.error('could not connect to postgres', err);}
                console.log("Total Records : ",tdata.length)
                fetchedData(null, tdata)
            })
            }],
         // This function is used to parse the data, that is fetched data from mongoDB, and pushing into Postgres temp Table
        insertData:["fetchData",function(aboveResult,dataInserted){  
            async.auto({
                //first block is parsing data and pushing to temp table
                "dataToTempTable":function(tempTableUpdated){          
                        var tdata = aboveResult.fetchData;           
                        dataToBeInserted = "";
                        for(i=0;i<tdata.length;i++){
                            let id = tdata[i]['_id'];
                            let userid_email = tdata[i]['userId'].replace("'", "''");
                            userid_email= sanitizer.escape(userid_email)
                            let itemid = tdata[i]['itemId'];
                            let itemtype = tdata[i]['itemType'];
                            let v = tdata[i]['__v'];
                            dataToBeInserted +=" ('"+id+"','"+userid_email+"','"+itemid+"','"+itemtype+"','"+v+"'), "
                            }
                        dataToBeInserted = dataToBeInserted.substring(0,dataToBeInserted.length-2)
                        var query = "temp_completelookups(completelookups_obj_id, userid_email, itemid, itemtype, __v) values ";
                        pg.insert(query,dataToBeInserted, function(err, result){
                            if (err){console.error('could not connect to postgres', err);}
                            else{   pg.Truncate("completelookups",function(err){
                                        tempTableUpdated();
                                    })
                                    }
                            })   
                    },
                    // The following  block is comparing the temp table with target table and will upsert the data into target table
                "insertingToMainTable":["dataToTempTable",function(aboveResult,mainTableUpdated){ 
                    query ="completelookups Select completelookups_obj_id,itemid, itemtype, userid, userid_email, __v from temp_completelookups";
                    pg.insert(query,"", function(err, r) {
                        if(err){ console.log(err)
                                    return err;
                                }
                        mainTableUpdated();
                            });
                    }],
                "updatingUserIds":["insertingToMainTable",function(aboveResult,updatedUserIds){
                    update = "completelookups SET userid = (select userid FROM users b WHERE completelookups.userid_email = b.username)";
                    pg.update(update,function(err,updated){
                        if (err){console.error('could not connect to postgres', err);} 
                        console.log("Updated userId")
                        updatedUserIds();
                        });
                        }],
                "droppingTempTable":["updatingUserIds",function(aboveResult,droppedTempTable){
                    pg.drop('completelookups',function(err,dropped){
                        if(err){console.log(err)}
                        console.log("Dropped the table")
                        droppedTempTable();
                        }); 
                    }]
                
                },function(errr,ress){
                    dataInserted();
                });
            }]
                    
        },function(error,response){
            console.log("Complete Lookups Data Inserted")
            finalCallback();   
    });
}            



