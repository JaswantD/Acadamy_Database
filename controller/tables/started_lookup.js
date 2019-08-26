/* This script is fetching started_lookup data from MongoDB and will store to temp table first. After the data stored in temp table
   this script will compare the temp data and target table data, if there will be any change in temp table respectively to target
   table it will update to target table. After completing the upsertion to target table it will delete the temp table.
*/
var pg = require('./../../databases/PostgresSqlConnection.js');
var mongoDB = require('../../databases/mongoConnection');
var moment = require('moment')
var async = require('async')
var ObjectId = require('mongodb').ObjectID;
var sanitizer = require('sanitizer');
exports.sLookupData = function(finalCallback){
    async.auto({

        // Following function is creating temp table for startedlookups
        createTable:function(cb){
            pg.drop("startedlookups",function(err){
                if(err){console.log(err)}
                })
                var createTb = "startedlookups_obj_id varchar(100), itemid varchar(100), itemtype varchar(100), userid varchar(100), userid_email varchar(100), __v int";
                pg.create("startedlookups",createTb, function(err){
                    if(err){console.log(err)}
                    console.log("Created Table")
                    cb();
                })
                
            },
        /* Following block is divided into 2 parts, first and second. 
            The first part is parsing the data and pushing into temp table.
            The second part is upserting the target table from temp table.
        */
        fetchData:["createTable",function(aboveResult,fetchedData){                
            mongoDB.getData('startedlookups',function(err,tdata){
                if (err){console.error('could not connect to postgres', err);}
                console.log("Total Records : ",tdata.length)
                fetchedData(null, tdata)
            })
        }],
        insertData:["fetchData",function(aboveResult,dataInserted){
            async.auto({
                "dataToTempTable":function(tempTableUpdated){
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
                            let userid_email = tdata[i]['userId'].replace("'", "''");
                            userid_email= sanitizer.escape(userid_email);
                            let itemid = tdata[i]['itemId'];
                            let itemtype = tdata[i]['itemType'];
                            let v = tdata[i]['__v'];
                            dataToBeInserted +=" ('"+id+"','"+itemid+"','"+itemtype+"','"+userid_email+"','"+v+"'), "
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
                            var query = "temp_startedlookups(startedlookups_obj_id, itemid, itemtype, userid_email, __v) values ";
                            pg.insert(query,dataQueryArray[z], function(err, result){
                                if (err){console.error('could not connect to postgres', err);}
                                else{insert(z+1)}
                                });
                            }
                        else{   pg.Truncate("startedlookups",function(err){
                                    tempTableUpdated();
                                    })
                            } 
                        }
                    
                    },
                "insertingToMainTable":["dataToTempTable",function(aboveResult,mainTableUpdated){
                    query ="startedlookups Select * from temp_startedlookups ";
                    pg.insert(query,"",function(err, r)
                    {
                        if(err){ console.log(err)
                                 return err;
                                }
                        mainTableUpdated();
                        });
                    }],
                "updatingUserIds":["insertingToMainTable",function(aboveResult,updatedUserIds){
                    update = "startedlookups SET userid = (select userid FROM users b WHERE startedlookups.userid_email = b.username)";
                    pg.update(update,function(err,updated){
                        if (err){console.error('could not connect to postgres', err);} 
                        console.log("Updated userId")
                        updatedUserIds();
                        });
                    }],
                "droppingTempTable":["updatingUserIds",function(aboveResult,droppedTempTable){
                    pg.drop('startedlookups',function(err,dropped){
                        if(err){console.log(err)}
                        droppedTempTable();
                        }); 
                    }]
                
                },
                function(errr,ress){
                    dataInserted();
            });
        }]
                    
        },
        function(error,response){
            console.log("Started_lookups Data Inserted") 
            finalCallback(); 
    });
}       
       
