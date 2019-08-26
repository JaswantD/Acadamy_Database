` This script is fetching earned_badges data from MongoDB and storing to temp table first. After the data stored in temp table
   this script will compare the temp data and target table data, if there will be any change in temp table respectively to target
   table it will update to target table. After completing the upsertion to target table it will delete the temp table.`

var pg = require('./../../databases/PostgresSqlConnection.js');
var mongoDB = require('../../databases/mongoConnection');
var moment = require('moment')
var async = require('async')
var ObjectId = require('mongodb').ObjectID;
var sanitizer = require('sanitizer');
exports.eBadgesData = function(finalCallback){
    async.auto({
        // Following @createTable function is used to create a temp table in postgres 
        createTable:function(cb){
            var fields = "earnedbadges_obj_id varchar(255), userid varchar(100), userid_email varchar(255), itemid varchar(255), itemtype varchar(255), parentitemid varchar(255), parentitemtype varchar(255), dateearned date, __v integer, created_date date";
            pg.drop("earnedbadges",function(err){
                if(err){console.log(err)}
                pg.create("earnedbadges",fields, function(err){
                    if(err){console.log(err)}
                    console.log("Created Table")
                    }) 
                cb();
                });
            },
        fetchData:["createTable",function(aboveResult,dataFetched){                
            mongoDB.getData("earnedbadges",function(err,tdata){
                console.log("Total Records : ",tdata.length)
                dataFetched(null, tdata);
                })
            }],
                /* Following block is divided into 2 parts, first and second. 
                The first part is parsing the data and pushing into temp table.
                The second part is upserting the target table from temp table.
                */
        insertData:["fetchData",function(aboveResult,dataInserted){
            async.auto({
                "dataToTempTable":function(tempTableUpdated){
                    var tdata = aboveResult.fetchData;
                    var dataQueryArray = [];
                    var start = 0;
                    if(tdata.length>500){
                        var last = 500;}
                    else{var last = tdata.length}                                                    
                    while(start<last){
                        dataToBeInserted = "";
                        for(i=start;i<last;i++){
                            let id =  tdata[i]['_id'];
                            let userid_email = tdata[i]['userId'];
                            let itemid = tdata[i]['itemId'];
                            let itemtype = tdata[i]['itemType'];
                            let parentitemid = tdata[i]['parentItemId'];
                            let parentitemtype = tdata[i]['parentItemType'];
                            let dateearned = moment(tdata[i]['dateEarned']).format('YYYY-MM-DD HH:mm:ss');
                            let v = tdata[i]['__v'];
                            let createddate =  moment(ObjectId(id).getTimestamp()).format('YYYY-MM-DD');
                            dataToBeInserted +=" ('"+id+"','"+userid_email+"','"+itemid+"','"+itemtype+"','"+parentitemid+"','"+parentitemtype+"','"+dateearned+"','"+v+"','"+createddate+"'), "
                            }
                        dataToBeInserted = dataToBeInserted.substring(0,dataToBeInserted.length-2)
                        dataQueryArray.push(dataToBeInserted)
                        start = last;
                        if(tdata.length-last>500){
                            last += 500;}
                        else{last = tdata.length;}  
                        }
                    insert(0)
                    function insert(z){
                        if(z < dataQueryArray.length){
                            console.log("inside insert Function");
                            var query = "temp_earnedbadges(earnedbadges_obj_id, userid_email, itemid, itemtype, parentitemid, parentitemtype, dateearned, __v, created_date) values ";
                            console.log("Inserting query")
                            pg.insert(query,dataQueryArray[z], function(err, result){
                                if (err){console.error('could not connect to postgres', err)};
                                insert(z+1);
                                });
                            }
                        
                        else{   pg.Truncate("earnedbadges",function(err){
                                    tempTableUpdated();
                                    })
                                }
                        }    
                    },
                "insertingToMainTable":["dataToTempTable",function(aboveResult,mainTableUpdated){
                    query = "earnedbadges Select * from temp_earnedbadges";
                    pg.insert(query,"",function(err, inserted){
                        if(err){ console.log(err)
                                 return err;
                            }
                            mainTableUpdated();
                        });
                    }],
                "updatingUserIds":["insertingToMainTable",function(aboveResult,updatedUserIds){
                    update = "earnedbadges SET userid = (select userid FROM users b WHERE earnedbadges.userid_email = b.username)";
                    pg.update(update,function(err,updated){
                        if (err){console.error('could not connect to postgres', err);} 
                        console.log("Updated userId")
                        updatedUserIds();
                        });
                    }],
                "droppingTempTable":["updatingUserIds",function(aboveResult,droppedTempTable){
                    pg.drop('earnedbadges',function(err,dropped){
                        if(err){console.log(err)}
                        console.log("Dropped the table")
                        droppedTempTable();
                        }); 
                    }]  
                },function(errr,ress){
                    dataInserted();
                });
            }]
        
    },function(error, result){
        console.log("EarnedBadges Data Inserted");
        finalCallback()
        });
}                


