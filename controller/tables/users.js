/* This script is fetching users data from MongoDB and storing to temp table first. After the data stored in temp table
   this script will compare the temp data and target table data, if there will be any change in temp table respectively to target
   table it will update to target table. After completing the upsertion to target table it will delete the temp table.
*/
var pg = require('./../../databases/PostgresSqlConnection.js');
var mongoDB = require('../../databases/mongoConnection');
var moment = require('moment')
var async = require('async')
var ObjectId = require('mongodb').ObjectID;
var sanitizer = require('sanitizer');
exports.usersData = function(finalCallback){
    async.auto({
        //Following function is creating temp table for users
        createTable:function(cb){
            var fields = "userid varchar(100), username varchar(100), __v int, emails varchar(100), firstname Text, lastname Text, accountid varchar(100),created_date date";
            pg.drop("users",function(err){
                if(err){console.log(err)}
                pg.create("users",fields, function(err){
                    if(err){console.log(err)}
                    else{console.log("Created Table")
                        cb();
                        }
                    });
                });
            },
        fetchData:["createTable",function(aboveResult,cbData){                
            mongoDB.getData("users",function(err,tdata){
                console.log("Total Records : ",tdata.length)
                cbData(null, tdata)
                })
            }],
            /* Following block is divided into 2 parts, first and second. 
            The first part is parsing the data and pushing into temp table.
            The second part is upserting the target table from temp table.
            */
        insertData:["fetchData",function(aboveResult,dataInserted){
            var tbData = aboveResult.fetchData;
            async.auto({
                "insertedToTemp":function(updatedTempTable){
                    var fname;
                    var lname;
                    var dataQueryArray = [];
                    var start = 0;
                    if(tbData.length>500){
                        var last = 500;}
                    else{var last = tbData.length}                                                    
                    while(start<last){
                        dataToBeInserted = "";
                        for(i=start;i<last;i++){
                                let userid = tbData[i]['_id'];
                                let username = tbData[i]['username'].replace("'", "''");
                                username= sanitizer.escape(username)
                                if(tbData[i].hasOwnProperty('name')){
                                    fname = tbData[i]['name']['first'].replace("'", "''");
                                    fname = sanitizer.escape(fname)
                                    if(tbData[i]['name']['last']){
                                        lname= tbData[i]['name']['last'].replace("'", "''");
                                        lname= sanitizer.escape(lname)
                                        }
                                    }
                                else {  fname = 'Null';
                                        lname= 'Null';
                                    }
                                let emails = tbData[i]['emails'][0].replace("'", "''");
                                emails = sanitizer.escape(emails)
                                if (tbData[i]['emails']>1){
                                    for(let em=1; em<tbData[i]['emails'].length;em++)
                                        temp = tbData[i]['emails'][em].replace("'", "''");
                                        temp = sanitizer.escape(temp)
                                        emails = emails + "," +temp
                                    }	
                                let v = tbData[i]['__v']
                                let accountid = 'Null'
                                if(tbData[i]['_json'] && tbData[i]['_json']['accounts']){
                                    accountid = tbData[i]['_json']['accounts']['id'] 
                                    }
                                let createddate =  moment(ObjectId(userid).getTimestamp()).format('YYYY-MM-DD');
                                dataToBeInserted +=" ('"+userid+"','"+username+"','"+v+"','"+emails+"','"+fname+"','"+lname+"','"+accountid+"','"+createddate+"'), "
                            }
                        dataToBeInserted = dataToBeInserted.substring(0,dataToBeInserted.length-2)
                        dataQueryArray.push(dataToBeInserted)
                        start = last;
                        if(tbData.length-last>500){
                            last += 500;}
                        else{last = tbData.length;}  
                        }
                    console.log("No. of lists in the array = ",dataQueryArray.length)
                    insert(0)
                    function insert(z){
                        if(z < dataQueryArray.length){
                            var query = 'temp_users(userid, username, __v, emails, firstname, lastname,accountid,created_date) values ';
                            pg.insert(query,dataQueryArray[z],function(err, result){
                                if (err){  console.error('could not connect to postgres', err);	}
                                else{console.log("Inserting batch %s of user's data...",z);
                                    insert(z+1)}
                                });
                            }   
                        else{   console.log("Transefering to main table");
                                updatedTempTable();
                            }
                        }
                    
                    },
                "insertingToMain":["insertedToTemp",function(aboveResult,updatedMainTable){
                    query ="users(userid, username, __v, emails, firstname, lastname, accountid,created_date) Select userid, username, __v, emails, firstname, lastname, accountid,created_date from temp_users ON CONFLICT(userid) DO UPDATE Set (userid, username, __v, emails, firstname, lastname, accountid,created_date)"+
                            "= (select userid, username, __v, emails, firstname, lastname, accountid,created_date from temp_users t2 where users.userid = t2.userid);"
                    pg.insert(query,"", function(err, r){
                        if(err){ console.log(err)
                                 return err;
                                }
                        console.log("Inserted") 
                        updatedMainTable()
                            });
                    }],
                "droppingTempTable":["insertingToMain",function(aboveResult,droppedTempTable){
                    pg.drop('users',function(err,dropped){
                        if(err){console.log(err)}
                        console.log("Dropped the table")
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
            finalCallback();
    });
}
