/* Following script is fetching quizzes table from mongoDB and parsing it into three tables quizzesschema, quizzesquestions and 
   quizzes_answerlist. it will push into temp tables respectively and then compare to target table and upsert accordingly. After completing
   the upsertion to target tables it will delete the temp tables.
*/
var pg = require('./../../databases/PostgresSqlConnection.js');
var mongoDB = require('../../databases/mongoConnection');
var moment = require('moment')
var async = require('async')
var sanitizer = require('sanitizer');
var ObjectId = require('mongodb').ObjectID;
exports.quizzesData = function(finalCallback){
    async.auto({
        //Following block is creating temp tables for quizzesschema, quizzesquestions and quizzes_answerlist
        createTable:function(cb){
                var create_qQus = "quizzesschema_obj_id varchar(100), quizid varchar(100), questions__sys__id varchar(100), questions__sys__contenttype varchar(100),questions__internaltitle Text, questiontext Text, internaltitle Text, created_date date";
                var create_qAns = "quizzesSchema_obj_id varchar(100),quizid varchar(100), quizzes__answerlist__sys__id varchar(100), questions__sys__id varchar(100), sys__contenttype varchar(100),iscorrectanswer varchar(10), answertextlong text,answertext Text, internaltitle Text, isuseranswer varchar(10)";
                var create_qSch = "quizzesschema_obj_id varchar(100), userid_email varchar(100), quizid varchar(100), passingscore smallint,quizname varchar(100),ispassable varchar(10), passed varchar(10),timestarted date,resultstatusid varchar(100),pointsscored smallint,pointstotal smallint, pointspct int,questionscorrect smallint, questionstotal smallint, questionspct real, __v int, status varchar(100), timecompleted date, created_date date";
                pg.drop("quizzesschema, temp_quizzesquestions, temp_quizzes_answerlist", function(err){
                    if(err){console.log("err")}
                    pg.create("quizzesschema",create_qSch, function(err){
                        if(err){console.log(err)}
                        }) 
                    pg.create("quizzesquestions",create_qQus, function(err){
                        if(err){console.log(err)}
                        }) 
                    pg.create("quizzes_answerlist",create_qAns, function(err){
                        if(err){console.log(err)}
                        console.log("table created")  
                        cb(); 
                        }) 
                    });
            },
        // Following block is fetching collections data from mongodb
        fetchData:["createTable",function(aboveResult,cbData){                
            mongoDB.getData('quizzes',function(err,tdata){
                if (err){console.error('could not connect to postgres', err);}
                    console.log("Total Records : ",tdata.length)
                    cbData(null, tdata)
            })
            }],
        //Following block is parsing data for 3 different tables and pushing into their temp tables repectively.
        insertData:["fetchData",function(aboveResult,cb1){
            var qData = aboveResult.fetchData;   
            async.auto({
                "Quizzes_Schema":function(cbQschma){
                    async.auto({
                        "insertingToTempTable":function(updatedTempTable){
                            var dataQueryArray = [];
                            var start = 0;
                            console.log("Total quizzes_Answer :",qData.length)
                            if(qData.length>500){
                                var last = 500;}
                            else{var last = qData.length}                                                    
                            while(start<last){
                                var qSchemaToBeFetched ="";
                                for(i=start;i<last;i++){
                                    var id = qData[i]['_id']
                                    var userid_email = qData[i]['userId']
                                    var quizid = ""
                                    if(qData[i].hasOwnProperty('quizId'))
                                        quizid = qData[i]['quizId']
                                    var passingscore = 0
                                    if(qData[i].hasOwnProperty('passingScore'))
                                        passingscore = qData[i]['passingScore']
                                    var quizname = ""
                                    if(qData[i].hasOwnProperty('quizName'))
                                        quizname = qData[i]['quizName'].replace(/[&\/\\#,+()$~%.'":*?<>{}[]/g,' ');
                                    var ispassable = qData[i]['isPassable']
                                    var passed = qData[i]['passed']
                                    var timestarted = null;
                                    if(qData[i].hasOwnProperty('timeStarted') && qData[i]['timeStarted'])
                                        timestarted = "'"+qData[i]['timeStarted'].toISOString().substring(0,10)+"'"
                                    var resultstatusid = ""
                                    if(qData[i].hasOwnProperty('resultStatusId'))
                                        resultstatusid = qData[i]['resultStatusId']
                                    var pointsscored = 0
                                    if(qData[i].hasOwnProperty('pointsScored'))
                                        pointsscored = qData[i]['pointsScored']
                                    var pointstotal = 0
                                    if(qData[i].hasOwnProperty('pointsTotal'))
                                        pointstotal = qData[i]['pointsTotal']
                                    var pointspct = 0
                                    if(qData[i].hasOwnProperty('pointsPct'))
                                        pointspct =qData[i]['pointsPct']
                                    var questionscorrect =0
                                    if(qData[i].hasOwnProperty('questionsCorrect'))
                                        questionscorrect = qData[i]['questionsCorrect']
                                    var questionstotal =0
                                    if(qData[i].hasOwnProperty('questionsTotal'))
                                        questionstotal = qData[i]['questionsTotal']
                                    var questionspct = 0
                                    if(qData[i].hasOwnProperty('questionsPct'))
                                        questionspct = qData[i]['questionsPct']
                                    var __v = qData['__v']|0;
                                    var status = ""
                                    if(qData[i].hasOwnProperty('status'))
                                        status = qData[i]['status']
                                    var timecompleted = null
                                    if(qData[i].hasOwnProperty('timeCompleted') && qData[i]['timeCompleted'] != undefined)
                                        timecompleted = "'"+qData[i]['timeCompleted'].toISOString().substring(0,10)+"'"
                                    var created_date =  moment(ObjectId(id).getTimestamp()).format('YYYY-MM-DD');
                                    qSchemaToBeFetched +=" ('"+id+"','"+userid_email+"','"+quizid+"',"+passingscore+",'"+quizname+"','"+ispassable+"','"+passed+"',"+timestarted+",'"+resultstatusid+"',"+pointsscored+","+pointstotal+","+pointspct+","+questionscorrect+","+questionstotal+","+questionspct+","+__v+",'"+status+"',"+timecompleted+",'"+created_date+"'), "
                                } 
                                qSchemaToBeFetched = qSchemaToBeFetched.substring(0,qSchemaToBeFetched.length-2)
                                dataQueryArray.push(qSchemaToBeFetched)
                                start = last;
                                if(qData.length-last>500){
                                    last += 500;}
                                else{last = qData.length;} 
                                
                            }
                            console.log("No. of lists in the array = ",dataQueryArray.length)
                            insert(0)
                            function insert(z){
                                if(z < dataQueryArray.length){
                                    var quizData = "temp_quizzesschema(quizzesschema_obj_id,userid_email,quizid,passingscore,quizname,ispassable,passed,timestarted,resultstatusid,pointsscored,pointstotal,pointspct,questionscorrect,questionstotal,questionspct,__v,status,timecompleted,created_date)values ";
                                    pg.insert(quizData,dataQueryArray[z], function(err, result){
                                        if (err){console.error('could not connect to postgres', err);}
                                        else{
                                            insert(z+1)   }
                                        });
                                } else { 
                                        if(dataQueryArray.length > 0){
                                            pg.Truncate("quizzesschema",function(err,res){
                                                updatedTempTable();
                                            });
                                            
                                        }
                                        else{updatedTempTable();}
                                }
                            }
                            
                            },
                        //following code is upserting data to quizzesschema by comparing it to temp table
                        "insertingToMainTable": ["insertingToTempTable", function(aboveResult,updatedMainTable){
                            var quizSch= `quizzesschema (quizzesschema_obj_id, userid_email, quizid, passingscore, quizname,
                                    ispassable, passed, timestarted, resultstatusid, pointsscored, pointstotal, pointspct,
                                    questionscorrect, questionstotal, questionspct, __v, status, timecompleted, created_date) 
                                    SELECT quizzesschema_obj_id, userid_email, quizid, passingscore, quizname, ispassable,
                                    passed, timestarted, resultstatusid, pointsscored, pointstotal, pointspct, questionscorrect,
                                    questionstotal, questionspct, __v, status, timecompleted, created_date FROM public.temp_quizzesschema;`
                            pg.insert(quizSch,"", function(err, r){
                                if(err){ 
                                        console.log(err)
                                        return err;
                                        }
                                updatedMainTable(null,"done")
                                });
                            }],
                        "updatingUserIds":["insertingToMainTable",function(aboveResult,updatedUserIds){
                            update = "quizzesschema SET userid = (select userid FROM users b WHERE quizzesschema.userid_email = b.username)";
                            pg.update(update,function(err,updated){
                                if (err){console.error('could not connect to postgres', err);} 
                                else{
                                        console.log("Updated userId")
                                        updatedUserIds();
                                    }
                                });
                            }],
                        "droppingTempTable":["updatingUserIds",function(aboveResult,droppedTempTable){
                            pg.drop('quizzesschema',function(err,dropped){
                                if(err){console.log(err)}
                                console.log("Dropped the table")
                                droppedTempTable();
                                }); 
                        }] 
                            
                            },function(err,res){
                                cbQschma(null,"done");
                        })
                    },
                // Following block is parsing data for Quizzes Questions and pushing to temp table.
                "Quizzes_Questions":["Quizzes_Schema",function(aboveResult,cbQus){
                    async.auto({
                        "insertingToTempTable":function(updatedTempTable){
                            console.log("Inside Quizzes_Questions")
                            var dataQueryArray = [];
                            var start = 0;
                            var last = 0
                            if(qData.length>500){
                                last = 500;}
                            else{last = qData.length}                                                    
                            while(start<last){
                                var qQusToBeFetched ="";
                                for(let i=start;i<last;i++){   
                                        let quizzesSchema_obj_id = qData[i]['_id']
                                        let qQus = ""
                                        if(qData[i].hasOwnProperty('questions')){
                                            qQus =  qData[i]['questions']
                                            if(qQus.length>0){
                                                for(j=0;j<qQus.length;j++){
                                                    let qQus_sys_id = qQus[j]['sys']['id']
                                                    var quizid = ""
                                                    if(qData[i].hasOwnProperty('quizId'))
                                                        quizid = qData[i]['quizId']
                                                    let qQus_contenttype = ""
                                                    if(qQus[j]['sys'].hasOwnProperty('contentType'))
                                                        qQus_contenttype = qQus[j]['sys']['contentType'].replace(/[&\/\\#,+()$~%.'":*<>{}[]/g,' ');
                                                    let qQus_internaltitle = ""
                                                    if(qQus[j].hasOwnProperty('internalTitle')){
                                                        qQus_internaltitle =  qQus[j]['internalTitle'].replace(/[&\/\\#,+()$~%.'":*<>{}[]/g,' ');
                                                    }
                                                    let internaltitle = ""
                                                    if(qData[i].hasOwnProperty('internalTitle')){
                                                        internaltitle =  qData[i]['internalTitle'].replace(/[&\/\\#,+()$~%.'":*<>{}[]/g,' ');
                                                    }
                                                    let questiontext = qQus[j]['questionText'].replace(/[&\/\\#,+()$~%.'":*<>{}[]/g,' ');
                                                    let created_date =  moment(ObjectId(quizzesSchema_obj_id).getTimestamp()).format('YYYY-MM-DD');
                                                    qQusToBeFetched +=" ('"+quizzesSchema_obj_id+"','"+quizid+"','"+qQus_sys_id+"','"+qQus_contenttype+"','"+qQus_internaltitle+"','"+questiontext+"','"+internaltitle+"','"+created_date+"'), "
                                                }
                                            } 
                                        }       
                                    }
                                qQusToBeFetched = qQusToBeFetched.substring(0,qQusToBeFetched.length-2)
                                dataQueryArray.push(qQusToBeFetched)
                                start = last;
                                if(qData.length-last>500){
                                    last += 500;}
                                else{last = qData.length;}
                            }
                            console.log("No. of lists in the array = ",dataQueryArray.length)
                            insert(0)
                            function insert(z){
                                if(z < dataQueryArray.length){
                                    var query = "temp_quizzesquestions(quizzesschema_obj_id, quizid, questions__sys__id, questions__sys__contenttype,questions__internaltitle, questiontext, internaltitle, created_date)  values ";
                                    pg.insert(query,dataQueryArray[z], function(err, result){
                                        if (err){console.error('could not connect to postgres', err);}
                                        else{   insert(z+1); }
                                        });
                                    } 
                                    else {
                                            if(dataQueryArray.length>0){ 
                                                pg.Truncate("quizzesquestions",function(err,res){
                                                    updatedTempTable();
                                                });
                                                
                                            }
                                            else{updatedTempTable();}
                                        }
                                }
                                    
                                
                            },
                        // Following block is upserting to quizzesquestions table by comparing it to temp table
                        "insertingTomainTable": ["insertingToTempTable", function(aboveResult,updatedMainTable){
                            var quizzQus=  `quizzesquestions (quizzesschema_obj_id,quizid, questions__sys__id, questions__sys__contenttype, 
                                            questions__internaltitle, questiontext, internaltitle, created_date) SELECT quizzesschema_obj_id,quizid, questions__sys__id, 
                                            questions__sys__contenttype, questions__internaltitle, questiontext, internaltitle, created_date FROM public.temp_quizzesquestions`
                            pg.insert(quizzQus,"", function(err, r){
                                if(err){ console.log(err)
                                            return err;
                                        }
                                console.log("Inserted") 
                                updatedMainTable(null,"done")
                                });
                            }],
                        "droppingTempTable":["insertingTomainTable",function(aboveResult,droppedTempTable){
                            pg.drop('quizzesquestions',function(err,dropped){
                                if(err){console.log(err)}
                                console.log("Dropped the table")
                                droppedTempTable();
                                }); 
                            }] 
                        },
                        function(err,res){
                            cbQus();
                    })
                }],
                
                // Following block is parsing data for Quizzes Answer and pushing to temp table.
                "Quizzes_Answers":["Quizzes_Questions",function(aboveResult,cbA){
                    async.auto({
                            "insertingToTempTable":function(updatedTempTable){
                                var dataQueryArray = [];
                                var start = 0;
                                if(qData.length>500){
                                    var last = 500;}
                                else{var last = qData.length}                                                    
                                while(start<last){
                                    var dataToBeInserted ="";
                                    for(let i=start;i<last;i++){   
                                        let quizzesSchema_obj_id = qData[i]['_id']
                                        let qQus =""
                                        if(qData[i].hasOwnProperty('questions')){
                                            qQus =  qData[i]['questions']
                                            if(qQus.length>0){
                                                for(j=0;j<qQus.length;j++){
                                                    let qAns = ""
                                                    if(qQus[j].hasOwnProperty('answerList')){
                                                        qAns = qQus[j]['answerList']
                                                        if(qAns.length>0){
                                                            for(ans=0;ans<qAns.length;ans++)
                                                            {
                                                                let qAns_sys_id = qAns[ans]['sys']['id']
                                                                var quizid = ""
                                                                if(qData[i].hasOwnProperty('quizId'))
                                                                    quizid = qData[i]['quizId']
                                                                let questions__sys__id = qQus[j]['sys']['id']
                                                                let qAns_contenttype = "";
                                                                if(qAns[ans]['sys'].hasOwnProperty('contentType'))
                                                                    qAns_contenttype = qAns[ans]['sys']['contentType'].replace(/[&\/\\#,+()$~%.'":*?<>{}[]/g,' ')
                                                                let isCorrectAnswer =  qAns[ans]['isCorrectAnswer']
                                                                let qAns_internaltitle = "";
                                                                if(qAns[ans].hasOwnProperty('internalTitle'))
                                                                    qAns_internaltitle = qAns[ans]['internalTitle'].replace(/[&\/\\#,+()$~%.'":*?<>{}[]/g,' ')
                                                                let answertext ="";
                                                                if(qAns[ans].hasOwnProperty('answerText'))
                                                                    answertext = qAns[ans]['answerText'].replace(/[&\/\\#,+()$~%.'":*?<>{}[]/g,' ')
                                                                let answertextlong = ""
                                                                if(qAns[ans].hasOwnProperty('answerTextLong'))
                                                                    answertextlong =  qAns[ans]['answerTextLong'].replace(/[&\/\\#,+()$~%.'":*?<>{}[]/g,' ')
                                                                let qAns_isUserAnswer = ""
                                                                if(qAns[ans].hasOwnProperty('isUserAnswer'))
                                                                    qAns_isUserAnswer = qAns[ans]['isUserAnswer']
                                                                dataToBeInserted +=" ('"+quizzesSchema_obj_id+"','"+quizid+"','"+qAns_sys_id+"','"+questions__sys__id+"','"+qAns_contenttype+"','"+isCorrectAnswer+"','"+qAns_internaltitle+"','"+answertext+"','"+answertextlong+"','"+qAns_isUserAnswer+"'), "
                                                                
                                                                
                                                            }
                                                        }            
                                                    }    
                                                }
                                            }    
                                        }    
                                        }
                                    dataToBeInserted = dataToBeInserted.substring(0,dataToBeInserted.length-2)
                                    dataQueryArray.push(dataToBeInserted)
                                    start = last;
                                    if(qData.length-last>500){
                                        last += 500;}
                                    else{last = qData.length;} 
                                
                                }
                                console.log("No. of lists in the array = ",dataQueryArray.length)
                                insert(0)
                                function insert(z){
                                    if(z < dataQueryArray.length){
                                        var query = "temp_quizzes_answerlist(quizzesSchema_obj_id,quizid, quizzes__answerlist__sys__id, questions__sys__id, sys__contenttype,iscorrectanswer, answertextlong,answertext, internaltitle, isuseranswer)  values ";
                                        pg.insert(query,dataQueryArray[z], function(err, result){
                                            if (err){console.error('could not connect to postgres', err);}
                                            else{   insert(z+1)}
                                        });
                                    } else { 
                                            if(dataQueryArray.length>0){
                                                pg.Truncate("quizzes_answerlist",function(err, res){
                                                    updatedTempTable();
                                                    });
                                                
                                                }
                                            else{updatedTempTable();}
                                            }
                                }   
                            },
                            // following code is upserting the quizzes_answer table by comparing it to temp table
                            "insertingToMainTable": ["insertingToTempTable", function(aboveResult,updatedMainTable){
                                var quizzAns=  `quizzes_answerlist(quizzesschema_obj_id,quizid, quizzes__answerlist__sys__id, questions__sys__id, sys__contenttype, iscorrectanswer, 
                                                answertextlong, answertext, internaltitle, isuseranswer) SELECT quizzesschema_obj_id,quizid, quizzes__answerlist__sys__id, questions__sys__id, 
                                                sys__contenttype, iscorrectanswer, answertextlong, answertext, internaltitle, isuseranswer FROM public.temp_quizzes_answerlist`
                                                
                                pg.insert(quizzAns,"", function(err, r){
                                    if(err){ console.log(err)
                                                return err;
                                            }
                                    console.log("Inserted") 
                                    updatedMainTable(null,"done")
                                    });
                                }],  
                            "droppingTempTable":["insertingToMainTable",function(aboveResult,droppedTempTable){
                                pg.drop('quizzes_answerlist',function(err,dropped){
                                    if(err){console.log(err)}
                                    console.log("Dropped the table")
                                    droppedTempTable();
                                    }); 
                                }] 
                            },
                            function(err,res){
                                cbA();
                    })
                }]
                
                },
                function(error,response){
                    finalCallback(null, "done");
                });
            }]
        },
        function(err,resp){
    });
}                

        