/* Following script is fetching exams collection data from mongoDB and parsing it into four tables 
    examschema, examquestions, exam_answerlist amd exam_qbscores. it will push into temp tables respectively and then 
    compare to target table and upsert accordingly. After completing the upsertion to target table it will delete the temp tables.
*/
var pg = require('./../../databases/PostgresSqlConnection.js');
var mongoDB = require('../../databases/mongoConnection');
var moment = require('moment')
var async = require('async')
var sanitizer = require('sanitizer');
var ObjectId = require('mongodb').ObjectID;
exports.examsData = function(finalCallback){
    async.auto({
        // Following script creating four temp tables for examsschema, examsquestions,examsanswerlist and exam_qbscores
        createTable:function(cb){
            
            var createExams = "exam_obj_id varchar(100),userid_email varchar(100), accountid varchar(100),examid varchar(100),	examtype varchar(100),	certificateid varchar(100),	certificationid varchar(100),	certificationtype varchar(100),certificationname varchar(100),baseprice real,couponcode varchar(100),couponid varchar(100),	isrecertify varchar(100),__v int,finalprice real,couponvalue real,	questionspct real,	questionstotal smallint,	questionscorrect smallint,	pointspct real,	pointstotal smallint,	pointsscored smallint,	resultstatusid varchar(100),	passed varchar(100),	ispassable varchar(100),	passingscore int,recertifydate Date,	retakedate Date,expiresdate Date,timetocomplete Date,	timestarted Date,status varchar(100),timecompleted Date,created_date Date";
            var createQus = "exams_obj_id varchar(100),examid varchar(100),questions__qbid varchar(100),sys__id varchar(100), questions__sys__contenttype Text, questiontext Text, pointvalue smallint, questiontype Text, internaltitle Text";
            var createAns = "exams_obj_id varchar(100),examid varchar(100),questions__qbid varchar(100), question__sys__id varchar(100),answerlist__sys__id varchar(100),answerlist__sys__contenttype text,answerlist__iscorrectanswer varchar(10), answerlist__answertextlong text, answerlist__answertext text, answerlist__internaltitle Text, answerlist__isuseranswer varchar(10),created_date date";
            var createQB = "exams_obj_id varchar(100),examid varchar(100),questionid varchar(100), qbscores_id varchar(100),pointspct varchar(100),questionspct varchar(100),pointstotal varchar(100),pointsscored varchar(100),questionstotal varchar(100),questionscorrect varchar(100)";
            pg.drop("exams, temp_examsquestions, temp_exams_answerlist, temp_examqb_scores", function(err){
                if(err){console.log("err")}
                pg.create("exams",createExams, function(err){
                    if(err){console.log(err)}
                    });
                pg.create("examsquestions",createQus, function(err){
                    if(err){console.log(err)}
                    }); 
                pg.create("exams_answerlist",createAns, function(err){
                    if(err){console.log(err)}
                    }); 
                pg.create("examqb_scores",createQB, function(err){
                    if(err){console.log(err)}
                    cb();
                    }); 
                });
            },
        // Following block is fetching the data from mongoDB Exam collection
        fetchData:["createTable",function(aboveResult,cbData){                
            mongoDB.getData('exams',function(err,tdata){
                if (err){console.error('could not connect to postgres', err);}
                console.log("Total Records : ",tdata.length)
                cbData(null,tdata)
                });
            }],
        //Following script is parsing the exam tables and distributing it into four tables 
        insertData:["fetchData",function(aboveResult,cb1){
            var exam = aboveResult.fetchData;
            async.auto({
                // Following block is parsing the examSchema data and pushing it to temp_exams table
                examData:function(examSchemaDataUpdata){
                    async.auto({
                        "insertingToTempTable":function(dataInserted){
                            var dataQueryArray = [];
                            var start = 0;
                            console.log("Inside exam :  ",exam.length)
                            if(exam.length>500){
                                var last = 500;}
                            else{var last = exam.length}                                                    
                            while(start<last){
                                var examToBeInserted = ""
                                for (i =start; i<last; i++){
                                    var exam_obj_id= exam[i]['_id'];
                                    var userid_email= exam[i]['userId']
                                    var accountid = ""
                                    if(exam[i].hasOwnProperty('accountId'))
                                        { accountid= exam[i]['accountId'] }
                                    var examId= exam[i]['examId']
                                    var examType= exam[i]['examType']
                                    var certificateId= ""
                                    if(exam[i].hasOwnProperty('certificateId'))
                                        certificateId= exam[i]['certificateId']
                                    var certificationId= ""
                                    if(exam[i].hasOwnProperty('certificationId'))
                                        certificationId= exam[i]['certificationId']
                                    var certificationType = ""
                                    if(exam[i].hasOwnProperty('certificationType'))
                                        certificationType= exam[i]['certificationType'].replace(/[&\/\\#,+()$~%.'":*?<>{}[]/g,' ');
                                    var certificationname = "";
                                    if(exam[i].hasOwnProperty('certificationName')){
                                            if(exam[i]['certificationName']){
                                                    certificationname= exam[i]['certificationName'].replace(/[&\/\\#,+()$~%.'":*?<>{}[]/g,' ');
                                                }
                                        }
                                    var basePrice= exam[i]['basePrice']|0;
                                    var couponCode= ""
                                    if(exam[i].hasOwnProperty('couponCode'))
                                        couponCode= exam[i]['couponCode'].replace(/[&\/\\#,+()$~%.'":*?<>{}[]/g,' ');
                                    var couponId =""
                                    if(exam[i].hasOwnProperty('couponId'))
                                            couponId= exam[i]['couponId']
                                    var isReCertify= exam[i]['isReCertify']|"";
                                    var __v= exam[i]['__v']|0;
                                    var finalPrice= exam[i]['finalPrice']|0;
                                    var couponValue= exam[i]['couponCodeValue']|0;
                                    var questionsPct= 0;
                                    if(exam[i].hasOwnProperty('questionsPct'))
                                        questionsPct= exam[i]['questionsPct']
                                    var questionsTotal=0;
                                    if(exam[i].hasOwnProperty('questionsTotal'))
                                        questionsTotal= exam[i]['questionsTotal']
                                    var questionsCorrect= 0;
                                    if(exam[i].hasOwnProperty('questionsCorrect'))
                                        questionsCorrect= exam[i]['questionsCorrect']
                                    var pointsPct= 0;
                                    if(exam[i].hasOwnProperty('pointsPct'))
                                        pointsPct= exam[i]['pointsPct']
                                    var pointsTotal= 0;
                                    if(exam[i].hasOwnProperty('pointsTotal'))
                                        pointsTotal= exam[i]['pointsTotal']
                                    var pointsScored= 0;
                                    if(exam[i].hasOwnProperty('pointsScored'))
                                        pointsScored= exam[i]['pointsScored']
                                    var resultStatusId= ""
                                    if(exam[i].hasOwnProperty('resultStatusId'))
                                        resultStatusId= exam[i]['resultStatusId']
                                    var passed= ""
                                    if(exam[i].hasOwnProperty('passed'))
                                        passed= exam[i]['passed']
                                    var isPassable= ""
                                    if(exam[i].hasOwnProperty('isPassable'))
                                        isPassable= exam[i]['isPassable']
                                    var passingScore=0;
                                    if(exam[i].hasOwnProperty('passingScore'))
                                        passingScore= exam[i]['passingScore']
                                    let recertifyDate=null;
                                    if(exam[i].hasOwnProperty('recertifyDate') && exam[i]['recertifyDate'] != undefined){
                                        recertifyDate= "'"+exam[i]['recertifyDate'].toISOString().substring(0,10)+"'";}
                                    let retakeDate= null;
                                    if(exam[i].hasOwnProperty('retakeDate') && exam[i]['retakeDate'] !=undefined){
                                        retakeDate= "'"+exam[i]['retakeDate'].toISOString().substring(0,10)+"'";}
                                    let expiresDate= null;
                                    if(exam[i].hasOwnProperty('expiresDate') && exam[i]['expiresDate'] !=undefined){
                                        expiresDate= "'"+exam[i]['expiresDate'].toISOString().substring(0,10)+"'";}
                                    let timeToComplete= null;
                                    if(exam[i].hasOwnProperty('timeToComplete')  && exam[i]['timeToComplete'] !=undefined){
                                        timeToComplete= "'"+exam[i]['timeToComplete'].toISOString().substring(0,10)+"'";}
                                    let timeStarted= null;
                                    if(exam[i].hasOwnProperty('timeStarted') && exam[i]['timeStarted'] !=undefined){
                                        timeStarted= "'"+exam[i]['timeStarted'].toISOString().substring(0,10)+"'";}
                                    var status= ""
                                    if(exam[i].hasOwnProperty('status'))
                                        status= exam[i]['status'].replace(/[&\/\\#,+()$~%.'":*?<>{}[]/g,'');
                                            
                                    let timeCompleted= null;
                                    if(exam[i].hasOwnProperty('timeCompleted') && exam[i]['timeCompleted'] !=undefined){
                                        timeCompleted= "'"+exam[i]['timeCompleted'].toISOString().substring(0,10)+"'"}
                                    var createddate = moment(ObjectId(exam_obj_id).getTimestamp()).format('YYYY-MM-DD')
                                    examToBeInserted +=" ('"+exam_obj_id+"','"+userid_email+"','"+ accountid+"','"+examId+"','"+	examType+"','"+	certificateId+"','"+	certificationId+"','"+	certificationType+"','"+certificationname+"','"+basePrice+"','"+couponCode+"','"+couponId+"','"+	isReCertify+"','"+	__v+"','"+finalPrice+"','"+couponValue+"','"+	questionsPct+"','"+	questionsTotal+"','"+	questionsCorrect+"','"+	pointsPct+"','"+	pointsTotal+"','"+	pointsScored+"','"+	resultStatusId+"','"+	passed+"','"+	isPassable+"','"+	passingScore+"',"+	recertifyDate+","+	retakeDate+","+	expiresDate+","+	timeToComplete+","+	timeStarted+",'"+	status+"',"+	timeCompleted+",'"+createddate+"') , "
                                }
                                examToBeInserted = examToBeInserted.substring(0,examToBeInserted.length-2)
                                dataQueryArray.push(examToBeInserted)
                                start = last;
                                if(exam.length-last>500){
                                    last += 500;}
                                else{last = exam.length;}   
                            }
                            console.log("No. of lists in the array = ",dataQueryArray.length)
                            insert(0)
                            function insert(z){
                                if(z < dataQueryArray.length){
                                    var examData = "temp_exams(exam_obj_id,userid_email, accountid,examid,	examtype,	certificateid,	certificationid,	certificationtype,certificationname,baseprice,couponcode,couponid,	isrecertify,	__v,finalprice,couponvalue,	questionspct,	questionstotal,	questionscorrect,	pointspct,	pointstotal,	pointsscored,	resultStatusid,	passed,	ispassable,	passingscore,	recertifydate,	retakedate,	expiresdate,	timetocomplete,	timestarted,	status,	timecompleted,created_date)  values ";
                                    pg.insert(examData,dataQueryArray[z], function(err, result){
                                            if (err){console.error('could not connect to postgres', err);}
                                            else{   insert(z+1)    }
                                        });
                                }   
                                else{
                                    console.log("Transefering to main table");
                                    pg.Truncate("exams",function(err){
                                        dataInserted();
                                    })    
                                }
                            }
                        },
                        //Following code is comparing postgres exam table with temp_exam table and upserting data to exam table
                        "insertingToMainTable": ["insertingToTempTable", function(aboveResult,examSchemaDataInserted){
                            examQuery = "exams(exams_obj_id,userid_email,accountid,examid,examtype,certificateid,certificationid,certificationtype,certificationname,baseprice,"+
                            "couponcode,couponid,isrecertify,__v,finalprice,questionspct,questionstotal,questionscorrect,pointspct,pointstotal,pointsscored,resultstatusid,passed,ispassable,"+
                            "passingscore,recertifydate,retakedate,expiresdate,timetocomplete,timestarted,status,timecompleted,created_date) "+
                            "Select exam_obj_id,userid_email,accountid,examid,examtype,certificateid,certificationid,certificationtype,certificationname,baseprice,"+
                            "couponcode,couponid,isrecertify,__v,finalprice,questionspct,questionstotal,questionscorrect,pointspct,pointstotal,pointsscored,"+
                            "resultstatusid,passed,ispassable,passingscore,recertifydate,retakedate,expiresdate,timetocomplete,timestarted,status,timecompleted,created_date "+ 
                            "from temp_exams";
                            pg.insert(examQuery,"", function(err, r)
                            {
                                if(err){ console.log(err)
                                            return err;
                                        }
                                examSchemaDataInserted(null,"done")
                                
                            });
                          
                        }],
                        "updatingUserIds":["insertingToMainTable",function(aboveResult,updatedUserIds){
                            update = "exams SET userid = (select userid FROM users b WHERE exams.userid_email = b.username)";
                            pg.update(update,function(err,updated){
                                if (err){console.error('could not connect to postgres', err);} 
                                console.log("Updated userId")
                                updatedUserIds();
                                });
                            }],
                        "droppingTempTable":["updatingUserIds",function(aboveResult,droppedTempTable){
                            pg.drop('exams',function(err,dropped){
                                if(err){console.log(err)}
                                console.log("Dropped the table")
                                droppedTempTable();
                                }); 
                            }]                 
                            
                    }, 
                    function(err,res){
                        
                        examSchemaDataUpdata();
                    })
                    },
                // Following block is parsing the examQuestions data and pushing it to temp_exams table
                //examQuestionsData:function(examQusDataUpdata){
                examQuestionsData:["examData",function(aboveResult,examQusDataUpdata){
                    async.auto({
                                "insertingToTempTable":function(tempExamQuesUpdated){
                                    var dataQueryArray = [];
                                    var start = 0;
                                    console.log("Inside examQus :  ",exam.length)
                                    if(exam.length>500){
                                        var last = 500;}
                                    else{var last = exam.length}                                                    
                                    while(start<last)
                                    {
                                        var examQusToBeInserted = ""
                                        for(var i=start;i<last;i++)
                                            {
                                                if(exam[i].hasOwnProperty('questions'))
                                                {  
                                                    qus = exam[i]['questions'];
                                                    if(qus.length > 0)
                                                    {
                                                        for (let j=0; j<qus.length;j++)
                                                            {
                                                                let exam_Obj_Id = exam[i]['_id'];
                                                                let examid = exam[i]['examId'];
                                                                let qbId = qus[j]['qbId'];
                                                                let sys_id = qus[j]['sys']['id'];
                                                                let sys_contenttype = ""
                                                                if(qus[j]['sys'].hasOwnProperty('contentType'))
                                                                sys_contenttype = qus[j]['sys']['contentType'].replace(/[&\/\\#,+()$~%.'":*<>{}[]/g,' ');
                                                                let questiontext="";
                                                                if(qus[j]['questionText'])
                                                                questiontext = qus[j]['questionText'].replace(/[&\/\\#,+()$~%.'":*<>{}[]/g,' ');;
                                                                let pointValue = qus[j]['pointValue'] | "";
                                                                let questiontype="";
                                                                if(qus[j]['questionType'])
                                                                questiontype = qus[j]['questionType'].replace(/[&\/\\#,+()$~%.'":<>{}[]/g,' ');;
                                                                let internaltitle="";
                                                                if(qus[j]['internalTitle'])
                                                                internaltitle = qus[j]['internalTitle'].replace(/[&\/\\#,+()$~%.'":*<>{}[]/g,' ');
                                                                examQusToBeInserted +=" ('"+exam_Obj_Id+"','"+examid+"','"+qbId+"','"+sys_id+"','"+sys_contenttype+"','"+questiontext+"','"+pointValue+"','"+questiontype+"','"+internaltitle+"') , "
                                                            }
                                                    }	
                                                }		
                                            }
                                            examQusToBeInserted = examQusToBeInserted.substring(0,examQusToBeInserted.length-2)
                                            dataQueryArray.push(examQusToBeInserted)
                                            start = last;
                                            if(exam.length-last>500){
                                                last += 500;}
                                            else{last = exam.length;}  
                                    }
                                    //Inserting data to temp table
                                    console.log("No. of lists in the array = ",dataQueryArray.length)
                                    insert(0)
                                    function insert(z){
                                        if(z < dataQueryArray.length){
                                            var examQus = "temp_examsquestions(exams_obj_id ,examid,questions__qbid , sys__id ,questions__sys__contenttype , questiontext , pointvalue , questiontype , internaltitle )  values ";
                                            pg.insert(examQus,dataQueryArray[z], function(err, result){
                                                if (err){console.error('could not connect to postgres', err);}
                                                else{   insert(z+1);    }
                                            });
                                        } else { 
                                                if(dataQueryArray.length>0){
                                                    pg.Truncate("examsquestions",function(err){
                                                        tempExamQuesUpdated();
                                                    });
                                                }
                                                else{tempExamQuesUpdated();}
                                            }
                                    }
                                    
                                },
                                //Following code upserting the data to examsquestions table by comparing with temp_examquestions table
                                "insertingToMainTable": ["insertingToTempTable", function(aboveResult,mainExamQuesUpdated){
                                    examQusData = "examsquestions Select * from temp_examsquestions";
                                    pg.insert(examQusData,"", function(err, r){
                                        if(err){ console.log(err)
                                                    return err;}
                                        mainExamQuesUpdated(); 
                                        
                                    });
                                        
                                }],
                                "droppingTempTable":["insertingToMainTable",function(aboveResult,droppedTempTable){
                                    pg.drop('examsquestions',function(err,dropped){
                                        if(err){console.log(err)}
                                        console.log("Dropped the table")
                                        droppedTempTable();
                                        }); 
                                    }]                                  
                                
                            },
                            function(err,res){
                                examQusDataUpdata(null,"done");
                            })
                    }],
                         //},
                // Following block is parsing the examAnswers data and pushing it to temp_exams_answerlist table
                examAnswerData:["examQuestionsData",function(aboveResult,examAnswerlistUpdated){
                    async.auto({
                        // Following block is parsing exams_answerlist data from exam table and pushing it to temp_exams_answerlist
                        "insertingToTempTable":function(tempExamAnsUpdated){
                            console.log("Inside examAns :  ",exam.length)
                            var dataQueryArray = [];
                            var start = 0;
                            if(exam.length>500){
                                var last = 500;}
                            while(start<last)
                            {
                                var examAnsToBeInserted ="";
                                for(i=start;i<last;i++){
                                    let qus = "";
                                    if(exam[i].hasOwnProperty('questions')){
                                        qus = exam[i]['questions'];
                                        if(qus.length>0)
                                        {
                                            for (j=0; j<qus.length;j++){
                                                let ans = qus[j]['answerList']
                                                for(let x = 0; x<ans.length;x++){
                                                    let exam_obj_id = exam[i]['_id']
                                                    let questions_sys_id = qus[j]['sys']['id']
                                                    let examid = exam[i]['examId']
                                                    let qbId = qus[j]['qbId'];
                                                    let answerlist_sys_id = ans[x]['sys']['id']
                                                    let answerlist_sys_contentType = "";
                                                    if(ans[x].hasOwnProperty('contentType'))
                                                        answerlist_sys_contentType =ans[x]['sys']['contentType'].replace(/[&\/\\#,+()$~%.'":*?<>{}[]/g,' ');
                                                    answerlist_sys_contentType = sanitizer.escape(answerlist_sys_contentType)
                                                    let answerlist_iscorrectanswer = ans[x]['isCorrectAnswer']
                                                    let answerlist_answertextLong = "";
                                                    if(ans[x].hasOwnProperty('answerTextLong'))
                                                        answerlist_answertextLong = ans[x]['answerTextLong'].replace(/[&\/\\#,+()$~%.'":*?<>{}[]/g,' ');
                                                        answerlist_answertextLong = sanitizer.escape(answerlist_answertextLong)
                                                    let answerlist_answertext = "";
                                                    if(ans[x].hasOwnProperty('answerText'))
                                                        answerlist_answertext = ans[x]['answerText'].replace(/[&\/\\#,+()$~%.'":*?<>{}[]/g,' ')
                                                        answerlist_answertext = sanitizer.escape(answerlist_answertext)
                                                    let answerlist_internaltitle = "";
                                                    if(ans[x].hasOwnProperty('internalTitle'))
                                                        answerlist_internaltitle = ans[x]['internalTitle'].replace(/[&\/\\#,+()$~%.'":*?<>{}[]/g,' ');
                                                        answerlist_internaltitle = sanitizer.escape(answerlist_internaltitle)
                                                    let answerlist_isuseranswer = "";
                                                    if(exam[i].hasOwnProperty('isUserAnswer'))
                                                        if(exam[i].hasOwnProperty('isUserAnswer')=="t" || exam[i].hasOwnProperty('isUserAnswer')==1){
                                                            answerlist_isuseranswer = 1;
                                                        }
                                                        else{
                                                            answerlist_isuseranswer = 0;
                                                        }
                                                    examAnsToBeInserted +=" ('"+exam_obj_id+"','"+examid+"','"+qbId+"','"+questions_sys_id+"','"+answerlist_sys_id+"','"+answerlist_sys_contentType+"','"+answerlist_iscorrectanswer+"','"+answerlist_answertextLong+"','"+answerlist_answertext+"','"+answerlist_internaltitle+"','"+answerlist_isuseranswer+"') , "
                                                }
                                            }
                                        }
                                    }
                                }
                                examAnsToBeInserted = examAnsToBeInserted.substring(0,examAnsToBeInserted.length-2)
                                dataQueryArray.push(examAnsToBeInserted) 
                                start = last;
                                if(exam.length-last>500){
                                    last += 500;}
                                else{last = exam.length;}   
                            }  
                            //following code pushing the data into temp_exams_answerlist table
                            console.log("No. of lists in the array = ",dataQueryArray.length)
                            insert(0)
                            function insert(z){
                                if(z < dataQueryArray.length){
                                    var examAns = "temp_exams_answerlist(exams_obj_id, examid, questions__qbid, question__sys__id, answerlist__sys__id, answerlist__sys__contenttype, answerlist__iscorrectanswer, answerlist__answertextlong, answerlist__answertext, answerlist__internaltitle, answerlist__isuseranswer)  values ";
                                    pg.insert(examAns,dataQueryArray[z], function(err, result){
                                            if (err){console.error('could not connect to postgres', err);}
                                            else{   insert(z+1);  }
                                        });
                                } else { 
                                        if(dataQueryArray.length>0){    
                                            console.log("Transfering Data to Main Table ");
                                            pg.Truncate("exams_answerlist",function(err){
                                                tempExamAnsUpdated();
                                            });
                                        }
                                        else{tempExamAnsUpdated();}
                                        
                                }
                            }
                            },
                        // following block is upserting data to exams_answerlist table by comparing it to temp table(containing current fetched data)
                        "insertingToMainTable": ["insertingToTempTable", function(aboveResult,mainExamAnsUpdated){
                            examAnswer="exams_answerlist SELECT exams_obj_id, examid, questions__qbid, question__sys__id,"+
                                    "answerlist__sys__id, answerlist__sys__contenttype, answerlist__iscorrectanswer,answerlist__answertextlong,"+
                                    "answerlist__answertext, answerlist__internaltitle, answerlist__isuseranswer FROM public.temp_exams_answerlist ";
                                         
                            console.log("inserting into main table")
                            pg.insert(examAnswer,"", function(err, r){
                                if(err){ console.log(err)
                                            return err;
                                        }
                                mainExamAnsUpdated();
                                 
                            });
                            }],
                        "droppingTempTable":["insertingToMainTable",function(aboveResult,droppedTempTable){
                            pg.drop('exams_answerlist',function(err,dropped){
                                if(err){console.log(err)}
                                console.log("Dropped the table")
                                droppedTempTable();
                                }); 
                            }]                            
                        
                            },
                            function(err,res){
                            console.log("Moving to get QB_Score Data")
                            examAnswerlistUpdated();
                        })
                    }],
                // Following block is parsing the exam_QBScore data and pushing it to temp_exams_qbscores table
                examQBData:["examAnswerData",function(aboveResult,examQBScoreUpdated){
                    async.auto({
                        "insertingToTempTable":function(tempExamQBScoreInserted){
                            console.log("Inside examQB :  ",exam.length)
                            var dataQueryArray = [];
                            var start = 0;
                            if(exam.length>500){
                                var last = 500;}
                            else{var last = exam.length}                                                    
                            while(start<last){
                                var examQBSToBeInserted ="";
                                for(i=start;i<last;i++){
                                    if(exam[i].hasOwnProperty('qbScores')){
                                        let rows = Object.keys(exam[i]['qbScores'])
                                        for(qbscore = 0;qbscore<rows.length;qbscore++){
                                            let exam_obj_id = exam[i]['_id']
                                            let examid = exam[i]['examId']
                                            let questionid = rows[qbscore]
                                            let qbscores_id = rows[qbscore]
                                            let pointspct = exam[i]['qbScores'][rows[qbscore]]['pointsPct'];
                                            let questionspct = exam[i]['qbScores'][rows[qbscore]]['questionsPct'];
                                            let pointstotal =	exam[i]['qbScores'][rows[qbscore]]['pointsTotal'];
                                            let pointsscored = exam[i]['qbScores'][rows[qbscore]]['pointsScored'];
                                            let questionstotal = exam[i]['qbScores'][rows[qbscore]]['questionsTotal'];
                                            let questionscorrect =	exam[i]['qbScores'][rows[qbscore]]['questionsCorrect'];
                                            examQBSToBeInserted +=" ('"+exam_obj_id+"','"+examid+"','"+questionid+"','"+qbscores_id+"','"+pointspct+"','"+questionspct+"','"+pointstotal+"','"+pointsscored+"','"+questionstotal+"','"+questionscorrect+"') , "
                                        }
                                    }
                                            
                                }
                                examQBSToBeInserted = examQBSToBeInserted.substring(0,examQBSToBeInserted.length-2)
                                dataQueryArray.push(examQBSToBeInserted)
                                start = last;
                                if(exam.length-last>500){
                                    last += 500;}
                                else{last = exam.length;}   		
                            } 
                            //following code is pushing data to temp_exams_qbscore table
                            console.log("No. of lists in the array = ",dataQueryArray.length)
                            insert(0)
                            function insert(z){
                                if(z < dataQueryArray.length){
                                    var qbscores= "temp_examqb_scores(exams_obj_id,examid,questionid, qbscores_id,pointspct,questionspct,pointstotal,pointsscored,questionstotal,questionscorrect) values ";
                                    pg.insert(qbscores,dataQueryArray[z], function(err, result){
                                        if (err){console.error('could not connect to postgres', err);}
                                        else{   insert(z+1)     }
                                        });
                                } else { 
                                        if(dataQueryArray.length>0){
                                            pg.Truncate("examqb_scores",function(err){
                                                tempExamQBScoreInserted(null,"done");
                                            });
                                        }
                                        else{tempExamQBScoreInserted(null,"done");}
                                        
                                }
                             }
                            },
                        //following code upserting data to exams_qbscores table by comparing the data inside temp_exams_qbscores table(currently fetched)
                        "insertingToMainTable": ["insertingToTempTable", function(aboveResult,mainExamQBScoreInserted){
                            examQB= "examqb_scores SELECT exams_obj_id, examid,questionid, qbscores_id, pointspct, questionspct,"+ 
                                    "pointstotal,pointsscored, questionstotal, questionscorrect FROM public.temp_examqb_scores";
                            pg.insert(examQB, "",function(err, r){
                                if(err){ console.log(err)
                                            return err;}
                                mainExamQBScoreInserted(null,"done");
                            });
                            }],
                        "droppingTempTable":["insertingToMainTable",function(aboveResult,droppedTempTable){
                            pg.drop('examqb_scores',function(err,dropped){
                                if(err){console.log(err)}
                                console.log("Dropped the table")
                                droppedTempTable();
                                }); 
                            }]                      
                        
                            },
                            function(err,res){
                                finalCallback();      
                            })
                    }]
                    },
                    function(error,response){
                        console.log("moving to next")
                    });
                    }]
            

                },
                function(err,resp){
            
              
    });
}

 