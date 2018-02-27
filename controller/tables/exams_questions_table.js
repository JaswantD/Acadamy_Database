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
        dbo.collection('exams').find({}).toArray(function(err, exam){
                if (err) throw err;
                //console.log(exam.length)		
                console.log("inserting Exam_Questions data");
                var createQus = "CREATE TABLE IF NOT EXISTS temp_exam_Questions(exams_obj_id varchar(100),questions__qbid varchar(100), questions__sys__contenttype varchar(100), sys__id varchar(100), questiontext varchar(100), pointvalue smallint, questiontype varchar(100), internaltitle varchar(100), created_date date)";
                var createAns = "CREATE TABLE IF NOT EXISTS temp_exam_Answerlist(exams_obj_id varchar(100),questions__qbid varchar(100), question__sys__id varchar(100),answerlist__sys__id varchar(100),answerlist__sys__locale varchar(100),answerlist__sys__contenttype varchar(100),answerlist__iscorrectanswer bit, answerlist__answertextlong text, answerlist__answertext text, answerlist__internaltitle varchar(100), answerlist__isuseranswer bit,created_date date)";
                var createQB = "CREATE TABLE IF NOT EXISTS temp_examqb_scores(exams_obj_id varchar(100),questionid varchar(100), qbscores_id varchar(100), pointspct varchar(100), questionspct varchar(100), pointstotal varchar(100), pointsscored varchar(100), questionstotal varchar(100),questionscorrect varchar(100))";
                pgClient.query(createQus, function(err){
                    if(err){console.log(err)}
                    
                }) 
                pgClient.query(createAns, function(err){
                    if(err){console.log(err)}
                    
                }) 
                pgClient.query(createQB, function(err){
                    if(err){console.log(err)}
                    
                }) 
                async.auto({
                        "QuestionsData":function(cb1)
                            {
                             for(let i=0; i<exam.length;i++)
                                {
                                    console.log(exam.length)
                                    if(exam[i].hasOwnProperty('questions'))
                                        {
                                            //console.log(obj[i]['questions'].length)
                                            qus = exam[i]['questions'];
                                            if(qus.length > 0)
                                                {
                                                console.log(qus.length)
                                                for (let j=0; j<qus.length;j++)
                                                    {
                                                        let exam_id = exam[i]['examId']
                                                        let qbId = qus[j]['qbId'];
                                                        let sys_id = qus[j]['sys']['id'];
                                                        let sys_contenttype = qus[j]['sys']['contentType'];
                                                        let questiontext = qus[j]['questionText'];
                                                        let pointValue = qus[j]['pointValue']
                                                        let questiontype = qus[j]['questionType'];
                                                        let internaltitle = qus[j]['internalTitle']
                                                        //let categorytopics = qus[j]['categoryTopics'];
                                                        //let categoryskills = qus[j]['categorySkills'];
                                                        //let categoryproducts = qus[j]['categoryProducts'][0]['name']
                                                        let createddate =  moment(ObjectId(exam_id).getTimestamp()).format('YYYY-MM-DD');
                                                        var query = "insert into temp_exam_Questions(exam_obj_id, question__qbid, questions__sys__contenttype, sys__id, questiontext, pointvalue, questiontype, internaltitle,createddate)  values($1,$2,$3,$4,$5,$6,$7,$8,$9);"
                                                        pgClient.query(query,[exam_id,qbId,sys_contenttype,sys_id,questiontext,pointValue,questiontype,internaltitle, createddate], function(err, result){
                                                                if (err){console.error('could not connect to postgres', err);}
                                                                else{console.log("..");}
                                                        });
                                                        
                                                    }
                                                    cb1(null,"done")
                                            }
                                        }
                                }                        
                            },
                            
                            
                            "AnswerData":function(cb1)
                            {
                             for(let i=0; i<exam.length;i++)
                                {
                                    console.log(exam.length)
                                    if(exam[i].hasOwnProperty('questions'))
                                        {
                                            //console.log(obj[i]['questions'].length)
                                            qus = exam[i]['questions'];
                                            if(qus.length > 0)
                                                {
                                                    console.log(qus.length)
                                                    let ans = qus[j]['answerList']
                                                    for (let j=0; j<qus.length;j++)
                                                        {
                                                            let ans = qus[j]['answerList']
                                                            for(let x = 0; x<1;x++)//ans.length; x++)
                                                                {
                                                                    let exam_id = exam[i]['examId']
                                                                    let qbId = qus[j]['qbId'];
                                                                    let qus_sys_id = qus[j]['sys']['id'];
                                                                    let answerlist_sys_id = ans[x]['sys']['id']
                                                                    let answerlist_sys_contentType =ans[x]['sys']['contentType']
                                                                    let answerlist_iscorrectanswer = ans[x]['isCorrectAnswer']
                                                                    let answerlist_answertextLong = ""
                                                                    if(ans[x].hasOwnProperty('answerTextLong'))
                                                                        {
                                                                            let answerlist_answertextLong = ans[x]['answerTextLong']
                                                                        }    
                                                                    let answerlist_answertext = ans[x]['answerText']
                                                                    let answerlist_internaltitle = ans[x]['internalTitle']
                                                                    let answerlist_isuseranswer = ans[x]['isUserAnswer']
                                                                    var query = "insert into temp_exam_Answerlist(exams_obj_id, questions__qbid, question__sys__id, answerlist__sys__id, answerlist__sys__contenttype, answerlist__iscorrectanswer, answerlist__answertextlong, answerlist__answertext, answerlist__internaltitle, answerlist__isuseranswer)  values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10);"
                                                                    pgClient.query(query,[exam_id,qbId,questions_sys_id,answerlist_sys_id,answerlist_sys_contentType,answerlist_iscorrectanswer,answerlist_answertextLong,answerlist_answertext,answerlist_internaltitle,answerlist_isuseranswer], function(err, result){
                                                                            if (err){console.error('could not connect to postgres', err);}
                                                                            else{console.log("..");}
                                                                    });
                                                                    
                                                                }
                                                                cb1(null,"done")
                                                        }    
                                                }  
                                                                  
                                        }
                                }                        
                            },
                            
                           "QBScores":function(cb1)
                           {
                            for(let i=0; i<exam.length;i++)
                               {
                                   console.log(exam.length)
                                   if(exam[i].hasOwnProperty('qbScores'))
                                       {
                                           //console.log(obj[i]['questions'].length)
                                           rows = exam[i]['qbScores'];
                                           if(rows.length > 0)
                                               {
                                               console.log(rows.length)
                                               for (let qbscore=0; qbscore<rows.length;qbscore++)
                                                   {
                                                    let exam_id = obj[i]['examId']
                                                    let questionid = rows[qbscore]
                                                    let qbscores_id = rows[qbscore]
                                                    let pointspct = obj[i]['qbScores'][rows[qbscore]]['pointsPct'];
                                                    let questionspct = obj[i]['qbScores'][rows[qbscore]]['questionsPct'];
                                                    let pointstotal =	obj[i]['qbScores'][rows[qbscore]]['pointsTotal'];
                                                    let pointsscored = obj[i]['qbScores'][rows[qbscore]]['pointsScored'];
                                                    let questionstotal = obj[i]['qbScores'][rows[qbscore]]['questionsTotal'];
                                                    let questionscorrect =	obj[i]['qbScores'][rows[qbscore]]['questionsCorrect'];
                                                    //let createddate = ObjectId(exam_id).getTimestamp()
                                                    //console.log(exam_id,qbscores_id,"\t",pointspct,"\t\t",questionspct,"\t",pointstotal,"\t\t",pointsscored,"\t\t",questionstotal,"\t\t",questionscorrect)
                                                    var query = "insert into temp_examqb_scores(exams_obj_id,questionid, qbscores_id,pointspct,questionspct,pointstotal,pointsscored,questionstotal,questionscorrect) values($1,$2,$3,$4,$5,$6,$7,$8,$9);"
                                                    //console.log(query)
                                                        
                                                    pgClient.query(query,[exam_id,questionid,qbscores_id,pointspct,questionspct,pointstotal,pointsscored,questionstotal,questionscorrect],function(err,result){
                                                            if (err){  console.error('could not connect to postgres', err);	}
                                                            else{console.log("..")}
                                                       });
                                                       
                                                   }
                                                   cb1(null,"done")
                                           }
                                       }
                               }                        
                           },
                            "second":["QuestionsData",function(aboveResult,cb2){
                                examQus ="INSERT INTO earnedcertificates "+
                                        "Select earnedcertificates_obj_id, userid_email, examid, examtype, certificateid, certificationid, certificationtype, dateearned, dateexpires, examresultsid, __v, certificationname, created_date from temp_earnedcertificates ON CONFLICT(earnedcertificates_obj_id)"+
                                        "DO UPDATE Set (earnedcertificates_obj_id, userid_email, examid, examtype, certificateid, certificationid,"+ 
                                        "certificationtype, dateearned, dateexpires, examresultsid, __v, certificationname, created_date)"+
                                        "= (select earnedcertificates_obj_id, userid_email, examid, examtype, certificateid, certificationid,"+ 
                                        "certificationtype, dateearned, dateexpires, examresultsid, __v, certificationname, created_date "+
                                        "from temp_earnedcertificates t2 where earnedcertificates.earnedcertificates_obj_id = t2.earnedcertificates_obj_id);"
                                pgClient.query(examQus, function(err, r)
                                {
                                    if(err){ console.log(err)
                                                return err;
                                            }
                                    console.log("Inserted") 
                                    pgClient.query("DROP TABLE temp_earnedcertificates;");   
                                })

                                examAns ="INSERT INTO earnedcertificates "+
                                    "Select earnedcertificates_obj_id, userid_email, examid, examtype, certificateid, certificationid, certificationtype, dateearned, dateexpires, examresultsid, __v, certificationname, created_date from temp_earnedcertificates ON CONFLICT(earnedcertificates_obj_id)"+
                                    "DO UPDATE Set (earnedcertificates_obj_id, userid_email, examid, examtype, certificateid, certificationid,"+ 
                                    "certificationtype, dateearned, dateexpires, examresultsid, __v, certificationname, created_date)"+
                                    "= (select earnedcertificates_obj_id, userid_email, examid, examtype, certificateid, certificationid,"+ 
                                    "certificationtype, dateearned, dateexpires, examresultsid, __v, certificationname, created_date "+
                                    "from temp_earnedcertificates t2 where earnedcertificates.earnedcertificates_obj_id = t2.earnedcertificates_obj_id);"
                                pgClient.query(examAns, function(err, r)
                                {
                                    if(err){ console.log(err)
                                                return err;
                                            }
                                    console.log("Inserted") 
                                    pgClient.query("DROP TABLE temp_earnedcertificates;");    
                                })

                                examQB ="INSERT INTO earnedcertificates "+
                                    "Select earnedcertificates_obj_id, userid_email, examid, examtype, certificateid, certificationid, certificationtype, dateearned, dateexpires, examresultsid, __v, certificationname, created_date from temp_earnedcertificates ON CONFLICT(earnedcertificates_obj_id)"+
                                    "DO UPDATE Set (earnedcertificates_obj_id, userid_email, examid, examtype, certificateid, certificationid,"+ 
                                    "certificationtype, dateearned, dateexpires, examresultsid, __v, certificationname, created_date)"+
                                    "= (select earnedcertificates_obj_id, userid_email, examid, examtype, certificateid, certificationid,"+ 
                                    "certificationtype, dateearned, dateexpires, examresultsid, __v, certificationname, created_date "+
                                    "from temp_earnedcertificates t2 where earnedcertificates.earnedcertificates_obj_id = t2.earnedcertificates_obj_id);"
                                pgClient.query(examQB, function(err, r)
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
        