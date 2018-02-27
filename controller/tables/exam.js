var fs = require('fs');

var file = "E:\\nodejs\\Exams.json";//".\\previous\\Exams.json";//
ObjectId = require('mongodb').ObjectID;
var pg = require('pg')	
var moment = require('moment')
var connectionString = 'postgres://u1bv8dublfgba0:pad39770d44b5a6fa47680ec00d961d88e1ad7f3126aa9ccab7857586718f5e23@ec2-34-236-4-159.compute-1.amazonaws.com:5432/de4eb90mqm9nim';
pg.defaults.ssl = true;
var pgClient = new pg.Client(connectionString);	
pgClient.connect(function(err){
	if (err){
    			return console.error('could not connect to postgres', err);
  		  }
    console.log("connected")
	fs.readFile(file, function (err, data)
	{
		if (err)
			{ 	console.log("err",err)
				return(err)
			}
		else
			{
				obj = JSON.parse(data);
				obj['createdDate']='';
				//following code is for exam
				//console.log('_id',  'userId',  'examId',  'examType',  'certificateId',  'certificationId',  'certificationType',  'basePrice',  'couponCode',  'couponCodeValue',  'isReCertify',  'finalPrice',  'questions',  '__v',  'questionsPct',  'questionsTotal',  'questionsCorrect',  'pointsPct',  'pointsTotal',  'pointsScored',  'resultStatusId',  'passed',  'isPassable',  'passingScore',  'recertifyDate',  'retakeDate',  'expiresDate',  'timeToComplete',  'timeStarted',  'status',  'timeCompleted')
				for(i =0; i<25; i++)//obj.length
					{
				 		//var keys = Object.keys(obj[i]);
						//console.log("keys of record ....",keys);
						exam_obj_id= obj[i]['_id'],
						userid_email= obj[i]['userId'],
						accountid = ""
						if(obj[i].hasOwnProperty('accountId'))
							{ accountid= obj[i]['accountId'] }
						examId= obj[i]['examId'],
						examType= obj[i]['examType'],
						certificateId= obj[i]['certificateId'],
						certificationId= obj[i]['certificationId'],
						certificationType= obj[i]['certificationType'],
						basePrice= obj[i]['basePrice'],
						couponCode= obj[i]['couponCode'],
						couponValue= obj[i]['couponCodeValue'],
						isReCertify= obj[i]['isReCertify'],
						finalPrice= obj[i]['finalPrice'],
						__v= obj[i]['__v'],
						questionsPct= '',
						questionsTotal= '', 
						questionsCorrect= '', 
						pointsPct= '', 
						pointsTotal= '', 
						pointsScored= '', 
						resultStatusId= '', 
						passed= '', 
						isPassable= '', 
						passingScore= '', 
						recertifyDate= '', 
						retakeDate= '', 
						expiresDate= '',
						timeToComplete= '',
						timeStarted= ''
						status= "",
						timeCompleted= "",
						couponId ="",
						certificationName = "",
						createddate =  moment(ObjectId(exam_obj_id).getTimestamp()).format('YYYY-MM-DD')
						if(obj[i].hasOwnProperty('questionsPct'))
						{
							questionsPct= obj[i]['questionsPct'],
							questionsTotal= obj[i]['questionsTotal'],
							questionsCorrect= obj[i]['questionsCorrect'],
							pointsPct= obj[i]['pointsPct'],
							pointsTotal= obj[i]['pointsTotal'],
							pointsScored= obj[i]['pointsScored'],
							resultStatusId= obj[i]['resultStatusId'],
							passed= obj[i]['passed'],
							isPassable= obj[i]['isPassable'],
							passingScore= obj[i]['passingScore'],
							recertifyDate= obj[i]['recertifyDate'].split("T")[0],
							retakeDate= obj[i]['retakeDate'].split("T")[0],
							expiresDate= obj[i]['expiresDate'].split("T")[0],
							timeToComplete= obj[i]['timeToComplete'].split("T")[0],
							timeStarted= obj[i]['timeStarted'].split("T")[0]
							
						}
						if(obj[i].hasOwnProperty('status'))
							{
								status= obj[i]['status']
							}	
						if(obj[i]['timeCompleted'] && obj[i].hasOwnProperty('timeCompleted'))
							{
								timeCompleted= obj[i]['timeCompleted'].split("T")[0]
							}
						if(obj[i].hasOwnProperty('couponId'))
							{
								couponId= obj[i]['couponId']
							}	
						if(obj[i].hasOwnProperty('certificationName'))
							{
								certificationName= obj[i]['certificationName']
							}
                        //console.log(exam_obj_id,userid_email, accountid,examId,	examType,	certificateId,	certificationId,	certificationType,	basePrice,	couponCode,	couponValue,	isReCertify,	finalPrice,	__v,	questionsPct,	questionsTotal,	questionsCorrect,	pointsPct,	pointsTotal,	pointsScored,	resultStatusId,	passed,	isPassable,	passingScore,	recertifyDate,	retakeDate,	expiresDate,	timeToComplete,	timeStarted,	status,	timeCompleted,createddate)
                        // var query = "insert into exams(exams_obj_id,userid_email, accountid,examid,	examtype,	certificateid,	certificationid,	certificationtype,	baseprice,	couponcode,	couponvalue,	isrecertify,	finalprice,	__v,	questionspct,	questionstotal,	questionscorrect,	pointspct,	pointstotal,	pointsscored,	resultstatusid,	passed,	ispassable,	passingscore,	recertifydate,	retakedate,	expiresdate,	timetocomplete,	timestarted,	status,timecompleted,created_date)  values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32);"
						// pgClient.query(query,[exam_obj_id,userid_email, accountid,examId,	examType,	certificateId,	certificationId,	certificationType,	basePrice,	couponCode,	couponValue,	isReCertify,	finalPrice,	__v,	questionsPct,	questionsTotal,	questionsCorrect,	pointsPct,	pointsTotal,	pointsScored,	resultStatusId,	passed,	isPassable,	passingScore,	recertifyDate,	retakeDate,	expiresDate,	timeToComplete,	timeStarted,	status,	timeCompleted,createddate], function(err, result)
						// 		{  if (err){console.error('could not connect to postgres', err);}
                        //                                         else{console.log("..");}
                        // });
                    }
            }
    });
}); 
//, couponid, certificationname