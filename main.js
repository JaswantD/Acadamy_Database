var async = require('async')
var users = require('./controller/tables/users.js');
var earnBadges = require('./controller/tables/earned_badges.js');
var earnCertificates = require('./controller/tables/earned_certificates.js');
var clookup = require('./controller/tables/complete_lookup.js');
var slookup = require('./controller/tables/started_lookup.js');
var exams = require('./controller/tables/exams.js');
var quizzes = require('./controller/tables/quizzes.js');
var pg = require('./databases/PostgresSqlConnection.js');
var CronJob = require('cron').CronJob;
// var job = new CronJob({
//     cronTime: '00 22 06 * * *',
//     onTick: function() {
//         runCronJob();
//         },
//     start: false,
//     timeZone: 'America/Los_Angeles'
//     });
// job.start();
// function runCronJob(){
    console.log("in cron job");
    async.auto({
        makeConnection:function(connected){
            pg.makeConnection(function(err){
                if(err){console.log(err)
                        throws(err);}
                else{connected();}
                })
            },
        getUsersData:['makeConnection',function(aboveresult,callback){
            console.log("Running Script for Users Data..");
            users.usersData(function(err,res){
                callback();
                })
            }],
        getEBadgesData:['getUsersData',function(aboveresult, callback1){
            console.log("Running Script for EarnedBadges Data..");
            earnBadges.eBadgesData(function(err,res){
                callback1()
                })
            }],
        getECertificatesData:['getEBadgesData',function(aboveresult, callback2){
            console.log("Running Script for EarnedCertificates Data..");
            earnCertificates.eCertificatesData(function(err,res){
                callback2()
                })
            }],
        getCLookupData:['getECertificatesData',function(aboveresult, callback3){
            console.log("Running Script for CompleteLookup Data..");
            clookup.cLookupData(function(err,res){
                callback3()
                })
            }],
        getSLookupData:['getCLookupData',function(aboveresult, callback4){
            console.log("Running Script for StartedLookup Data..");
            slookup.sLookupData(function(err,res){
                callback4()
                })
            }],
        getExamsData:['getSLookupData',function(aboveresult, callback5){
            console.log("Running Script for Exams Data..");
            exams.examsData(function(err,res){
                callback5()
                })
            }],
    
        getQuizzesData:['getExamsData',function(aboveresult, callback6){
            console.log("Running Script for Quizzes Data..");
            quizzes.quizzesData(function(err,res){
                callback6()
                })
            }],
            closeConnection:['getQuizzesData',function(aboveresult, disconnected){
                pg.closeConn(function(err,connected){
                    if(err){console.log(err)
                            throws(err);}
                    else{disconnected();}
                    
                })
            }],


        },
        function(err, result){
            console.log("All Tables Updated.")
        });

    // }