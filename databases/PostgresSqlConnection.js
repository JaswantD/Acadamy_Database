require('dotenv').config({path: __dirname + './../.env'});
const { Pool } = require('pg');
pool = new Pool({
    connectionString:process.env.DATABASE_URL,
    ssl: true,
})
module.exports = {
    makeConnection: (connected) => {
        pool.connect(function(err,res){
            if(err){console.log(err)}
            connected();
            })
        },

    create: (tablename, args, createdTable) => {
        var qry = "CREATE TABLE IF NOT EXISTS temp_"+tablename+"("+args+")";
        pool.query(qry,function(err, res) {
            if(err) {   return console.error('error running query', err);}
            createdTable();
            });
        },

    insert:(query, values,dataInserted) => {
        pool.query("insert into "+query+""+values+"",function(err, res) {
            if(err) {   return console.error('error running query', err); }
            dataInserted();
            })
        }, 

    update:(query,updatedUserIds) => {
        pool.query("Update "+query,function(err, res) {
            if(err) {   return console.error('error running query', err);}
            updatedUserIds();
            });
        }, 
    
    drop:(tablename,dropped) => {
        pool.query("Drop table if exists temp_"+tablename,function(err, res) {
            if(err) {   return console.error('error running query', err);}
            dropped();
            });
        }, 
    Truncate:(tablename,truncated) => {
        pool.query("TRUNCATE table "+tablename,function(err, res) {
            if(err) {   return console.error('error running query', err);}
            truncated();
            });
        }, 
    closeConn:(closed) => {
        pool.end();
        closed()
        }, 
    };
    
