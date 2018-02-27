var pg = require('pg')
pg.defaults.ssl = true;
require('dotenv').config({path: __dirname + './../.env'});
const pg_connectionString = "postgres://"+process.env.PG_USER+":"+process.env.PG_PWD+"@"+process.env.PG_HOST+":"+process.env.PG_PORT+"/"+process.env.PG_DB
global.pgClient = new pg.Client(pg_connectionString);
pgClient.connect(function(err) {
  if(err) {
    return console.error('could not connect to postgres', err);
  }
  
  

});