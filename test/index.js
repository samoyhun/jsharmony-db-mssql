/*
Copyright 2017 apHarmony

This file is part of jsHarmony.

jsHarmony is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

jsHarmony is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this package.  If not, see <http://www.gnu.org/licenses/>.
*/

var JSHmssql = require('../index');
var JSHdb = require('jsharmony-db');
var assert = require('assert');
var path = require('path');
var fs = require('fs');
var _ = require('lodash');
var os = require('os');

var dbconfig = { };

var path_TestDBConfig = path.join(os.homedir(),'jsharmony/testdb_mssql.json');
if(fs.existsSync(path_TestDBConfig)){
  dbconfig = JSON.parse(fs.readFileSync(path_TestDBConfig,'utf8'));
  console.log('\r\n==== Loading test database config ====\r\n'+JSON.stringify(dbconfig,null,4)+'\r\n');
}

var tempTable = 'create table #c(c_id bigint); insert into #c(c_id) values (1);insert into #c(c_id) values (2);insert into #c(c_id) values (3);';
var globalTable = "IF OBJECT_ID('tempdb.dbo.##jsh_c', 'U') IS NOT NULL DROP TABLE ##jsh_c; create table ##jsh_c(c_id bigint); insert into ##jsh_c(c_id) values (1);insert into ##jsh_c(c_id) values (2);insert into ##jsh_c(c_id) values (3);";

dbconfig = _.extend({_driver: new JSHmssql(), server: "server.domain.com", database: "DBNAME", user: "DBUSER", password: "DBPASS", options: { pooled: true }, pool: { max: 1 } },dbconfig);
var db = new JSHdb(dbconfig);

describe('Basic',function(){
  it('Select Parameter', function (done) {
    //Connect to database and get data
    var C_ID = '1';
    db.Recordset('','select @C_ID C_ID',[JSHdb.types.BigInt],{'C_ID': C_ID},function(err,rslt){
      assert(!err,'Success');
      assert((rslt && rslt.length && (rslt[0].C_ID==C_ID)),'Parameter returned correctly');
      return done();
    });
  });
  it('Scalar', function (done) {
    //Connect to database and get data
    db.Scalar('',tempTable + 'select count(*) from #c',[],{},function(err,rslt){
      assert(!err,'Success');
      assert(rslt==3,'Scalar correct');
      return done();
    });
  });
  it('Row', function (done) {
    //Connect to database and get data
    var C_ID = '1';
    db.Row('',tempTable+'select * from #c where c_id=@C_ID;',[JSHdb.types.BigInt],{'C_ID': C_ID},function(err,rslt){
      assert(!err,'Success');
      assert(rslt && (rslt.c_id==C_ID),'Recordset correct');
      return done();
    });
  });
  it('Recordset', function (done) {
    //Connect to database and get data
    db.Recordset('',tempTable+'select * from #c;',[],{},function(err,rslt){
      assert(!err,'Success');
      assert(rslt && rslt.length && (rslt.length==3) && (rslt[0].c_id==1),'Recordset correct');
      return done();
    });
  });
  it('MultiRecordset', function (done) {
    //Connect to database and get data
    db.MultiRecordset('',tempTable+'select * from #c;select count(*) cnt from #c;',[],{},function(err,rslt){
      assert(!err,'Success');
      assert(rslt && rslt.length && (rslt.length==2),'Multiple recordsets returned');
      assert(rslt[0] && (rslt[0].length==3) && (rslt[0][0].c_id==1),'Recordset 1 correct');
      assert(rslt[1] && (rslt[1].length==1) && (rslt[1][0].cnt==3),'Recordset 2 correct');
      return done();
    });
  });
  it('Error', function (done) {
    //Connect to database and get data
    db.Command('','select b;',[],{},function(err,rslt){
      assert(err,'Success');
      return done();
    });
  });
  it('Application Error', function (done) {
    //Connect to database and get data
    db.Command('',"raiserror ('Application Error - Test Error',16,1);",[],{},function(err,rslt){
      assert(err,'Success');
      return done();
    });
  });
  it('Application Error', function (done) {
    //Connect to database and get data
    db.Scalar('CONTEXT',"select context_info();",[],{},function(err,rslt){
      assert(rslt && (rslt.toString().substr(0,7)=='CONTEXT'),'Context found');
      return done();
    });
  });
  it('No transaction in progress', function (done) {
    //Connect to database and get data
    db.Scalar('',"select @@TRANCOUNT;",[],{},function(err,rslt){
      assert(!rslt,'No transaction in progress');
      return done();
    });
  });
  it('Create global table', function (done) {
    //Connect to database and get data
    db.Scalar('',globalTable,[],{},function(err,rslt){
      assert(!err,'Success');
      return done();
    });
  });
  
  it('Bad Transaction', function (done) {
    //Connect to database and get data
    db.ExecTransTasks({
      task1: function(dbtrans, callback, transtbl){
        db.Command('','insert into ##jsh_c(c_id) values(4);',[],{},dbtrans,function(err,rslt){ callback(err, rslt); });
      },
      task2: function(dbtrans, callback, transtbl){
        db.Scalar('','select @@TRANCOUNT',[],{},dbtrans,function(err,rslt){ assert(rslt==1,'Transaction in progress'); callback(err, rslt); });
      },
      task3: function(dbtrans, callback, transtbl){
        db.Recordset('','select * from ##jsh_c',[],{},dbtrans,function(err,rslt){ assert(rslt && (rslt.length==4),'Row count correct'); callback(err, rslt); });
      },
      task3: function(dbtrans, callback, transtbl){
        //raiserror ('Application Error - Test Error',16,1);
        db.Recordset('',"raiserror ('Application Error - Test Error',16,1);",[],{},dbtrans,function(err,rslt){ callback(err, rslt); });
      },
    },function(err,rslt){
      assert(err,'Rollback generated an error');
      return done();
    });
  });
  it('Transaction Rolled back', function (done) {
    //Connect to database and get data
    db.Scalar('','select count(*) from ##jsh_c',[],{},function(err,rslt){
      assert(!err,'Success');
      assert(rslt==3,'Row count correct');
      return done();
    });
  });

  it('Good Transaction', function (done) {
    //Connect to database and get data
    db.ExecTransTasks({
      task1: function(dbtrans, callback, transtbl){
        db.Command('','insert into ##jsh_c(c_id) values(4);',[],{},dbtrans,function(err,rslt){ callback(err, rslt); });
      },
      task2: function(dbtrans, callback, transtbl){
        db.Scalar('','select @@TRANCOUNT',[],{},dbtrans,function(err,rslt){ assert(rslt==1,'Transaction in progress'); callback(err, rslt); });
      },
    },function(err,rslt){
      assert(!err,'Success');
      return done();
    });
  });
  it('Transaction Committed', function (done) {
    //Connect to database and get data
    db.Scalar('','select count(*) from ##jsh_c',[],{},function(err,rslt){
      assert(!err,'Success');
      assert(rslt==4,'Row count correct');
      return done();
    });
  });

  it('Drop global table', function (done) {
    //Connect to database and get data
    db.Scalar('',"drop table ##jsh_c",[],{},function(err,rslt){
      assert(!err,'Success');
      return done();
    });
  });
  after(function(done){
    assert(db.dbconfig._driver.pool.length==1,'Pool exists');
    assert(db.dbconfig._driver.pool[0].isConnected,'Pool connected');
    db.Close(function(){
      assert(!db.dbconfig._driver.pool[0].isConnected,'Pool closed');
      return done();
    });
  });
});