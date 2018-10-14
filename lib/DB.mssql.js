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

var DB = require('jsharmony-db');
var types = DB.types;
var mssql = require('mssql');
var _ = require('lodash');
var async = require('async');
var moment = require('moment');

function DBdriver() {
  this.name = 'mssql';
  this.sql = require('./DB.mssql.sql.js');
  this.meta = require('./DB.mssql.meta.js');
  this.pool = []; /* { dbconfig: xxx, con: yyy, isConnected: false } */
  this.silent = false;

  //Initialize platform
  this.platform = {
    Log: function(msg){ console.log(msg); },
    Config: {
      debug_params: {
        db_error_sql_state: false  //Log SQL state during DB error
      }
    }
  }
  this.platform.Log.info = function(msg){ console.log(msg); }
  this.platform.Log.warning = function(msg){ console.log(msg); }
  this.platform.Log.error = function(msg){ console.log(msg); }
}

function initDBConfig(dbconfig){
  if(!dbconfig) return;
  if(!dbconfig.options) dbconfig.options = {};
  if(!dbconfig.options.useUTC) dbconfig.options.useUTC = false;
  if(!dbconfig.options.pooled) dbconfig.options.pooled = false;
}

DBdriver.prototype.getPooledConnection = function (dbconfig, onInitialized, numRetries, onFail) {
  if(!dbconfig) throw new Error('dbconfig is required');
  if (!numRetries) numRetries = 0;
  var _this = this;

  var mspool = null;
  //Check if pool was already added
  for(var i=0;i<this.pool.length;i++){
    if(this.pool[i].dbconfig==dbconfig) mspool = this.pool[i];
  }
  //Add pool if it does not exist
  if(!mspool){
    _this.pool.push({
      dbconfig: dbconfig,
      con: null,
      isConnected: false
    });
    mspool = _this.pool[_this.pool.length - 1];
  }
  //Initialize pool connection if it was not initialized
  if(!mspool.con){
    mspool.con = new mssql.Connection(dbconfig, function (err) {
      if (err && !_this.silent){
        _this.platform.Log('MSSQL Pool Error: ' + err.toString());
        if(onFail) return onFail(err);
      }
      else {
        mspool.isConnected = true;
        return onInitialized(mspool.con);
      }
    });
  }
  else if (!mspool.isConnected) {
    if (numRetries > 100) { 
      if(!_this.silent) _this.platform.Log('Timeout waiting for MSSQL Pool');
      if(onFail) onFail(new Error('Timeout waiting for MSSQL Pool'));
      _this.closePool(dbconfig);
      return; 
    }
    if(!_this.silent) _this.platform.Log('Retry: '+numRetries);
    setTimeout(function () { _this.getPooledConnection(dbconfig, onInitialized, numRetries + 1, onFail); }, 100);
  }
  else return onInitialized(mspool.con);
}

DBdriver.prototype.closePool = function(dbconfig, onClosed){
  if(!dbconfig) throw new Error('dbconfig is required');
  if(!onClosed) onClosed = function(){};
  var _this = this;

  var mspool = null;
  //Check if dbconfig exists in pool
  for(var i=0;i<this.pool.length;i++){
    if(this.pool[i].dbconfig==dbconfig) mspool = this.pool[i];
  }
  if(!mspool) return onClosed();

  if(mspool.con && mspool.isConnected){
    mspool.isConnected = false;
    mspool.con.close(function(){
      mspool.con = null;
      onClosed();
    });
  }
  else{
    mspool.con = null;
    if(onClosed) onClosed();
  }
}

DBdriver.prototype.Init = function (cb) { if(cb) return cb(); }

DBdriver.prototype.Close = function(onClosed){
  var _this = this;
  async.each(_this.pool, function(mspool, pool_cb){
    _this.closePool(mspool.dbconfig, pool_cb);
  }, onClosed);
}

DBdriver.prototype.getDBParam = function (dbtype, val) {
  if (!dbtype) throw new Exception('Cannot get dbtype of null object');
  if (val === null) return val;
  if (typeof val === 'undefined') return val;
  if (dbtype.name == 'Boolean') {
    if (!val) return false;
    if (val == '0') return false;
    if (val.toString().toLowerCase() == 'false') return false;
  }
  else if (dbtype.name == 'Date') {
    if (_.isNumber(val) && !isNaN(val)) {
      return moment(moment.utc(val).format('YYYY-MM-DDTHH:mm:ss.SSS'), "YYYY-MM-DDTHH:mm:ss.SSS").toDate();
    }
  }
  else if (dbtype.name == 'DateTime') {
    if(_.isDate(val)){
      if('jsh_utcOffset' in val){ val.getTimezoneOffset = function(){ return val.jsh_utcOffset; }; }
      if('jsh_microseconds' in val){ val.nanosecondDelta = val.jsh_microseconds / 1000000; }
    }
  }
  else if (dbtype.name == 'Time') {
    if(_.isDate(val)){
      if('jsh_utcOffset' in val){ val.getTimezoneOffset = function(){ return val.jsh_utcOffset; }; }
      if('jsh_microseconds' in val){ val.nanosecondDelta = val.jsh_microseconds / 1000000; }
    }
  }
  return val;
}

DBdriver.prototype.getDBType = function (dbtype,desc) {
  if (!desc) desc='';
  if (!dbtype) throw new Error('Parameter '+desc+' database type invalid or not set');
  if (dbtype.name == 'VarChar') {
    if (dbtype.length == types.MAX) return mssql.NVarChar(mssql.MAX);
    return mssql.NVarChar(dbtype.length);
  }
  else if (dbtype.name == 'Char') {
    if (dbtype.length == types.MAX) return mssql.Char(mssql.MAX);
    return mssql.Char(dbtype.length);
  }
  else if (dbtype.name == 'VarBinary') {
    if (dbtype.length == types.MAX) return mssql.VarBinary(mssql.MAX);
    return mssql.VarBinary(dbtype.length);
  }
  else if (dbtype.name == 'BigInt') { return mssql.BigInt(); }
  else if (dbtype.name == 'Int') { return mssql.Int(); }
  else if (dbtype.name == 'SmallInt') { return mssql.SmallInt(); }
  else if (dbtype.name == 'TinyInt') { return mssql.TinyInt(); }
  else if (dbtype.name == 'Decimal') { 
    //return mssql.Decimal(dbtype.prec_h, dbtype.prec_l); 
    return mssql.VarChar(dbtype.length||50); 
  }
  else if (dbtype.name == 'Float') { 
    return mssql.VarChar(dbtype.length||128); 
  }
  else if (dbtype.name == 'Date') { return mssql.Date(); }
  else if (dbtype.name == 'Time') { var prec = dbtype.prec; if(typeof prec=='undefined') prec = 7;  return mssql.Time(prec); }
  else if (dbtype.name == 'DateTime') { 
    var prec = dbtype.prec; 
    if(typeof prec=='undefined') prec = 7; 
    if(dbtype.preserve_timezone) return mssql.DateTimeOffset(prec);
    else return mssql.DateTime2(prec); 
  }
  else if (dbtype.name == 'Boolean') { return mssql.Bit(); }
  throw new Error('Invalid datetype: ' + JSON.stringify(dbtype));
}

DBdriver.prototype.ExecSession = function (dbtrans, dbconfig, session) {
  if(!dbconfig) throw new Error('dbconfig is required');
  var _this = this;
  
  if (dbtrans) {
    session(null, dbtrans.con, '', function () { });
  }
  else {
    initDBConfig(dbconfig);
    if(dbconfig.options.pooled){
      _this.getPooledConnection(
        dbconfig,
        function (con) { 
          session(null, con, dbconfig._presql || '', function () { }); 
        }, 
        0,
        function(err){ return _this.ExecError(err, session, "DB Connect Error: "); }
      );
    }
    else {
      initDBConfig(dbconfig);
      var con = new mssql.Connection(dbconfig, function (err) {
        if (err) { return _this.ExecError(err, session, "DB Connect Error: "); }
        session(null, con, dbconfig._presql || '', function () { con.close(); });
      });
    }
  }
}

DBdriver.prototype.ExecError = function(err, callback, errprefix) {
  if (this.platform.Config.debug_params.db_error_sql_state && !this.silent) this.platform.Log((errprefix || '') + err.toString());
  if (callback) return callback(err, null);
  else throw err;
}

DBdriver.prototype.Exec = function (dbtrans, context, return_type, sql, ptypes, params, callback, dbconfig) {
  if(!dbconfig) throw new Error('dbconfig is required');
  var _this = this;
  
  _this.ExecSession(dbtrans, dbconfig, function (err, con, presql, conComplete) {
    if(dbtrans && (dbtrans.dbconfig != dbconfig)) err = new Error('Transaction cannot span multiple database connections');
    if(err) {
      if (callback != null) callback(err, null);
      else throw err;
      return;
    }
    //Non-streaming
    /*
	  var r = new mssql.Request();
	  r.input('CODEVAL',mssql.NVarChar(8),'OTHER');
	  r.query('select * from GCOD_C_BCTGR where CODEVAL=@CODEVAL',function(err,rs){
		  if(typeof err != 'undefined') _this.platform.Log(err);
		  else console.dir(rs);
	  });
	  */
	  //r.execute
	  //r.output (output parameter)
	  //use prepare for multiple executions on one connection
	  //https://github.com/patriksimek/node-mssql#batch
	
	  var dbrslt = null;
    //Streaming
    var r = new mssql.Request(con);
    r.stream = true;
    var i = 0;
    var no_errors = true;
    for (var p in params) {
      var val = params[p];
      if (val === '') val = null;
      var ptype = DBdriver.prototype.getDBType(ptypes[i],p);
      val = DBdriver.prototype.getDBParam(ptypes[i], val);
      r.input(p, ptype, val);
      i++;
    }
    r.query(getContextSQL(context) + presql + sql);
    r.on('recordset', function (cols) {
      if (dbrslt instanceof Error) return;
      if (return_type == 'multirecordset') {
        if (dbrslt == null) dbrslt = [];
        dbrslt.push([]);
      }
      else if (return_type == 'recordset') {
        dbrslt = [];
      }
    });
    r.on('row', function (row) {
      if (dbrslt instanceof Error) return;
      if (row) {
        for (var key in row) if (row.hasOwnProperty(key)) row[key] = parseResultData(row[key]);
      }
      if (return_type == 'row') dbrslt = row;
      else if (return_type == 'recordset') dbrslt.push(row);
      else if (return_type == 'multirecordset') dbrslt[dbrslt.length - 1].push(row);
      else if (return_type == 'scalar') {
        if (DB.util.Size(row) == 0) dbrslt = null;
        for (var key in row) if (row.hasOwnProperty(key)) dbrslt = row[key];
      }
    });
    r.on('error', function (err) {
      if ((dbrslt != null) && (dbrslt instanceof Error)) {
        // Make Application Errors a priority, otherwise concatenate errors
        if (dbrslt.message.indexOf('Application Error - ') == 0) { }
        else if (err.message.indexOf('Application Error - ') == 0) { dbrslt = err; }
        else if (dbrslt.code == err.code) dbrslt.message += ' - ' + err.message;
      }
      else dbrslt = err;
      if (_this.platform.Config.debug_params.db_error_sql_state && no_errors && !_this.silent){
        no_errors = false;
        _this.platform.Log('SQL Error: ' + sql + ' ' + JSON.stringify(ptypes) + ' ' + JSON.stringify(params));
      }
    });
    r.on('done', function (rslt) {
      conComplete();
      if (dbrslt instanceof Error) {
        if (callback != null) callback(dbrslt, null);
        else throw dbrslt;
        return;
      }
      if (callback != null) callback(null, dbrslt);
    });
  });
};

function parseResultData(val) {
  if (val instanceof Date) {
    var mdt = moment(val);
    if (!mdt.isValid()) return val;
    var rslt = mdt.format("YYYY-MM-DDTHH:mm:ss.SSS");
    if(val.nanosecondsDelta){ 
      var ns_str = "0000"+(val.nanosecondsDelta*10000000).toString();
      ns_str = ns_str.slice(-4);
      rslt += ns_str.replace(/0+$/,'');
    }
    return rslt;
  }
  return val;
}

DBdriver.prototype.ExecTransTasks = function (execTasks, callback, dbconfig) {
  if(!dbconfig) throw new Error('dbconfig is required');
  var _this = this;
  
  _this.ExecSession(null, dbconfig, function (err, con, presql, conComplete) {
    if(err) return callback(err, null);
    var contrans = new mssql.Transaction(con);
    var trans = new DB.TransactionConnection(contrans,dbconfig);
    trans.begin(function (err) {
      execTasks(trans, function (dberr, rslt) {
        if (dberr != null) { trans.rollback(function (err) { conComplete(); callback(dberr, null); }); }
        else { trans.commit(function (err) { conComplete(); callback(err, rslt); }); }
      });
    });
  });
};

DBdriver.prototype.escape = function (val) { return this.sql.escape(val); }

function getContextSQL(context) {
  if(!context) return '';
  return 'set context_info 0x' + DB.util.str2hex(context) + ';';
}

exports = module.exports = DBdriver;
