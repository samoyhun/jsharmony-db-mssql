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
        db_log_level: 6,           //Bitmask: 2 = WARNING, 4 = NOTICES :: Database messages logged to the console / log 
        db_error_sql_state: false  //Log SQL state during DB error
      }
    }
  }
  this.platform.Log.info = function(msg){ console.log(msg); }
  this.platform.Log.warning = function(msg){ console.log(msg); }
  this.platform.Log.error = function(msg){ console.log(msg); }
}

DBdriver.prototype.getDefaultSchema = function(){
  return 'dbo';
}

DBdriver.prototype.logRawSQL = function(sql){
  if (this.platform.Config.debug_params && this.platform.Config.debug_params.db_raw_sql && this.platform.Log) {
    this.platform.Log.info(sql, { source: 'database_raw_sql' });
  }
}

function initDBConfig(dbconfig){
  if(!dbconfig) return;
  if(!dbconfig.options) dbconfig.options = {};
  if(!dbconfig.options.useUTC) dbconfig.options.useUTC = false;
  if(!dbconfig.options.pooled) dbconfig.options.pooled = false;
  if(!('encrypt' in dbconfig.options)) dbconfig.options.encrypt = true;
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
    mspool.con = new mssql.ConnectionPool(dbconfig, function (err) {
      if (err && !_this.silent){
        _this.platform.Log('MSSQL Pool Error: ' + err.toString(), { source: 'database' });
        if(onFail) return onFail(err);
      }
      else {
        mspool.isConnected = true;
        return onInitialized(mspool.con);
      }
    });
  }
  else if (!mspool.isConnected) {
    var maxRetries = 100;
    if(dbconfig && ('maxConnectRetries' in dbconfig)) maxRetries = dbconfig.maxConnectRetries;
    if (numRetries >= maxRetries) { 
      if(!_this.silent) _this.platform.Log('Timeout waiting for MSSQL Pool', { source: 'database' });
      if(onFail) onFail(new Error('Timeout waiting for MSSQL Pool'));
      _this.closePool(dbconfig);
      return; 
    }
    if(!_this.silent) _this.platform.Log('Retry: '+numRetries, { source: 'database' });
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
  var _this = this;
  if (!dbtype) throw new Error('Cannot get dbtype of null object');
  if (val === null) return 'NULL';
  if (typeof val === 'undefined') return 'NULL';
  
  if ((dbtype.name == 'VarChar') || (dbtype.name == 'Char')) {
    var valstr = val.toString();
    if (dbtype.length == types.MAX) return "N'" + _this.escape(valstr) + "'";
    return "N'" + _this.escape(valstr.substring(0, dbtype.length)) + "'";
  }
  else if (dbtype.name == 'VarBinary') {
    var valbin = null;
    if (val instanceof Buffer) valbin = val;
    else valbin = new Buffer(val.toString());
    if (valbin.legth == 0) return "NULL";
    return "0x" + valbin.toString('hex').toUpperCase();
  }
  else if ((dbtype.name == 'BigInt') || (dbtype.name == 'Int') || (dbtype.name == 'SmallInt') || (dbtype.name == 'TinyInt')) {
    var valint = parseInt(val);
    if (isNaN(valint)) { return "NULL"; }
    return valint.toString();
  }
  else if (dbtype.name == 'Boolean') {
    if((val==='')||(typeof val == 'undefined')) return "NULL";
    var valbool = val.toString().toUpperCase();
    return "'" + _this.escape(val.toString()) + "'";
  }
  else if (dbtype.name == 'Decimal') {
    var valfloat = parseFloat(val);
    if (isNaN(valfloat)) { return "NULL"; }
    return _this.escape(val.toString());
  }
  else if (dbtype.name == 'Float') {
    var valfloat = parseFloat(val);
    if (isNaN(valfloat)) { return "NULL"; }
    return _this.escape(val.toString());
  }
  else if ((dbtype.name == 'Date') || (dbtype.name == 'Time') || (dbtype.name == 'DateTime')) {
    var suffix = '';

    var valdt = null;
    if (val instanceof Date) { valdt = val; }
    else if(_.isNumber(val) && !isNaN(val)){
      valdt = moment(moment.utc(val).format('YYYY-MM-DDTHH:mm:ss.SSS'), "YYYY-MM-DDTHH:mm:ss.SSS").toDate();
    }
    else {
      if (isNaN(Date.parse(val))) return "NULL";
      valdt = new Date(val);
    }

    var mdate = moment(valdt);
    if (!mdate.isValid()) return "NULL";

    if(!_.isNumber(val)){
      if('jsh_utcOffset' in val){
        //Time is in UTC, Offset specifies amount and timezone
        var neg = false;
        if(val.jsh_utcOffset < 0){ neg = true; }
        suffix = moment.utc(new Date(val.jsh_utcOffset*(neg?-1:1)*60*1000)).format('HH:mm');
        //Reverse offset
        suffix = ' '+(neg?'+':'-')+suffix;

        mdate = moment.utc(valdt);
        mdate = mdate.add(val.jsh_utcOffset*-1, 'minutes');
      }

      if('jsh_microseconds' in val){
        var ms_str = "000"+(Math.round(val.jsh_microseconds)).toString();
        ms_str = ms_str.slice(-3);
        suffix = ms_str.replace(/0+$/,'') + suffix;
      }
    }

    var rslt = '';
    if (dbtype.name == 'Date') rslt = "'" + mdate.format('YYYY-MM-DD') + "'";
    else if (dbtype.name == 'Time') rslt = "'" + mdate.format('HH:mm:ss.SSS') + suffix + "'";
    else rslt = "'" + mdate.format('YYYY-MM-DD HH:mm:ss.SSS') + suffix + "'";
    return rslt;
  }
  throw new Error('Invalid datetype: ' + JSON.stringify(dbtype));
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
      var con = new mssql.ConnectionPool(dbconfig, function (err) {
        if (err) { return _this.ExecError(err, session, "DB Connect Error: "); }
        session(null, con, dbconfig._presql || '', function () { con.close(); });
      });
    }
  }
}

DBdriver.prototype.ExecError = function(err, callback, errprefix) {
  if (this.platform.Config.debug_params.db_error_sql_state && !this.silent) this.platform.Log((errprefix || '') + err.toString(), { source: 'database' });
  if (callback) return callback(err, null);
  else throw err;
}

DBdriver.prototype.Exec = function (dbtrans, context, return_type, sql, ptypes, params, callback, dbconfig) {
  if(!dbconfig) throw new Error('dbconfig is required');
  var _this = this;
  
  _this.ExecSession(dbtrans, dbconfig, function (err, con, presql, conComplete) {
    if(dbtrans && (dbtrans.dbconfig != dbconfig)) err = new Error('Transaction cannot span multiple database connections');
    if(err) {
      if (callback != null) callback(err, null, null);
      else throw err;
      return;
    }
	
    var dbrslt = null;
    var stats = { notices: [], warnings: [] };
    var no_errors = true;

    //Streaming
    var r = new mssql.Request(con);
    r.stream = true;

    var execsql = presql + sql;

    //Apply ptypes, params to SQL
    var ptypes_ref = {};
    var i = 0;
    for (var p in params) {
      ptypes_ref[p] = ptypes[i];
      i++;
    }
    //Sort params by length
    var param_keys = _.keys(params);
    param_keys.sort(function (a, b) { return b.length - a.length; });
    //Replace params in SQL statement
    for (var i = 0; i < param_keys.length; i++) {
      var p = param_keys[i];
      var val = params[p];
      if (val === '') val = null;
      execsql = DB.util.ReplaceAll(execsql, '@' + p, _this.getDBParam(ptypes_ref[p], val));
    }

    //Add context SQL
    execsql = _this.getContextSQL(context) + execsql;

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
    r.on('info', function (msg) {
      if(msg.number) stats.warnings.push(new DB.Message(DB.Message.WARNING, msg.message));
      else stats.notices.push(new DB.Message(DB.Message.NOTICE, msg.message));
    });
    r.on('error', function (err) {
      if ((dbrslt != null) && (dbrslt instanceof Error)) {
        // Make Application Errors a priority, otherwise concatenate errors
        if (dbrslt.message.indexOf('Application Error - ') == 0) { }
        else if (dbrslt.message.indexOf('Execute Form - ') == 0) { }
        else if (err.message.indexOf('Application Error - ') == 0) { dbrslt = err; }
        else if (dbrslt.code == err.code) dbrslt.message += ' - ' + err.message;
      }
      else dbrslt = err;
      if (_this.platform.Config.debug_params.db_error_sql_state && no_errors && !_this.silent){
        no_errors = false;
        _this.platform.Log('SQL Error: ' + (err.message||'') + ' - ' + sql + ' ' + JSON.stringify(ptypes) + ' ' + JSON.stringify(params), { source: 'database' });
      }
    });
    r.on('done', function (rslt) {
      setTimeout(function(){
        conComplete();
        if (dbrslt instanceof Error) {
          if (callback != null) callback(dbrslt, null, null);
          else throw dbrslt;
          return;
        }
        DB.util.LogDBResult(_this.platform, { sql: execsql, dbrslt: dbrslt, notices: stats.notices, warnings: stats.warnings });
        if (callback != null) callback(null, dbrslt, stats);
      },1);
    });

    _this.logRawSQL(execsql);
    r.query(execsql);
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
    trans.con.begin(function (err) {
      execTasks(trans, function (dberr, rslt) {
        if (dberr != null) { trans.con.rollback(function (err) { conComplete(); callback(dberr, null); }); }
        else { trans.con.commit(function (err) { conComplete(); callback(err, rslt); }); }
      });
    });
  });
};

DBdriver.prototype.escape = function (val) { return this.sql.escape(val); }

DBdriver.prototype.getContextSQL = function(context) {
  if(!context) return '';
  return 'set context_info 0x' + DB.util.str2hex(context) + ';';
}

exports = module.exports = DBdriver;
