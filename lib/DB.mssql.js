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
  this.con = null;
  this.IsConnected = false;
}

function initConstring(constring){
  if(!constring) return;
  if(!constring.options) constring.options = {};
  if(!constring.options.useUTC) constring.options.useUTC = false;
}

DBdriver.prototype.Init = function (onInitialized, numRetries) {
  if (!numRetries) numRetries = 0;
  var _this = this;
  initConstring(global.dbconfig);
  if (!this.con) {
    _this.con = new mssql.Connection(global.dbconfig, function (err) {
      if (err) DB.log('MSSQL Pool Error: ' + err.toString());
      else {
        _this.IsConnected = true;
        return onInitialized();
      }
    });
  }
  else if (!this.IsConnected) {
    if (numRetries > 100) { DB.log('Timeout waiting for MSSQL Pool'); return; }
    window.setTimeout(function () { _this.Init(onInitialized, numRetries + 1); }, 100);
  }
  else return onInitialized();
}

DBdriver.prototype.getDBParam = function (dbtype, val) {
  if (!dbtype) throw new Exception('Cannot get dbtype of null object');
  if (val === null) return val;
  if (typeof val === 'undefined') return val;
  if (dbtype.name == 'Bit') {
    if (!val) return false;
    if (val == '0') return false;
    if (val.toString().toLowerCase() == 'false') return false;
  }
  else if (dbtype.name == 'Date') {
    if (_.isNumber(val) && !isNaN(val)) {
      return moment(moment.utc(val).format('YYYY-MM-DDTHH:mm:ss.SSS'), "YYYY-MM-DDTHH:mm:ss.SSS").toDate();
    }
  }
  return val;
}

DBdriver.prototype.getDBType = function (dbtype) {
  if (!dbtype) throw new Error('Cannot get dbtype of null object');
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
  else if (dbtype.name == 'Decimal') { return mssql.Decimal(dbtype.prec_h, dbtype.prec_l); }
  else if (dbtype.name == 'Date') { return mssql.Date(); }
  else if (dbtype.name == 'Time') { return mssql.Time(dbtype.prec); }
  else if (dbtype.name == 'DateTime') { return mssql.DateTime(dbtype.prec); }
  else if (dbtype.name == 'Bit') { return mssql.Bit(); }
  throw new Error('Invalid datetype: ' + JSON.stringify(dbtype));
}

DBdriver.prototype.ExecSession = function (dbtrans, constring, session) {
  var _this = this;
  
  if (dbtrans) {
    session(null, dbtrans, '', function () { });
  }
  else {
    if (constring && (constring != global.dbconfig)) {
      initConstring(constring);
      var con = new mssql.Connection(constring, function (err) {
        if (err) { return ExecError(err, session, "DB Connect Error: "); }
        session(null, con, constring._presql || '', function () { con.close(); });
      });
    }
    else {
      initConstring(global.dbconfig);
      _this.Init(function () { session(null, _this.con, global.dbconfig._presql || '', function () { }); });
    }
  }
}

function ExecError(err, callback, errprefix) {
  if (global.debug_params && global.debug_params.db_error_sql_state) DB.log((errprefix || '') + err.toString());
  if (callback) return callback(err, null);
  else throw err;
}

DBdriver.prototype.Exec = function (dbtrans, context, return_type, sql, ptypes, params, callback, constring) {
  var _this = this;
  
  _this.ExecSession(dbtrans, constring, callback, function (err, con, presql, conComplete) {
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
		  if(typeof err != 'undefined') DB.log(err);
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
    for (var p in params) {
      var val = params[p];
      if (val === '') val = null;
      var ptype = DBdriver.prototype.getDBType(ptypes[i]);
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
      if (global.debug_params && global.debug_params.db_error_sql_state) DB.log('SQL Error: ' + sql + ' ' + JSON.stringify(ptypes) + ' ' + JSON.stringify(params));
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
    return mdt.format("YYYY-MM-DDTHH:mm:ss.SSS");
  }
  return val;
}

DBdriver.prototype.ExecTransTasks = function (dbtasks, callback, constring) {
  var _this = this;
  _this.ExecSession(null, constring, function (err, con, presql, conComplete) {
    if(err) return callback(err, null);
    var trans = new mssql.Transaction(con);
    var transtbl = {};
    dbtasks = _.reduce(dbtasks, function (rslt, dbtask, key) {
      rslt[key] = function (callback) {
        var xcallback = function (err, rslt) {
          if (rslt != null) {
            if (!_.isArray(rslt) || rslt.length < 2)
              transtbl = _.extend(transtbl, rslt);
          }
          callback(err, rslt);
        };
        return dbtask.call(null, trans, xcallback, transtbl);
      };
      return rslt;
    }, {});
    trans.begin(function (err) {
      async.series(dbtasks, function (dberr, rslt) {
        if (dberr != null) { trans.rollback(function (err) { conComplete(); callback(dberr, null); }); }
        else { trans.commit(function (err) { conComplete(); callback(err, rslt); }); }
      });
    });
  });
};

DBdriver.prototype.escape = function (val) {
  if (val === 0) return val;
  if (val === 0.0) return val;
  if (val === "0") return val;
  if (!val) return '';
  
  if (!isNaN(val)) return val;
  
  val = val.replace(/[\0\x01\x02\x03\x04\x05\x06\x07\x08\x0b\x0c\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f]/g, '');
  val = val.replace(/[']/g, "''");
  return val;
}

function getContextSQL(context) {
  return 'set context_info 0x' + DB.util.str2hex(context) + ';';
}

exports = module.exports = DBdriver;
