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
var dbtypes = DB.types;
var _ = require('lodash');
var async = require('async');
var path = require('path');
var triggerFuncs = require('./DB.mssql.triggerfuncs.js');

function DBObjectSQL(db, sql){
  this.db = db;
  this.sql = sql;
}

function getDBType(column){
  var length = '(max)';
  if(('length' in column) && (column.length >=0)) length = '('+column.length.toString()+')';
  var prec = '';
  if('precision' in column){
    prec = '(';
    if(_.isArray(column.precision)){
      for(var i=0;i<column.precision.length;i++){
        if(i>0) prec += ',';
        prec += column.precision[i].toString();
      }
    }
    else prec += (column.precision||'').toString();
    prec += ')';
  }

  if(column.type=='varchar') return 'nvarchar'+length;
  else if(column.type=='char') return 'nchar'+length;
  else if(column.type=='binary') return 'varbinary'+length;
  else if(column.type=='bigint') return 'bigint';
  else if(column.type=='int') return 'int';
  else if(column.type=='smallint') return 'smallint';
  else if(column.type=='tinyint') return 'tinyint';
  else if(column.type=='boolean') return 'bit';
  else if(column.type=='date') return 'date'+prec;
  else if(column.type=='time') return 'time'+prec;
  else if(column.type=='datetime') return 'datetime'+prec;
  else if(column.type=='decimal') return 'decimal'+prec;
  else if(column.type=='float') return 'float'+prec;
  else if(column.type) throw new Error('Column '+column.name+' datatype not supported: '+column.type);
  else throw new Error('Column '+column.name+' missing type');
}

DBObjectSQL.prototype.getjsHarmonyFactorySchema = function(jsh){
  if(jsh&&jsh.Modules&&jsh.Modules['jsHarmonyFactory']){
    return jsh.Modules['jsHarmonyFactory'].schema||'';
  }
  return '';
}

DBObjectSQL.prototype.parseSchema = function(name){
  name = name || '';
  var rslt = {
    schema: '',
    name: name
  }
  var idx = name.indexOf('.');
  if(idx>=0){
    rslt.schema = name.substr(0,idx);
    rslt.name = name.substr(idx+1);
  }
  return rslt;
}

DBObjectSQL.prototype.init = function(jsh, module, obj){
  var sql = '';
  var caption = ['','',''];
  if(obj.caption){
    if(_.isArray(obj.caption)){
      if(obj.caption.length == 1) caption = ['', obj.caption[0].toString(), obj.caption[0].toString()];
      else if(obj.caption.length == 2) caption = ['', obj.caption[0].toString(), obj.caption[1].toString()];
      else if(obj.caption.length >= 3) caption = ['', obj.caption[1].toString(), obj.caption[2].toString()];
    }
    else caption = ['', obj.caption.toString(), obj.caption.toString()];
  }
  if(obj.type=='table'){
    sql += 'create table '+obj.name+'(\n';
    var sqlcols = [];
    if(obj.columns) for(var i=0; i<obj.columns.length;i++){
      var column = obj.columns[i];
      var sqlcol = '  '+column.name;
      sqlcol += ' '+getDBType(column);
      if(column.identity) sqlcol += ' identity';
      if(column.key) sqlcol += ' primary key';
      if(column.unique) sqlcol += ' unique';
      if(!column.null) sqlcol += ' not null';
      if(column.foreignkey){
        var foundkey = false;
        for(var tbl in column.foreignkey){
          if(foundkey) throw new Error('Table ' +obj.name + ' > Column '+column.name+' cannot have multiple foreign keys');
          sqlcol += ' foreign key references '+tbl+'('+column.foreignkey[tbl]+')';
          foundkey = true;
        }
      }
      if(!(typeof column.default == 'undefined')){
        var defaultval = '';
        if(column.default===null) defaultval = 'null';
        else if(_.isString(column.default)) defaultval = "'" + this.sql.escape(column.default) + "'";
        else if(_.isNumber(column.default)) defaultval = this.sql.escape(column.default.toString());
        else if(_.isBoolean(column.default)) defaultval = (column.default?"1":"0");
        if(defaultval) sqlcol += ' default ' + defaultval;
      }
      
      sqlcols.push(sqlcol);
    }
    sql += sqlcols.join(',\n') + '\n';
    if(obj.unique && obj.unique.length){
      for(var i=0;i<obj.unique.length;i++){
        var uniq = obj.unique[i];
        if(uniq && uniq.length){
          if(sqlcols.length) sql += '  , ';
          var cname = obj.name.replace(/\W/g, '_');
          sql += 'constraint unique_'+cname+'_'+(i+1).toString()+' unique (' + uniq.join(',') + ')\n';
        }
      }
    }
    sql += ');\nGO\n';
    var { schema: tableschema, name: tablename } = this.parseSchema(obj.name);
    sql += "exec sys.sp_addextendedproperty @name=N'MS_Description', @value=N'"+this.sql.escape(caption[2])+"' , @level0type=N'SCHEMA',@level0name=N'"+this.sql.escape(tableschema)+"', @level1type=N'TABLE',@level1name=N'"+this.sql.escape(tablename)+"';\nGO\n";
  }
  else if(obj.type=='code'){
    var jsHarmonyFactorySchema = this.getjsHarmonyFactorySchema(jsh);
    if(jsHarmonyFactorySchema) jsHarmonyFactorySchema += '.';
    var { schema: codeschema, name: codename } = this.parseSchema(obj.name);
    if(codename.substr(0,5)=='code_') codename = codename.substr(5);
    var code_type = 'sys';
    if(obj.code_type && (obj.code_type=='app')) code_type = 'app';
    //sql += "insert into "+jsHarmonyFactorySchema+jsh.map['code_'+code_type]+" (code_name, code_desc, code_schema, code_type) VALUES ('"+obj.name+"', '"+caption[2]+"', '{schema}', '"+code_type+"');\nGO\n";
    sql += "exec "+jsHarmonyFactorySchema+"create_code_"+code_type+" '"+this.sql.escape(codeschema)+"','"+this.sql.escape(codename)+"','"+this.sql.escape(caption[2])+"';\nGO\n";
  }
  return sql;
}

DBObjectSQL.prototype.escapeVal = function(val){
  if(val===null) return 'null';
  else if(_.isString(val)) return "N'" + this.sql.escape(val) + "'";
  else if(_.isBoolean(val)) return (val?'1':'0');
  else if(val && val.sql) return val.sql;
  else return this.sql.escape(val.toString());
}

DBObjectSQL.prototype.getRowInsert = function(jsh, module, obj, row){
  var _this = this;

  row = _.extend({}, row);
  var files = [];
  if(row._FILES){
    files = row._FILES;
    delete row._FILES;
  }

  var sql = 'insert into '+obj.name+'('+_.keys(row).join(',')+') select ';
  sql += _.map(_.values(row), function(val){ return _this.escapeVal(val); }).join(',');
  sql += " where not exists (select * from "+obj.name+" where ";
  var data_keys = (obj.data_keys ? obj.data_keys : _.keys(row));
  sql += _.map(data_keys, function(key){ return key+'='+_this.escapeVal(row[key]); }).join(' and ');
  sql += ");\n";

  for(var file_src in files){
    var file_dst = path.join(jsh.Config.datadir,files[file_src]);
    file_src = path.join(path.dirname(obj.path),'data_files',file_src);
    file_dst = _this.sql.escape(file_dst);
    file_dst = DB.util.ReplaceAll(file_dst,'{{',"'+cast(");
    file_dst = DB.util.ReplaceAll(file_dst,'}}'," as nvarchar)+'");
    sql += "select '%%%copy_file:"+_this.sql.escape(file_src)+">"+file_dst+"%%%' from "+obj.name+" where "+_this.getInsertKey(obj, obj.name, row)+";\n";
  }

  if(sql){
    var objFuncs = _.extend({
      'TABLENAME': obj.name
    }, triggerFuncs);
    sql = this.db.ParseSQLFuncs(sql, objFuncs);
  }

  return sql;
}

DBObjectSQL.prototype.getKeyJoin = function(obj, tbl1, tbl2){
  var primary_keys = [];
  var joinexp = [];
  _.each(obj.columns, function(col){
    if(col.key) joinexp.push(tbl1+"."+col.name+"="+tbl2+"."+col.name);
  });
  if(!joinexp.length) throw new Error('No primary key in table '+obj.name);
  return joinexp;
}

DBObjectSQL.prototype.getInsertKey = function(obj, tbl, data){
  var _this = this;
  var primary_keys = [];
  var joinexp = [];
  _.each(obj.columns, function(col){
    if(col.key){
      if(col.identity) joinexp.push(tbl+"."+col.name+"=scope_identity()");
      else joinexp.push(tbl+"."+col.name+"="+_this.escapeVal);
    }
  });
  if(!joinexp.length) throw new Error('No primary key in table '+obj.name);
  return joinexp;
}

DBObjectSQL.prototype.resolveTrigger = function(obj, type){
  var _this = this;
  var sql = '';
  
  if(type=='insert'){
    _.each(obj.columns, function(col){
      if(col.default && col.default.sql){
        sql += "update "+obj.name+" set "+col.name+"="+col.default.sql+" from inserted where "+obj.name+"."+col.name+" is null and "+_this.getKeyJoin(obj,obj.name,'inserted').join(' and ');
        sql+=";\n";
      }
    });
  }

  if(type=='validate_update'){
    _.each(obj.columns, function(col){
      if(col.actions && _.includes(col.actions, 'prevent_update')){
        sql += "if(update("+col.name+")) begin raiserror('Cannot update column "+_this.sql.escape(col.name)+"',16,1); rollback transaction; return; end;\n";
      }
    });
  }

  _.each(obj.triggers, function(trigger){
    if(_.includes(trigger.on,type)){
      if(trigger.sql) sql += trigger.sql + "\n";
      if(trigger.exec){
        var execsql = '';
        if(_.isArray(trigger.exec)) execsql = trigger.exec.join(';\n');
        else execsql = trigger.exec.toString();
        sql += execsql + "\n";
      }
    }
  });
  if(sql){
    var objFuncs = _.extend({
      'TABLENAME': obj.name,
      'INSERTTABLEKEYJOIN': _this.getKeyJoin(obj,obj.name,'inserted').join(' and '),
      'INSERTDELETEKEYJOIN': _this.getKeyJoin(obj,'inserted','deleted').join(' and ')
    }, triggerFuncs);
    sql = this.db.ParseSQLFuncs(sql, objFuncs);
  }
  return sql;
}


DBObjectSQL.prototype.getTriggers = function(jsh, module, obj){
  var _this = this;
  var rslt = {};
  _.each(['validate_insert','validate_update','insert','update','delete'], function(op){
    var sql = _this.resolveTrigger(obj, op);
    if(sql) rslt[op] = sql;
  });
  return rslt;;
}

DBObjectSQL.prototype.restructureInit = function(jsh, module, obj){
  var sql = '';
  var triggers = this.getTriggers(jsh, module, obj);
  //Apply trigger functions

  if(triggers.validate_insert){
    sql += 'create trigger '+obj.name+'_on_validate_insert on '+obj.name+' for insert as\n'
    sql += 'begin\n' + triggers.validate_insert + '\nend\nGO\n';
  }
  if(triggers.validate_update){
    sql += 'create trigger '+obj.name+'_on_validate_update on '+obj.name+' for update as\n'
    sql += 'begin\n' + triggers.validate_update + '\nend\nGO\n';
  }
  if(triggers.insert){
    sql += 'create trigger '+obj.name+'_on_insert on '+obj.name+' for insert as\n'
    sql += 'begin\n' + triggers.insert + '\nend\nGO\n';
  }
  if(triggers.update){
    sql += 'create trigger '+obj.name+'_on_update on '+obj.name+' for update as\n'
    sql += 'begin\n' + triggers.update + '\nend\nGO\n';
  }
  if(triggers.delete){
    sql += 'create trigger '+obj.name+'_on_delete on '+obj.name+' for delete as\n'
    sql += 'begin\n' + triggers.delete + '\nend\nGO\n';
  }
  return sql;
}

DBObjectSQL.prototype.restructureDrop = function(jsh, module, obj){
  var sql = '';
  var triggers = this.getTriggers(jsh, module, obj);
  _.each(['validate_insert','validate_update','insert','update','delete'], function(op){
    if(triggers[op]){
      var triggerName = obj.name+"_on_"+op;
      sql += "if (object_id(N'"+triggerName+"') is not null) drop trigger "+triggerName+";\nGO\n";
    }
  });
  return sql;
}

DBObjectSQL.prototype.initData = function(jsh, module, obj){
  var sql = '';
  if(obj.init_data && obj.init_data.length){
    for(var i=0;i<obj.init_data.length;i++){
      var row = obj.init_data[i];
      sql += this.getRowInsert(jsh, module, obj, row);
    }
    sql += 'GO\n';
  }
  return sql;
}

DBObjectSQL.prototype.sampleData = function(jsh, module, obj){
  var sql = '';
  if(obj.sample_data && obj.sample_data.length){
    for(var i=0;i<obj.sample_data.length;i++){
      var row = obj.sample_data[i];
      sql += this.getRowInsert(jsh, module, obj, row);
    }
    sql += 'GO\n';
  }
  return sql;
}

DBObjectSQL.prototype.drop = function(jsh, module, obj){
  var sql = '';
  if(obj.type=='table'){
    sql += "if (object_id('"+this.sql.escape(obj.name)+"', 'U') is not null) drop table "+(obj.name)+";\n";
  }
  else if(obj.type=='code'){
    sql += "if (object_id('"+this.sql.escape(obj.name)+"', 'U') is not null) drop table "+(obj.name)+";\n";
  }
  if(sql) sql += 'GO\n'
  return sql;
}

DBObjectSQL.prototype.initSchema = function(jsh, module){
  if(module && module.schema) return 'create schema '+module.schema+';\nGO\n';
  return '';
}

DBObjectSQL.prototype.dropSchema = function(jsh, module){
  if(module && module.schema) return "if (exists(select name from sys.schemas where name = N'"+this.sql.escape(module.schema)+"')) drop schema "+module.schema+";\n";
  return '';
}

exports = module.exports = DBObjectSQL;