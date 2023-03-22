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
var _ = require('lodash');
var triggerFuncs = require('./DB.mssql.triggerfuncs.js');
var path = require('path');

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
  else if(column.type=='varbinary') return 'varbinary'+length;
  else if(column.type=='bigint') return 'bigint';
  else if(column.type=='int') return 'int';
  else if(column.type=='smallint') return 'smallint';
  else if(column.type=='tinyint') return 'tinyint';
  else if(column.type=='boolean') return 'bit';
  else if(column.type=='date') return 'date';
  else if(column.type=='time') return 'time'+prec;
  else if(column.type=='datetime'){
    if(prec) return 'datetime2'+prec;
    return 'datetime';
  }
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
};

DBObjectSQL.prototype.parseSchema = function(name){
  name = name || '';
  var rslt = {
    schema: '',
    name: name
  };
  var idx = name.indexOf('.');
  if(idx>=0){
    rslt.schema = name.substr(0,idx);
    rslt.name = name.substr(idx+1);
  }
  return rslt;
};

DBObjectSQL.prototype.init = function(jsh, module, obj){
  var _this = this;
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
  var objstrname = '';
  if(obj.name) objstrname = obj.name.replace(/\W/g, '_');
  if('sql_create' in obj) sql = DB.util.ParseMultiLine(obj.sql_create)+'\nGO\n';
  else if((obj.type=='table') && obj.columns){
    sql += 'create table '+obj.name+'(\n';
    var sqlcols = [];
    var sqlforeignkeys = [];
    var sqlprimarykeys = [];
    var sqlunique = [];
    if(obj.columns) for(let i=0; i<obj.columns.length;i++){
      var column = obj.columns[i];
      var sqlcol = '  '+column.name;
      sqlcol += ' '+getDBType(column);
      if(column.identity) sqlcol += ' identity';
      if(column.key) sqlprimarykeys.push(column.name);
      if(column.unique) sqlunique.push([column.name]);
      if(!column.null) sqlcol += ' not null';
      if(column.foreignkey){
        var foundkey = false;
        for(let tbl in column.foreignkey){
          if(foundkey) throw new Error('Table ' +obj.name + ' > Column '+column.name+' cannot have multiple foreign keys');
          var foreignkey_col = column.foreignkey[tbl];
          if(_.isString(foreignkey_col)) foreignkey_col = { column: foreignkey_col };
          var foreignkey = ' constraint fk_'+objstrname+'_'+column.name+' foreign key ('+column.name+') references '+tbl+'('+foreignkey_col.column+')';
          if(foreignkey_col.on_delete){
            if(foreignkey_col.on_delete=='cascade') foreignkey += ' on delete cascade';
            else if(foreignkey_col.on_delete=='null') foreignkey += ' on delete set null';
            else throw new Error('Table ' +obj.name + ' > Column '+column.name+' - column.foreignkey.on_delete action not supported.');
          }
          if(foreignkey_col.on_update){
            if(foreignkey_col.on_update=='cascade') foreignkey += ' on update cascade';
            else if(foreignkey_col.on_update=='null') foreignkey += ' on update set null';
            else throw new Error('Table ' +obj.name + ' > Column '+column.name+' - column.foreignkey.on_update action not supported.');
          }
          sqlforeignkeys.push(foreignkey);
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
    if(obj.foreignkeys){
      _.each(obj.foreignkeys, function(foreignkey){
        if(!foreignkey.columns || !foreignkey.columns.length) throw new Error('Table ' +obj.name + ' > Foreign Key missing "columns" property');
        if(!foreignkey.foreign_table) throw new Error('Table ' +obj.name + ' > Foreign Key (' + foreignkey.columns.join(',') + ') missing "foreign_table" property');
        if(!foreignkey.foreign_columns || !foreignkey.foreign_columns.length) throw new Error('Table ' +obj.name + ' > Foreign Key (' + foreignkey.columns.join(',') + ') missing "foreign_columns" property');
        var fkeyname = 'fk_'+objstrname+'_'+foreignkey.columns.join('_');
        var sqlforeignkey = ' constraint '+fkeyname+' foreign key (' + foreignkey.columns.join(',') + ') references ' + foreignkey.foreign_table + '(' + foreignkey.foreign_columns.join(',') + ')';
        if(foreignkey.on_delete){
          if(foreignkey.on_delete=='cascade') sqlforeignkey += ' on delete cascade';
          else if(foreignkey.on_delete=='null') sqlforeignkey += ' on delete set null';
          else throw new Error('Table ' +obj.name + ' > Foreign Key (' + foreignkey.columns.join(',') + ') - on_delete action not supported.');
        }
        if(foreignkey.on_update){
          if(foreignkey.on_update=='cascade') sqlforeignkey += ' on update cascade';
          else if(foreignkey.on_update=='null') sqlforeignkey += ' on update set null';
          else throw new Error('Table ' +obj.name + ' > Foreign Key (' + foreignkey.columns.join(',') + ') - on_update action not supported.');
        }
        sqlforeignkeys.push(sqlforeignkey);
      });
    }
    sqlcols = sqlcols.concat(sqlforeignkeys);
    sql += sqlcols.join(',\n') + '\n';
    if(sqlprimarykeys.length) sql += ',\n' + ' constraint pk_'+objstrname+'_'+sqlprimarykeys.join('_')+' primary key (' + sqlprimarykeys.join(',') + ')\n';
    sqlunique = sqlunique.concat(obj.unique||[]);
    var unique_names = {};
    for(let i=0;i<sqlunique.length;i++){
      var uniq = sqlunique[i];
      if(uniq && uniq.length){
        if(sqlcols.length) sql += '  , ';
        var baseunqname = (obj.name+'_'+uniq.join('_')).replace(/\W/g, '_');
        var unqname = baseunqname;
        for(let j=1;(unqname in unique_names);j++) unqname = baseunqname + '_' + j.toString();
        unique_names[unqname] = true;
        sql += 'constraint unique_'+unqname+' unique (' + uniq.join(',') + ')\n';
      }
    }
    sql += ');\nGO\n';
    if(obj.index && obj.index.length){
      var index_names = {};
      for(let i=0;i<obj.index.length;i++){
        var index = obj.index[i];
        if(index && index.columns && index.columns.length){
          var baseidxname = (obj.name+'_'+index.columns.join('_')).replace(/\W/g, '_');
          var idxname = baseidxname;
          for(let j=1;(idxname in index_names);j++) idxname = baseidxname + '_' + j.toString();
          index_names[idxname] = true;
          sql += 'create index index_'+idxname+' on ' + obj.name + '(' + index.columns.join(',') + ');\nGO\n';
        }
      }
    }
    var { schema: tableschema, name: tablename } = this.parseSchema(obj.name);
    sql += "exec sys.sp_addextendedproperty @name=N'MS_Description', @value=N'"+this.sql.escape(caption[2])+"' , @level0type=N'SCHEMA',@level0name=N'"+this.sql.escape(tableschema?tableschema:_this.db.getDefaultSchema())+"', @level1type=N'TABLE',@level1name=N'"+this.sql.escape(tablename)+"';\nGO\n";
  }
  else if(obj.type=='view'){
    sql += 'create view '+obj.name+' as \n';
    if(obj.with){
      sql += ' with ';
      var first_with = true;
      for(var withName in obj.with){
        var withExpr = obj.with[withName];
        if(!first_with) sql += ',';
        if(_.isString(withExpr)||_.isArray(withExpr)){
          sql += withName+' as ('+DB.util.ParseMultiLine(withExpr)+')';
        }
        else {
          if(withExpr.recursive) sql += withName+'('+withExpr.recursive.join(',')+')';
          else sql += withName;
          sql += ' as (';
          sql += DB.util.ParseMultiLine(withExpr.sql);
          sql += ')';
          first_with = false;
        }
      }
    }
    sql += ' select \n';
    if(obj.distinct) sql += 'distinct ';
    var cols = [];
    var from = [];
    for(var tblname in obj.tables){
      let tbl = obj.tables[tblname];
      _.each(tbl.columns, function(col){
        var colname = col.name;
        if(col.sqlselect){
          var colsql = DB.util.ParseMultiLine(col.sqlselect);
          if(col.type){
            colsql = 'cast(' + colsql + ' as ' + getDBType(col) + ')';
          }
          cols.push('(' + colsql + ') as ' + col.name);
        }
        else {
          var resolveSchema = (!tbl.table && !tbl.sql);
          if(colname.indexOf('.')<0){
            colname = tblname + '.' + colname;
            if(obj.with && (tblname in obj.with)) resolveSchema = false;
          }
          var numdots = (colname.match(/\./g) || []).length;
          if(resolveSchema && (numdots < 2)){
            let { schema: tbl_schema } = _this.parseSchema(obj.name);
            if(!tbl_schema) tbl_schema = _this.db.getDefaultSchema();
            if(tbl_schema) colname = tbl_schema + '.' + colname;
          }
          cols.push(colname);
        }
      });
      if(tbl.join_type){
        var join = '';
        if(tbl.join_type=='inner') join = 'inner join';
        else if(tbl.join_type=='left') join = 'left outer join';
        else if(tbl.join_type=='right') join = 'right outer join';
        else throw new Error('View ' +obj.name + ' > ' + tblname + ' join_type must be inner, left, or right');
        if(tbl.sql) join += ' (' + DB.util.ParseMultiLine(tbl.sql) + ') ';
        else if(tbl.table) join += ' ' + tbl.table + ' as ';
        join += ' ' + tblname;
        if(tbl.join_columns){
          var join_cols = [];
          if(_.isArray(tbl.join_columns)){
            join_cols = tbl.join_columns;
          }
          else {
            for(var joinsrc in tbl.join_columns){
              var joinval = tbl.join_columns[joinsrc];
              var joinexp = joinsrc + '=' + tbl.join_columns[joinsrc];
              if((joinval||'').toUpperCase()=='NULL') joinexp = joinsrc + ' is ' + tbl.join_columns[joinsrc];
              join_cols.push(joinexp);
            }
          }
          if(join_cols.length) join += ' on ' + join_cols.map(function(expr){ return '(' + expr.toString() + ')'; }).join(' and ');
        }
        else join += ' on 1=1';
        from.push(join);
      }
      else{
        if(tbl.sql) from.push('(' + DB.util.ParseMultiLine(tbl.sql) + ') '+tblname);
        else if(tbl.table) from.push(tbl.table + ' as '+tblname);
        else from.push(tblname);
      }
    }
    sql += cols.join(',\n    ') + ' \n  from ' + from.join('\n    ');
    var sqlWhere = DB.util.ParseMultiLine(obj.where || '').trim();
    if(sqlWhere) sql += '\n  where ' + sqlWhere;
    var sqlGroupBy = DB.util.ParseMultiLine(obj.group_by || '').trim();
    if(sqlGroupBy) sql += '\n  group by ' + sqlGroupBy;
    var sqlHaving = DB.util.ParseMultiLine(obj.having || '').trim();
    if(sqlHaving) sql += '\n  having ' + sqlHaving;
    var sqlOrderBy = DB.util.ParseMultiLine(obj.order_by || '').trim();
    if(sqlOrderBy) sql += '\n  order by ' + sqlOrderBy;
    sql += ';\nGO\n';
  }
  else if(obj.type=='code'){
    let jsHarmonyFactorySchema = this.getjsHarmonyFactorySchema(jsh);
    if(jsHarmonyFactorySchema) jsHarmonyFactorySchema += '.';
    let { schema: codeschema, name: codename } = this.parseSchema(obj.name);
    if(codename.substr(0,5)=='code_') codename = codename.substr(5);
    let code_type = 'sys';
    if(obj.code_type && (obj.code_type=='app')) code_type = 'app';
    let sql_codeschema = (codeschema ? "'"+this.sql.escape(codeschema)+"'" : 'null');
    sql += "insert into "+jsHarmonyFactorySchema+jsh.map['code_'+code_type]+" (code_name, code_desc, code_schema, code_type) VALUES ('"+this.sql.escape(codename)+"', '"+this.sql.escape(caption[2])+"', "+sql_codeschema+", '"+code_type+"');\nGO\n";
    sql += "exec "+jsHarmonyFactorySchema+"create_code_"+code_type+" "+sql_codeschema+",'"+this.sql.escape(codename)+"','"+this.sql.escape(caption[2])+"';\nGO\n";
  }
  else if(obj.type=='code2'){
    let jsHarmonyFactorySchema = this.getjsHarmonyFactorySchema(jsh);
    if(jsHarmonyFactorySchema) jsHarmonyFactorySchema += '.';
    let { schema: codeschema, name: codename } = this.parseSchema(obj.name);
    if(codename.substr(0,6)=='code2_') codename = codename.substr(6);
    let code_type = 'sys';
    if(obj.code_type && (obj.code_type=='app')) code_type = 'app';
    let sql_codeschema = (codeschema ? "'"+this.sql.escape(codeschema)+"'" : 'null');
    sql += "insert into "+jsHarmonyFactorySchema+jsh.map['code2_'+code_type]+" (code_name, code_desc, code_schema, code_type) VALUES ('"+this.sql.escape(codename)+"', '"+this.sql.escape(caption[2])+"', "+sql_codeschema+", '"+code_type+"');\nGO\n";
    sql += "exec "+jsHarmonyFactorySchema+"create_code2_"+code_type+" "+sql_codeschema+",'"+this.sql.escape(codename)+"','"+this.sql.escape(caption[2])+"';\nGO\n";
  }

  if(obj.init && obj.init.length){
    for(let i=0;i<obj.init.length;i++){
      var row = obj.init[i];
      sql += this.getRowInsert(jsh, module, obj, row);
    }
  }

  if(sql) sql = this.db.ParseSQLFuncs(sql, this.getTriggerFuncs());
  return sql;
};

DBObjectSQL.prototype.escapeVal = function(val){
  if(val===null) return 'null';
  else if(typeof val == 'undefined') return 'null';
  else if(_.isString(val)) return "N'" + this.sql.escape(val) + "'";
  else if(_.isBoolean(val)) return (val?'1':'0');
  else if(val && val.sql) return '('+val.sql+')';
  else return this.sql.escape(val.toString());
};

DBObjectSQL.prototype.getRowInsert = function(jsh, module, obj, row){
  var _this = this;

  row = _.extend({}, row);
  var files = [];
  if(row._FILES){
    files = row._FILES;
    delete row._FILES;
  }

  var sql = '';
  var no_file_rowid = false;
  if(_.keys(row).length==0){ no_file_rowid = true; }
  else if((_.keys(row).length==1) && ('sql' in row)){
    sql = DB.util.ParseMultiLine(row.sql).trim();
    if(sql[sql.length-1] != ';') sql = sql + ';';
    sql += '\n';
    no_file_rowid = true;
  }
  else{
    sql = 'insert into '+obj.name+'('+_.keys(row).join(',')+') select ';
    sql += _.map(_.values(row), function(val){ return _this.escapeVal(val); }).join(',');
    var data_keys = (obj.data_keys ? obj.data_keys : _.keys(row));
    if(data_keys.length){
      sql += " where not exists (select * from "+obj.name+" where ";
      sql += _.map(data_keys, function(key){
        var val = _this.escapeVal(row[key]);
        if(val==='null') return key+' is '+val;
        return key+'='+val;
      }).join(' and ');
      sql += ")";
    }
    sql += ";\n";
  }

  for(var file_src in files){
    var file_dst = path.join(jsh.Config.datadir,files[file_src]);
    file_src = path.join(path.dirname(obj.path),'data_files',file_src);
    file_dst = _this.sql.escape(file_dst);
    file_dst = DB.util.ReplaceAll(file_dst,'{{',"'+cast(");
    file_dst = DB.util.ReplaceAll(file_dst,'}}'," as nvarchar)+'");

    if(no_file_rowid){
      sql += "select '%%%copy_file:"+_this.sql.escape(file_src)+">"+file_dst+"%%%';\n";
    }
    else {
      sql += "select '%%%copy_file:"+_this.sql.escape(file_src)+">"+file_dst+"%%%' from "+obj.name+" where "+_this.getInsertKey(obj, obj.name, row)+";\n";
    }
  }

  if(sql){
    var objFuncs = _.extend({
      'TABLENAME': obj.name
    }, _this.getTriggerFuncs());
    sql = this.db.ParseSQLFuncs(sql, objFuncs);
  }

  return sql;
};

DBObjectSQL.prototype.getTriggerFuncs = function(){
  return _.extend({}, this.db.SQLExt.Funcs, triggerFuncs);
};

DBObjectSQL.prototype.getKeys = function(obj){
  var rslt = [];
  _.each(obj.columns, function(col){
    if(col.key) rslt.push(col.name);
  });
  return rslt;
};

DBObjectSQL.prototype.getKeyJoin = function(obj, tbl1, tbl2, options){
  options = _.extend({ no_errors: false, cursor: false, null_join: false }, options);
  var joinexp = [];
  _.each(obj.columns, function(col){
    if(col.key){
      if(options.cursor) joinexp.push(tbl1+"."+col.name+"=@"+tbl2+"_"+col.name);
      else if(options.null_join){
        joinexp.push('(('+tbl1+"."+col.name+"="+tbl2+"."+col.name+') or ('+tbl1+"."+col.name+" is null and "+tbl2+"."+col.name+' is null))');
      }
      else joinexp.push(tbl1+"."+col.name+"="+tbl2+"."+col.name);
    }
  });
  if(!options.no_errors && !joinexp.length) throw new Error('Cannot define join expression between '+tbl1+' and '+tbl2+': No primary key in table '+obj.name);
  return joinexp;
};

DBObjectSQL.prototype.getInsertKey = function(obj, tbl, data){
  var _this = this;
  var joinexp = [];
  _.each(obj.columns, function(col){
    if(col.key){
      if(col.identity) joinexp.push(tbl+"."+col.name+"=scope_identity()");
      else joinexp.push(tbl+"."+col.name+"="+_this.escapeVal(data[col.name]));
    }
  });
  if(!joinexp.length) throw new Error('Cannot define inserted key expression for '+tbl+': No primary key in table '+obj.name);
  return joinexp;
};

function trimRightComma(sql){
  if(!sql) return sql;
  sql = sql.trim();
  if(sql[sql.length-1]==',') sql = sql.substr(0,sql.length-1);
  return sql;
}

function trimSemicolons(sql){
  var trim_sql;
  while((trim_sql = sql.replace(/;\s*\n\s*;/g, ";")) != sql) sql = trim_sql;
  return sql;
}

DBObjectSQL.prototype.resolveTrigger = function(obj, type, prefix){
  prefix = prefix || '';
  var _this = this;
  var sql = '';
  var rowsql = '';

  if(!prefix){
    if(obj.type=='table'){
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
            rowsql += "if(update("+col.name+")) begin raiserror('Cannot update column "+_this.sql.escape(col.name)+"',16,1); rollback transaction; return; end;\n";
          }
        });
      }
    }
  }

  _.each(obj.triggers, function(trigger){
    if((trigger.prefix||'') != prefix) return;
    if(_.includes(trigger.on,type)){
      if(trigger.sql) rowsql += trigger.sql + "\n";
      if(trigger.exec){
        var execsql = '';
        if(!_.isArray(trigger.exec)) trigger.exec = [trigger.exec];
        execsql = _.map(trigger.exec, function(tsql){
          if(_.isArray(tsql)){
            var s_tsql = '';
            for(var i=0;i<tsql.length;i++){
              var cur_tsql = tsql[i].trim();
              s_tsql += cur_tsql + ' ';
            }
            tsql = s_tsql;
          }
          tsql = tsql.trim();
          while(tsql[tsql.length-1]==';'){ tsql = tsql.substr(0, tsql.length-1); }
          return tsql;
        }).join(';\n');
        rowsql += execsql + "\n";
      }
    }
  });

  if(sql || rowsql){
    var rowkey = '';
    if((type=='insert') || (type=='validate_insert')){
      rowkey = _this.getKeyJoin(obj,obj.name,'inserted', { no_errors: true, cursor: true }).join(' and ');
    }
    else {
      rowkey = _this.getKeyJoin(obj,obj.name,'deleted', { no_errors: true, cursor: true }).join(' and ');
    }
    var objFuncs = _.extend({
      'TABLENAME': obj.name,
      'ROWKEY': rowkey,
    }, _this.getTriggerFuncs());
    if(sql){
      sql = this.db.ParseSQLFuncs(sql, objFuncs);
      sql = trimSemicolons(sql);
    }
    if(rowsql){
      rowsql = this.db.ParseSQLFuncs(rowsql, objFuncs);
      rowsql = trimSemicolons(rowsql);
      rowsql = rowsql.trim();
      if(rowsql){
        if(rowsql[rowsql.length-1] != ';') rowsql += ';';
      }
    }

    var presql = 'declare rowcur cursor local for select ';
    _.each(obj.columns, function(col){
      presql += 'deleted.'+col.name+', inserted.'+col.name+',';
    });
    presql = trimRightComma(presql);
    presql += ' from deleted full outer join inserted on '+(_this.getKeyJoin(obj,'inserted','deleted', { no_errors: true, null_join: ((type=='update') || (type=='validate_update')) }).join(' and ')||'1=1');
    presql += ';\n';

    _.each(obj.columns, function(col){
      if(!col.type) throw new Error('Column '+obj.name+' '+col.name+' missing type');
      presql += 'declare @deleted_'+col.name+' ' + getDBType(col) + ';\n';
      presql += 'declare @inserted_'+col.name+' ' + getDBType(col) + ';\n';
    });

    presql += "declare @TP char(1);\n";
    presql += "if exists (select * from inserted) \n";
    presql += "  if exists (select * from deleted) \n";
    presql += "    set @TP = 'U';\n";
    presql += "  else \n";
    presql += "    set @TP = 'I';\n";
    presql += "else \n";
    presql += "  if exists (select * from deleted) \n";
    presql += "    set @TP = 'D';\n";
    presql += "  else \n";
    presql += "    begin \n";
    presql += "      return;\n";
    presql += "    end;\n";

    presql += "open rowcur;\n";
    presql += "fetch next from rowcur into ";
    _.each(obj.columns, function(col){
      presql += '@deleted_'+col.name+',';
      presql += '@inserted_'+col.name+',';
    });
    presql = trimRightComma(presql) + ';\n';
    presql += 'while (@@fetch_status = 0) begin \n';




    var postsql = "fetch next from rowcur into ";
    _.each(obj.columns, function(col){
      postsql += '@deleted_'+col.name+',';
      postsql += '@inserted_'+col.name+',';
    });
    postsql = trimRightComma(postsql) + ';\n';
    postsql += 'end\n';
    postsql += 'close rowcur;\n';
    postsql += 'deallocate rowcur;\n';
    postsql += 'return;\n';

    if(rowsql){
      sql += '\r\n' + presql + '\n\n\n' + rowsql + '\n\n\n' + postsql;
    }
  }
  return sql;
};


DBObjectSQL.prototype.getTriggers = function(jsh, module, obj, prefix){
  var _this = this;
  var rslt = {};
  _.each(['validate_insert','validate_update','insert','update','delete'], function(op){
    var sql = _this.resolveTrigger(obj, op, prefix);
    if(sql) rslt[op] = sql;
  });
  return rslt;
};

DBObjectSQL.prototype.restructureInit = function(jsh, module, obj, prefix){
  prefix = prefix || '';
  var _this = this;
  var sql = '';
  var triggers = this.getTriggers(jsh, module, obj, prefix);
  //Apply trigger functions

  var trigger_types = [];
  _.each(obj.triggers, function(trigger){
    _.each(trigger.on, function(trigger_type){
      if(!_.includes(trigger_types, trigger_type)) trigger_types.push(trigger_type);
    });
  });
  for(var trigger_type in triggers){
    if(!_.includes(trigger_types, trigger_type)) trigger_types.push(trigger_type);
  }

  _.each(trigger_types, function(trigger_type){
    if(!triggers[trigger_type]) return;
    if(obj.type=='table'){
      if(trigger_type=='validate_insert'){
        sql += 'create trigger '+obj.name+'_'+prefix+'on_validate_insert on '+obj.name+' for insert as\n';
        sql += 'begin\n' + triggers.validate_insert + '\nend\nGO\n';
      }
      if(trigger_type=='validate_update'){
        sql += 'create trigger '+obj.name+'_'+prefix+'on_validate_update on '+obj.name+' for update as\n';
        sql += 'begin\n' + triggers.validate_update + '\nend\nGO\n';
      }
      if(trigger_type=='insert'){
        sql += 'create trigger '+obj.name+'_'+prefix+'on_insert on '+obj.name+' for insert as\n';
        sql += 'begin\n' + triggers.insert + '\nend\nGO\n';
      }
      if(trigger_type=='update'){
        sql += 'create trigger '+obj.name+'_'+prefix+'on_update on '+obj.name+' for update as\n';
        sql += 'begin\n' + triggers.update + '\nend\nGO\n';
      }
      if(trigger_type=='delete'){
        sql += 'create trigger '+obj.name+'_'+prefix+'on_delete on '+obj.name+' for delete as\n';
        sql += 'begin\n' + triggers.delete + '\nend\nGO\n';
      }
    }
    else if(obj.type=='view'){
      if(trigger_type=='insert'){
        sql += 'create trigger '+obj.name+'_'+prefix+'on_insert on '+obj.name+' instead of insert as\n';
        sql += 'begin\n' + triggers.insert + '\nend\nGO\n';
      }
      if(trigger_type=='update'){
        sql += 'create trigger '+obj.name+'_'+prefix+'on_update on '+obj.name+' instead of update as\n';
        sql += 'begin\n' + triggers.update + '\nend\nGO\n';
      }
      if(trigger_type=='delete'){
        sql += 'create trigger '+obj.name+'_'+prefix+'on_delete on '+obj.name+' instead of delete as\n';
        sql += 'begin\n' + triggers.delete + '\nend\nGO\n';
      }
    }
  });
  if(!prefix) _.each(_.uniq(_.map(obj.triggers, 'prefix')), function(_prefix){
    if(_prefix) sql += _this.restructureInit(jsh, module, obj, _prefix);
  });
  return sql;
};

DBObjectSQL.prototype.restructureDrop = function(jsh, module, obj, prefix){
  prefix = prefix || '';
  var _this = this;
  var sql = '';
  var triggers = this.getTriggers(jsh, module, obj, prefix);
  _.each(['validate_insert','validate_update','insert','update','delete'], function(op){
    if(triggers[op]){
      var triggerName = obj.name+"_"+prefix+"on_"+op;
      sql += "if (object_id(N'"+triggerName+"') is not null) drop trigger "+triggerName+";\nGO\n";
    }
  });
  if(!prefix) _.each(_.uniq(_.map(obj.triggers, 'prefix')), function(_prefix){
    if(_prefix) sql += _this.restructureDrop(jsh, module, obj, _prefix);
  });
  return sql;
};

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
};

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
};

DBObjectSQL.prototype.drop = function(jsh, module, obj){
  var sql = '';
  if('sql_drop' in obj) sql = DB.util.ParseMultiLine(obj.sql_drop)+'\n';
  else if((obj.type=='table') && obj.columns){
    sql += "if (object_id('"+this.sql.escape(obj.name)+"', 'U') is not null) drop table "+(obj.name)+";\n";
  }
  else if(obj.type=='view'){
    sql += "if (object_id('"+this.sql.escape(obj.name)+"', 'V') is not null) drop view "+(obj.name)+";\n";
  }
  else if(obj.type=='code'){
    let jsHarmonyFactorySchema = this.getjsHarmonyFactorySchema(jsh);
    if(jsHarmonyFactorySchema) jsHarmonyFactorySchema += '.';
    let { schema: codeschema, name: codename } = this.parseSchema(obj.name);
    if(codename.substr(0,5)=='code_') codename = codename.substr(5);
    let code_type = 'sys';
    if(obj.code_type && (obj.code_type=='app')) code_type = 'app';
    let sql_codeschema = (codeschema ? "='"+this.sql.escape(codeschema)+"'" : ' is null');
    sql += "if (object_id('"+this.sql.escape(obj.name)+"', 'U') is not null) drop table "+(obj.name)+";\nGO\n";
    sql += "delete from "+jsHarmonyFactorySchema+jsh.map['code_'+code_type]+" where code_name='"+this.sql.escape(codename)+"' and code_schema "+sql_codeschema+";\n";
  }
  else if(obj.type=='code2'){
    let jsHarmonyFactorySchema = this.getjsHarmonyFactorySchema(jsh);
    if(jsHarmonyFactorySchema) jsHarmonyFactorySchema += '.';
    let { schema: codeschema, name: codename } = this.parseSchema(obj.name);
    if(codename.substr(0,6)=='code2_') codename = codename.substr(6);
    let code_type = 'sys';
    if(obj.code_type && (obj.code_type=='app')) code_type = 'app';
    let sql_codeschema = (codeschema ? "='"+this.sql.escape(codeschema)+"'" : ' is null');
    sql += "if (object_id('"+this.sql.escape(obj.name)+"', 'U') is not null) drop table "+(obj.name)+";\nGO\n";
    sql += "delete from "+jsHarmonyFactorySchema+jsh.map['code2_'+code_type]+" where code_name='"+this.sql.escape(codename)+"' and code_schema "+sql_codeschema+";\n";
  }
  if(sql) sql += 'GO\n';
  return sql;
};

DBObjectSQL.prototype.initSchema = function(jsh, module){
  // RequestError: 'CREATE SCHEMA' must be the first statement in a query batch.
  //   possible cause: you have included a `context` option in the options, causing an invisible statement to be output before this one.
  //   GO won't fix it because those get replaced elsewhere in the library.
  if(module && module.schema) return 'create schema '+module.schema+';\nGO\n';
  return '';
};

DBObjectSQL.prototype.dropSchema = function(jsh, module){
  if(module && module.schema) return "if (exists(select name from sys.schemas where name = N'"+this.sql.escape(module.schema)+"')) drop schema "+module.schema+";\n";
  return '';
};

exports = module.exports = DBObjectSQL;