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

function DBmeta(db){
  this.db = db;
}

DBmeta.prototype.getTables = function(table, callback){
  var tables = [];
  var messages = [];
  var sql_param_types = [];
  var sql_params = {};
  var sql = "select \
      schemas.name schema_name, \
      tables.Name table_name, \
      extended_properties.value [description] \
      from sys.tables \
      inner join sys.schemas on sys.schemas.schema_id = sys.tables.schema_id \
      left outer join sys.extended_properties on extended_properties.major_id = tables.object_id and extended_properties.minor_id = 0 and extended_properties.name='MS_Description' \
      ";
  if(table){
    sql += "where sys.tables.name=@table_name and sys.schemas.name=@schema_name";
    sql_param_types = [dbtypes.VarChar(dbtypes.MAX), dbtypes.VarChar(dbtypes.MAX)];
    sql_params = {'schema_name':table.schema||'dbo','table_name':table.name};
  }
  this.db.Recordset('',sql,sql_param_types,sql_params,function(err,rslt){
    if(err){ return callback(err); }
    for(var i=0;i<rslt.length;i++){
      var dbtable = rslt[i];
      if(!table){
        if(dbtable.schema_name == 'jsharmony') continue;
        if(dbtable.schema_name == 'dbo'){
          if(dbtable.table_name == 'dtproperties') continue;
          if(dbtable.table_name == 'sysdiagrams') continue;
        }
      }
      tables.push({schema:dbtable.schema_name,name:dbtable.table_name,description:dbtable.description});
    }
    return callback(null, messages, tables);
  });
}

DBmeta.prototype.getTableFields = function(tabledef, callback){
  var _this = this;
  var fields = [];
  var messages = [];
  _this.db.Recordset('',"select \
      columns.name column_name, \
      types.name type_name, \
      columns.max_length, \
      columns.precision, \
      columns.scale, \
      case when columns.default_object_id = 1 or columns.is_nullable = 1 then 0 else 1 end as required, \
      case when columns.is_identity=1 or columns.is_computed=1 then 1 else 0 end as readonly, \
      extended_properties.value [description], \
      case when (select count(*) from sys.indexes inner join sys.index_columns on index_columns.object_id = indexes.object_id and index_columns.index_id = indexes.index_id where index_columns.column_id = columns.column_id and indexes.object_id = tables.object_id and indexes.is_primary_key=1) > 0 then 1 else 0 end primary_key \
      from sys.columns \
      inner join sys.tables on sys.tables.object_id = sys.columns.object_id \
      inner join sys.schemas on sys.schemas.schema_id = sys.tables.schema_id \
      inner join sys.types on sys.columns.user_type_id = sys.types.user_type_id \
      left outer join sys.extended_properties on extended_properties.major_id = tables.object_id and extended_properties.minor_id = columns.column_id and extended_properties.name='MS_Description' \
      where sys.tables.name=@table_name and sys.schemas.name=@schema_name \
      order by column_id",
      [dbtypes.VarChar(dbtypes.MAX), dbtypes.VarChar(dbtypes.MAX)],
      {'schema_name':tabledef.schema||'dbo','table_name':tabledef.name},
      function(err,rslt){
    if(err){ return callback(err); }

    //Convert to jsHarmony Data Types / Fields
    for(var i=0;i<rslt.length;i++){
      var col = rslt[i];
      var field = { name: col.column_name };
      if(col.type_name=="varchar" || 
         col.type_name=="nvarchar"){ 
        field.type = "varchar"; 
        field.length = col.max_length;
        if(field.length==-1){ /* MAX*/ }
        else if(col.type_name=="nvarchar") field.length = field.length / 2;
      }
      else if(col.type_name=="char" ||
              col.type_name=="nchar"){ 
        field.type = "char"; 
        field.length = col.max_length; 
        if(field.length==-1){ /* MAX*/ }
        else if(col.type_name=="nchar") field.length = field.length / 2;
      }
      else if(col.type_name=="text" ||
              col.type_name=="ntext"){ 
        field.type = "varchar"; 
        field.length = -1;
      }

      /*
      else if(col.type_name=="datetime2"){ field.type = "datetime"; field.length = col.scale; }
      else if(col.type_name=="datetime"){ field.type = "datetime"; field.length = col.scale; }
      else if(col.type_name=="date"){ field.type = "date"; }
      else if(col.type_name=="time"){ field.type = "time"; field.length = col.scale; }
      */

      else if(col.type_name=="bigint"){ field.type = "bigint"; }
      else if(col.type_name=="int"){ field.type = "int"; }
      else if(col.type_name=="smallint"){ field.type = "smallint"; }
      else if(col.type_name=="bit"){ field.type = "bit"; }

      else if((col.type_name=="decimal")||(col.type_name=="numeric")){ field.type = "decimal"; field.precision = [col.precision, col.scale]; }
      else if(col.type_name=="money"){ field.type = "money"; }
      else if(col.type_name=="smallmoney"){ field.type = "smallmoney"; }

      else if(col.type_name=="binary"){ field.type = "binary"; field.length = col.max_length; }
      else if(col.type_name=="varbinary"){ field.type = "varbinary"; field.length = col.max_length; }
      else if(col.type_name=="image"){ field.type = "image"; }
      else if(col.type_name=="timestamp"){ field.type = "timestamp"; }
      else if(col.type_name=="uniqueidentifier"){ field.type = "uniqueidentifier"; }
      else if(col.type_name=="sql_variant"){ field.type = "sql_variant"; }
      else if(col.type_name=="hierarchyid"){ field.type = "hierarchyid"; }
      else if(col.type_name=="geometry"){ field.type = "geometry"; }
      else if(col.type_name=="geography"){ field.type = "geography"; }
      else if(col.type_name=="xml"){ field.type = "xml"; }
      else if(col.type_name=="sysname"){ field.type = "sysname"; }

      else{
        messages.push('WARNING - Skipping Column: '+tabledef.schema+'.'+tabledef.name+'.'+col.column_name+': Data type '+col.type_name + ' not supported.');
        continue;
      }
      field.coldef = col;
      fields.push(field);
    }
    return callback(null, messages, fields);
  });
}

exports = module.exports = DBmeta;