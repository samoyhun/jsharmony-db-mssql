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

function DBmeta(db){
  this.db = db;
}

DBmeta.prototype.getTables = function(table, options, callback){
  var _this = this;
  options = _.extend({ ignore_jsharmony_schema: true }, options);

  var tables = [];
  var messages = [];
  var sql_param_types = [];
  var sql_params = {};
  var sql = "select \
      schemas.name schema_name, \
      objects.name table_name, \
      extended_properties.value [description], \
      case objects.type when 'U' then 'table' else 'view' end table_type \
      from sys.objects \
      inner join sys.schemas on sys.schemas.schema_id = sys.objects.schema_id \
      left outer join sys.extended_properties on extended_properties.major_id = objects.object_id and extended_properties.minor_id = 0 and extended_properties.name='MS_Description' \
      where TYPE IN ('U','V') \
      ";
  if(table){
    sql += " and sys.tables.name=@table_name and sys.schemas.name=@schema_name";
    sql_param_types = [dbtypes.VarChar(dbtypes.MAX), dbtypes.VarChar(dbtypes.MAX)];
    sql_params = {'schema_name':table.schema||_this.db.getDefaultSchema(),'table_name':table.name};
  }
  sql += " order by schema_name, table_name;";
  this.db.Recordset('',sql,sql_param_types,sql_params,function(err,rslt){
    if(err){ return callback(err); }
    for(var i=0;i<rslt.length;i++){
      var dbtable = rslt[i];
      if(!table){
        if(options.ignore_jsharmony_schema && (dbtable.schema_name == 'jsharmony')) continue;
        if(dbtable.schema_name == _this.db.getDefaultSchema()){
          if(dbtable.table_name == 'dtproperties') continue;
          if(dbtable.table_name == 'sysdiagrams') continue;
        }
      }
      tables.push({
        schema:dbtable.schema_name,
        name:dbtable.table_name,
        description:dbtable.description,
        table_type:dbtable.table_type,
        model_name:(dbtable.schema_name==_this.db.getDefaultSchema()?dbtable.table_name:dbtable.schema_name+'_'+dbtable.table_name)
      });
    }
    return callback(null, messages, tables);
  });
}

DBmeta.prototype.getTableFields = function(tabledef, callback){
  var _this = this;
  var fields = [];
  var messages = [];
  var tableparams = { 'schema_name':null,'table_name':null };
  var defaultSchema = _this.db.getDefaultSchema();
  if(tabledef) tableparams = {'schema_name':tabledef.schema||_this.db.getDefaultSchema(),'table_name':tabledef.name};
  _this.db.Recordset('',"select \
      sys.schemas.name schema_name, \
      sys.tables.name table_name, \
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
      where sys.tables.name=isnull(@table_name,sys.tables.name) and sys.schemas.name=isnull(@schema_name,sys.schemas.name) \
      order by schema_name,table_name,column_id",
      [dbtypes.VarChar(dbtypes.MAX), dbtypes.VarChar(dbtypes.MAX)],
      tableparams,
      function(err,rslt){
    if(err){ return callback(err); }

    //Convert to jsHarmony Data Types / Fields
    for(var i=0;i<rslt.length;i++){
      var col = rslt[i];

      if(col.schema_name == defaultSchema){
        if(col.table_name == 'dtproperties') continue;
        if(col.table_name == 'sysdiagrams') continue;
      }

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

      else if(col.type_name=="time"){ field.type = "time"; field.precision = col.scale; }
      else if(col.type_name=="date"){ field.type = "date"; field.precision = col.scale; }
      else if(col.type_name=="datetime"){ field.type = "datetime"; field.precision = col.scale; }
      else if(col.type_name=="smalldatetime"){ field.type = "datetime"; field.precision = col.scale; }
      else if(col.type_name=="datetime2"){ field.type = "datetime"; field.precision = col.scale; }
      else if(col.type_name=="datetimeoffset"){ field.type = "datetimeoffset"; field.precision = col.scale; }

      else if(col.type_name=="bigint"){ field.type = "bigint"; }
      else if(col.type_name=="int"){ field.type = "int"; }
      else if(col.type_name=="smallint"){ field.type = "smallint"; }
      else if(col.type_name=="tinyint"){ field.type = "tinyint"; }
      else if(col.type_name=="bit"){ field.type = "bit"; }

      else if((col.type_name=="decimal")||(col.type_name=="numeric")){ field.type = "decimal"; field.precision = [col.precision, col.scale]; }
      else if(col.type_name=="money"){ field.type = "money"; }
      else if(col.type_name=="smallmoney"){ field.type = "smallmoney"; }
      else if(col.type_name=="float"){ field.type = "float"; field.precision = col.precision; }
      else if(col.type_name=="real"){ field.type = "real"; }

      else if(col.type_name=="binary"){ field.type = "binary"; field.length = col.max_length; }
      else if(col.type_name=="varbinary"){ field.type = "varbinary"; field.length = col.max_length; }
      else if(col.type_name=="image"){ field.type = "image"; }
      else if(col.type_name=="timestamp"){ field.type = "timestamp"; col.readonly = 1; }
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

DBmeta.prototype.getForeignKeys = function(tabledef, callback){
  var _this = this;
  var fields = [];
  var messages = [];
  var tableparams = { 'schema_name':null,'table_name':null };
  if(tabledef) tableparams = {'schema_name':tabledef.schema||'dbo','table_name':tabledef.name};
  _this.db.Recordset('',"select \
                          fk.name id, \
                          cs.name child_schema,ct.name child_table,cc.name child_column, \
                          ps.name parent_schema,pt.name parent_table,pc.name parent_column \
                          from sys.foreign_keys fk \
                          inner join sys.foreign_key_columns fkc on fkc.constraint_object_id = fk.object_id \
                          inner join sys.columns cc on cc.object_id = fkc.parent_object_id and cc.column_id = fkc.parent_column_id \
                          inner join sys.tables ct on ct.object_id = fkc.parent_object_id \
                          inner join sys.schemas cs on cs.schema_id = ct.schema_id \
                          inner join sys.columns pc on pc.object_id = fkc.referenced_object_id and pc.column_id = fkc.referenced_column_id \
                          inner join sys.tables pt on pt.object_id = fkc.referenced_object_id \
                          inner join sys.schemas ps on ps.schema_id = pt.schema_id \
                          where ct.name=isnull(@table_name,ct.name) and cs.name=isnull(@schema_name,cs.name) \
                          order by child_schema,child_table,id,parent_column; \
                        ",
      [dbtypes.VarChar(dbtypes.MAX), dbtypes.VarChar(dbtypes.MAX)],
      tableparams,
      function(err,rslt){
    if(err){ return callback(err); }

    //Convert to jsHarmony Data Types / Fields
    for(var i=0;i<rslt.length;i++){
      var col = rslt[i];
      var field = { 
        from: {
          schema_name: col.child_schema,
          table_name: col.child_table,
          column_name: col.child_column
        },
        to: {
          schema_name: col.parent_schema,
          table_name: col.parent_table,
          column_name: col.parent_column
        }
      };
      fields.push(field);
    }
    return callback(null, messages, fields);
  });
}

exports = module.exports = DBmeta;