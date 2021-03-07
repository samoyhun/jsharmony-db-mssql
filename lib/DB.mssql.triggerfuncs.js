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

exports = module.exports = {
  "set": {
    "params": ["COL","VAL"],
    "sql": [
      "update %%%TABLENAME%%% set %%%COL%%%=%%%VAL%%% where %%%ROWKEY%%%"
    ]
  },
  "setif": {
    "params": ["COND","COL","VAL"],
    "sql": [
      "update %%%TABLENAME%%% set %%%COL%%%=%%%VAL%%% where %%%ROWKEY%%% and (%%%COND%%%)"
    ]
  },
  "update": {
    "params": ["COL"],
    "sql": [
      "(((deleted(%%%COL%%%) is null) and (inserted(%%%COL%%%) is not null)) or ((deleted(%%%COL%%%) is not null) and (inserted(%%%COL%%%) is null)) or ((deleted(%%%COL%%%) is not null) and (inserted(%%%COL%%%) is not null) and (deleted(%%%COL%%%)<>inserted(%%%COL%%%))))"
    ]
  },
  "top1": {
    "params": ["SQL"],
    "sql": [
      "top 1 %%%SQL%%%"
    ]
  },
  "null": {
    "params": ["VAL"],
    "sql": [
      "(%%%VAL%%% is null)"
    ]
  },
  "errorif": {
    "params": ["COND","MSG"],
    "exec": [
      "var rslt = 'if('+COND.trim()+') ';",
      "MSG = MSG.trim();",
      "if(MSG && (MSG[0]=='\\'')) MSG = '\\'Application Error - '+MSG.substr(1);",
      "rslt += 'begin raiserror('+MSG+',16,1); if (@@TRANCOUNT > 0) rollback transaction; return; end;';",
      "return rslt;"
    ]
  },
  "inserted": {
    "params": ["COL"],
    "sql": [
      "@inserted_%%%COL%%%"
    ]
  },
  "insert_values": {
    "params": ["..."],
    "sql": [
      "select %%%...%%%"
    ]
  },
  "deleted": {
    "params": ["COL"],
    "sql": [
      "@deleted_%%%COL%%%"
    ]
  },
  "with_insert_identity": {
    "params": ["TABLE","COL","INSERT_STATEMENT","..."],
    "exec": [
      "var identity_var = COL.trim();  while(identity_var in this.vars) identity_var += '_'; this.vars[identity_var] = true;",
      "var rslt = INSERT_STATEMENT.trim() + ';\\n';",
      "rslt += 'declare @'+identity_var+' bigint; \\nselect @'+identity_var+'=scope_identity();\\n';",
      "var EXEC_STATEMENT = [].slice.call(arguments).splice(3,arguments.length-3).join(',');",
      "EXEC_STATEMENT = EXEC_STATEMENT.replace(/@@INSERT_ID/g,'@'+identity_var);",
      "rslt += EXEC_STATEMENT;",
      "return rslt;"
    ]
  },
  "increment_changes": {
    "params": ["NUM"],
    "sql": [
      "set rowcount 1"
    ]
  },
  "return_insert_key": {
    "params": ["TBL","COL","SQLWHERE"],
    "sql": [
      "select %%%COL%%% from %%%TBL%%% where %%%SQLWHERE%%%"
    ]
  },
  "clear_insert_identity": {
    "params": [],
    "sql": "",
  },
  "last_insert_identity": {
    "params": [],
    "sql": "(select scope_identity())"
  }
};