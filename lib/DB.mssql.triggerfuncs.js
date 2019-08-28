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
      "update %%%TABLENAME%%% set %%%COL%%%=%%%VAL%%% from inserted where %%%INSERTTABLEKEYJOIN%%%"
    ]
  },
  "setif": {
    "params": ["COND","COL","VAL"],
    "sql": [
      "update %%%TABLENAME%%% set %%%COL%%%=%%%VAL%%% from inserted where %%%INSERTTABLEKEYJOIN%%% and (%%%COND%%%)"
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
    "sql": [
      "if(exists(select * from inserted where (%%%COND%%%))) begin raiserror(%%%MSG%%%,16,1); rollback transaction; return; end;"
    ]
  },
  "inserted": {
    "params": ["COL"],
    "sql": [
      "inserted.%%%COL%%%"
    ]
  },
  "deleted": {
    "params": ["COL"],
    "sql": [
      "(select %%%COL%%% from deleted where %%%INSERTDELETEKEYJOIN%%%)"
    ]
  },
  "top1": {
    "params": ["SQL"],
    "sql": [
      "top 1 %%%SQL%%%"
    ]
  }
};