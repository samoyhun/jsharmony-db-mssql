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

describe('Basic',function(){
  it('Select', function (done) {
    //Connect to database and get data
    this.timeout(30000);
    var C_ID = '1';
    var dbconfig = { _driver: new JSHmssql(), server: "server.domain.com", database: "DBNAME", user: "DBUSER", password: "DBPASS" };
    var db = new JSHdb(dbconfig);
    db.Recordset('','select * from C where C_ID=@C_ID',[JSHdb.types.BigInt],{'C_ID': C_ID},function(err,rslt){
      assert((rslt && rslt.length && (rslt[0].C_ID==C_ID)),'Success');
      db.Close(done);
    });
  });
});