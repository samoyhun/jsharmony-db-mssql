# ==================
# jsharmony-db-mssql
# ==================

jsHarmony Database Connector for SQL Server

## Installation

npm install jsharmony-db-mssql --save

## Usage

```javascript
var JSHmssql = require('jsharmony-db-mssql');
var JSHdb = require('jsharmony-db');
global.dbconfig = { _driver: new JSHmssql(), server: "server.domain.com", database: "DBNAME", user: "DBUSER", password: "DBPASS" };
var db = new JSHdb();
db.Recordset('','select * from C where C_ID >= @C_ID',[JSHdb.types.BigInt],{'C_ID': 10},function(err,rslt){
  console.log(rslt);
  done();
});
```

This library uses the NPM mssql library.  Use any of the connection settings available in that library.

## Release History

* 1.0.0 Initial release