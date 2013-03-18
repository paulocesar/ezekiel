_ = require('more-underscore/src')
sql = require('../sql')
{ SqlJoin, SqlFrom, SqlToken, SqlRawName, SqlFullName } = sql
SqlFormatter = require('./sql-formatter')

schema = {
    createTempTable: (table) ->

    nameTempTable: (baseName) ->
        throw new Error('nameTempTable: you must provide a baseName') unless baseName?
        return '#' + baseName
}

_.extend(SqlFormatter.prototype, schema)
