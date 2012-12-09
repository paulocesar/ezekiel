_ = require("more-underscore/src")
{ SqlPredicate, SqlRawName, SqlStatement } = sql = require('./index')

class SqlInsert extends SqlStatement
    constructor: (table, @values) -> super(table)

    toSql: (f) ->
        return f.insert(@)

_.extend(sql, {
    insert: (t, values) -> new SqlInsert(t, values)

    SqlInsert: SqlInsert
})

module.exports = SqlInsert
