_ = require("more-underscore/src")
{ SqlPredicate, SqlRawName, SqlFilteredStatement } = sql = require('./index')

class SqlDelete extends SqlFilteredStatement
    toSql: (f) ->
        return f.delete(@)

_.extend(sql, {
    delete: (t, predicate) -> new SqlDelete(t, predicate)
    SqlDelete
})

module.exports = SqlDelete
