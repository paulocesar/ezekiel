_ = require("more-underscore/src")
{ SqlToken, SqlRawName, SqlFilteredStatement } = sql = require('./index')

class SqlUpdate extends SqlFilteredStatement
    constructor: (t, @values, predicate) -> super(t, predicate)

    toSql: (f) ->
        return f.update(@)

_.extend(sql, {
    update: (t, values, predicate) -> new SqlUpdate(t, values, predicate)
    SqlUpdate
})

module.exports = SqlUpdate
