_ = require("underscore")
sql = require('./index')
{ SqlFilteredStatement } = require('./shared')

class SqlUpdate extends SqlFilteredStatement
    constructor: (t, @values, predicate) -> super(t, predicate)

    toSql: (f) -> f.update(@)

_.extend(sql, {
    update: (t, values, predicate) -> new SqlUpdate(t, values, predicate)
    SqlUpdate
})
