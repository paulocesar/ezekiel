_ = require("underscore")
sql = require('./index')
{ SqlFilteredStatement } = require('./shared')

class SqlDelete extends SqlFilteredStatement
    toSql: (f) -> f.delete(@)

_.extend(sql, {
    delete: (t, predicate) -> new SqlDelete(t, predicate)
    SqlDelete
})
