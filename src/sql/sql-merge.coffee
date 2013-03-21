_ = require("underscore")
{ SqlStatement } = sql = require('./index')
shared = require('./shared')

class SqlMerge extends SqlStatement
    constructor: (table) -> super(table)
    using: (@rows) -> @
    source: (@rows) -> @
    toSql: (f) -> f.merge(@)

_.extend(sql, {
    merge: (t) -> new SqlMerge(t)
    SqlMerge
})
