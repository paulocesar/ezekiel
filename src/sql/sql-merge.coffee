_ = require("more-underscore/src")
{ SqlStatement } = sql = require('./index')
shared = require('./shared')

class SqlMerge extends SqlStatement
    constructor: (table) -> super(table)
    using: (@values) ->
    source: (@values) ->
    toSql: (f) -> f.merge(@)

_.extend(sql, {
    merge: (t) -> new SqlMerge(t)

    SqlMerge
})
