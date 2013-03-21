_ = require("underscore")
{ SqlStatement } = sql = require('./index')
shared = require('./shared')


class SqlInsert extends SqlStatement
    constructor: (table, @values) -> super(table)
    toSql: (f) -> f.insert(@)
    output: shared.output

_.extend(sql, {
    insert: (t, values) -> new SqlInsert(t, values)

    SqlInsert
})
