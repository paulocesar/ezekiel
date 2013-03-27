_ = require("underscore")
F = require('functoids/src')
{ SqlStatement } = sql = require('./index')
shared = require('./shared')


class SqlInsert extends SqlStatement
    constructor: (table, @values) ->
        super(table)
        F.demandNonEmptyObject(values, 'values')

    toSql: (f) -> f.insert(@)
    output: shared.output

_.extend(sql, {
    insert: (t, values) -> new SqlInsert(t, values)
    SqlInsert
})
