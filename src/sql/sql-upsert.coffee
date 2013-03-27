_ = require("underscore")
F = require('functoids/src')
{ SqlStatement } = sql = require('./index')
shared = require('./shared')

class SqlUpsert extends SqlStatement
    constructor: (table, @values, onColumns...) ->
        super(table)
        F.demandGoodObject(values, 'values')

        onColumns = _.flatten(onColumns)
        F.demandArrayOfGoodStrings(onColumns, 'onColumns')
        @onColumns = onColumns

    output: shared.output
    toSql: (f) -> f.upsert(@)

_.extend(sql, {
    upsert: (t, values, onColumns) -> new SqlUpsert(t, values, onColumns)
    SqlUpsert
})
