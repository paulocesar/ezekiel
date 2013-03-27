_ = require("underscore")
F = require('functoids/src')
sql = require('./index')
{ SqlFilteredStatement } = require('./shared')

class SqlUpdate extends SqlFilteredStatement
    constructor: (table, @values, predicate) ->
        super(table, predicate)
        F.demandNotNil(values, 'values')

    toSql: (f) -> f.update(@)

_.extend(sql, {
    update: (t, values, predicate) -> new SqlUpdate(t, values, predicate)
    SqlUpdate
})
