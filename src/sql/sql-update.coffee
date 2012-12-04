_ = require("more-underscore/src")
{ SqlToken, SqlRawName, SqlFilteredStatement } = sql = require('./index')

class SqlUpdateExpression extends SqlToken
    constructor: (column, @value) ->
        @column = sql.name(column)

    toSql: (f) -> f.updateExpr(@)

class SqlUpdate extends SqlFilteredStatement
    init: () ->
        @exprs = []

    toSql: (f) ->
        return f.update(@)

    set: (o) ->
        for k, v of o
            @exprs.push(sql.updateExpr(k, v))
        return @


_.extend(sql, {
    update: (t) -> new SqlUpdate(t)
    updateExpr: (column, value) -> new SqlUpdateExpression(column, value)

    SqlUpdate: SqlUpdate
})

module.exports = SqlUpdate
