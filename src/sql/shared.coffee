_ = require("more-underscore/src")
{ SqlPredicate, SqlStatement } = sql = require('./index')

class SqlFilteredStatement extends SqlStatement
    constructor: (table, predicate) ->
        super(table)
        @where(predicate) if predicate?

    where: (terms...) ->
        @whereClause = SqlPredicate.addOrCreate(@whereClause, terms)
        return @

    and: (terms...) ->
        return @where(terms...) unless @whereClause

        @whereClause.and(terms...)
        return @

    or: (terms...) ->
        return @where(sql.or(terms...)) unless @whereClause

        @whereClause.or(terms...)
        return @

output = (columns) ->
    unless columns?
        e = "You must specify either a single column (e.g., 'id', 'userId') or an array of " +
            "columns (e.g., ['id', 'timestamp', 'lastName']) as output"

        throw new Error(e)

    @outputColumns = _.unwrapArgs(arguments)
    return @

module.exports = {
    SqlFilteredStatement,
    output
}
