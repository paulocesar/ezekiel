_ = require("more-underscore/src")
{ SqlPredicate, SqlRawName, SqlStatement, SqlToken } = sql = require('./index')

class SqlAliasedExpression extends SqlToken
    constructor: (a) ->
        @atom = _.firstOrSelf(a)
        @alias = _.secondOrNull(a)

        # This is only used by formatters, users don't have to worry about it
        @_model = null
        @_token = null

class SqlFrom extends SqlAliasedExpression
    toSql: (f) -> f.from(@)

class SqlJoin extends SqlAliasedExpression
    constructor: (a, predicate) ->
        super(a)
        @predicate = if predicate? then new SqlPredicate(predicate) else null

    toSql: (f) -> f.join(@)

class SqlSelect extends SqlStatement
    constructor: (tableList...) ->
        @columns = []
        @tables = []
        @joins = []
        @orderings = []
        @groupings = []

        (@from(t) for t in tableList)

    addFrom: (table, a) -> a.push(table)

    select: (columns...) ->
        @columns = columns
        return @

    distinct: () ->
        @quantifier = "DISTINCT"
        return @

    all: () ->
        @quantifier = "ALL"
        return @

    skip: (n) ->
        @cntSkip = n
        return @

    take: (n) ->
        @cntTake = n
        return @

    from: (table) ->
        @tables.push(table)
        return @

    join: (j, clause) ->
        join = if clause? then sql.join(j, clause) else j
        @joins.push(join)
        return @

    where: (terms...) ->
        @whereClause = @addTerms(@whereClause, terms)
        return @

    groupBy: (atoms...) ->
        @groupings.push(atoms...)
        return @

    having: (terms...) ->
        @havingClause = @addTerms(@havingClause, terms)
        return @

    addTerms: (predicate, terms) ->
        @lastPredicate = SqlPredicate.addOrCreate(predicate, terms)
        return @lastPredicate

    orderBy: (orderings...) ->
        @orderings.push(orderings...)
        return @

    and: (terms...) ->
        return @where(terms...) unless @lastPredicate

        @lastPredicate.and(terms...)
        return @

    or: (terms...) ->
        return @where(sql.or(terms...)) unless @lastPredicate
        @lastPredicate.or(terms...)
        return @

    toSql: (f) ->
        return f.select(@)

p = SqlSelect.prototype
p.limit = p.top = p.take

_.extend(sql, {
    select: (t...) ->
        s = new SqlSelect()
        s.select(t...)
    
    from: (t) -> new SqlSelect(t)
    join: (table, clause) -> new SqlJoin(table, clause)

    SqlSelect
    SqlJoin
    SqlFrom
})

module.exports = SqlSelect
