_ = require('more-underscore/src')

sql = {
    verbatim: (s) -> new SqlVerbatim(s)
    predicate: (p...) -> new SqlPredicate(p)
    literal: (o) -> new SqlLiteral(o)
    isLiteral: (o) -> o instanceof SqlLiteral || !(o instanceof SqlToken)

    call: (name, args...) -> new FunctionCall(name, args)

    name: (n...) ->
        return n[0] if n[0] instanceof SqlToken
        return new SqlFullName(n[0]) if _.isArray(n[0])
        return new SqlFullName(n) if n.length > 1
        return new SqlRawName(n[0])

    expr: (e) -> new SqlExpression(e)
    and: (terms...) -> new SqlAnd(_.map(terms, SqlPredicate.wrap))
    or: (terms...) -> new SqlOr(_.map(terms, SqlPredicate.wrap))

    star: (table) -> new SqlStar(table)
}

class SqlToken
    cascade: (fn) ->
        return if fn(@) == false

        children = @getChildren()
        return unless children

        for c in children
            if (c instanceof SqlToken)
                c.cascade(fn)
            else
                fn(c)

    getChildren: (fn) -> null

    toSql: () -> ''

class SqlVerbatim extends SqlToken
    constructor: (@s) ->
    toSql: -> @s

class SqlExpression extends SqlVerbatim
    constructor: (@s) ->

class SqlLiteral extends SqlToken
    constructor: (@l) ->
    toSql: (f) -> f.literal(@l)

class SqlStar extends SqlToken
    constructor: (@table) ->
    toSql: (f) -> f.star(@)

class SqlRawName extends SqlToken
    constructor: (@name) ->
    toSql: (f) -> f.rawName(@)

class SqlFullName extends SqlToken
    constructor: (@parts) ->
    toSql: (f) -> f.fullName(@)
    tip: () -> _.last(@parts)
    prefix: () -> @parts[@parts.length - 2] ? null
    setDefaultPrefix: (prefix) ->
        @parts.unshift(prefix) unless @prefix()?
        return @

class SqlParens extends SqlToken
    constructor: (@contents) ->
    getChildren: -> [@contents]
    toSql: (f) -> f.parens(@contents)

class FunctionCall extends SqlToken
    constructor: (@name, @args) ->
    toSql: (f) -> f.functionCall(@)

class BinaryOp extends SqlToken
    @push: (left, right, ops = []) ->
        # where( field: sql.isNull }
        if _.isFunction(right)
            right = right()

        # where( field: [10, 20, 30] )
        if _.isArray(right)
            newbie = sql.in(left, right)
        # where( field: { '>': 10, '<>': 15 } )
        else if _.isObject(right) && !(right instanceof SqlToken)
            for op, operand of right
                ops.push(new BinaryOp(left, op, operand))
            return
        # where( field: sql.startsWith('Radioh') )
        else if right instanceof BinaryOp
            newbie = new BinaryOp(left, right.op, right.right)
        # where( field: sql.isNull() )
        else if right instanceof NaryOp
            newbie = new NaryOp(right.op, right.atoms.concat(left))
        # where( field: 10 )
        else
            newbie = sql.equals(left, right)

        ops.push(newbie)

    constructor: (@left, @op, @right) ->
    getChildren: -> [@left, @right]
    toSql: (f) -> f.binaryOp(@left, @op, @right)

class NaryOp extends SqlToken
    constructor: (@op, @atoms) ->
    toSql: (f) -> f.naryOp(@op, @atoms)

class SqlPredicate extends SqlToken
    @wrap: (term) ->
        if term instanceof SqlToken
            return term

        if _.isString(term)
            return new SqlParens(new SqlVerbatim(term))

        pieces = []

        if _.isArray(term)
            BinaryOp.push(term[0], term[1], pieces)
        else if _.isObject(term)
            for k, v of term
                BinaryOp.push(k, v, pieces)

        if (pieces.length > 0)
            return if pieces.length == 1 then pieces[0] else new SqlAnd(pieces)

        throw new Error("Unsupported predicate term: " + term.toString())

    @addOrCreate: (predicate, terms) ->
        if predicate?
            predicate.and(terms...)
        else
            predicate = new SqlPredicate(terms)

        return predicate

    constructor: (terms) ->
        if (terms.length? > 1)
            @expr = sql.and(terms...)
        else
            @expr = SqlPredicate.wrap(_.firstOrSelf(terms))

    append: (terms, connector) ->
        if !(@expr instanceof connector)
            @expr = new connector([@expr])

        for t in terms
            @expr.terms.push(SqlPredicate.wrap(t))

        return @

    and: (terms...) -> @append(terms, SqlAnd)
    or: (terms...) -> @append(terms, SqlOr)

    getChildren: -> [@expr]
    toSql: (f) -> @expr.toSql(f)

class SqlBooleanOp extends SqlToken
    constructor: (@terms) ->
    getChildren: () -> @terms

class SqlAnd extends SqlBooleanOp
    toSql: (formatter) -> formatter.and(@terms)

class SqlOr extends SqlBooleanOp
    toSql: (formatter) -> formatter.or(@terms)

class SqlStatement extends SqlToken
    constructor: (table) ->
        unless table?
            throw new Error('SqlStatement constructor: you must provide a table')

        @targetTable = sql.name(table)

module.exports = sql

_.extend(sql, {
    SqlToken
    SqlExpression
    SqlRawName
    SqlFullName
    SqlPredicate
    SqlAnd
    SqlOr
    SqlStatement
    SqlLiteral
    BinaryOp
    NaryOp
    FunctionCall
})

require('./sql-select')
require('./sql-insert')
require('./sql-delete')
require('./sql-update')
require('./sql-operators')
require('./sql-functions')
require('./sql-merge')
