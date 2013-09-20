_ = require('underscore')
sql = require('../../sql')
{ SqlJoin, SqlFrom, SqlToken, SqlRawName, SqlFullName } = sql

rgxParseName = ///
    ( [^.]+ )   # anything that's not a .
    \.?         # optional . at the end
///g

rgxStar = /^(?:(.+)\.)?\*$/
rgxExpression = /[()\+\*\-/]/

class MysqlFormatter
    constructor: (@schema) ->
        @sources = []

    fullNameFromString: (s) ->
        parts = []
        while (match = rgxParseName.exec(s))
            parts.push(match[1])

        return new SqlFullName(parts)

    fullName: (n) ->
        return n.parts
            .map((s) -> "`#{s}`")
            .join('.')

    rawName: (n) ->
        return n.name
            .split('.')
            .map( (s) -> "`#{s}`" )
            .join('.')

    parseNameOrExpression: (s) ->
        if rgxStar.test(s)
            tableName = s.match(rgxStar)[1]
            return sql.star(tableName)

        if rgxExpression.test(s)
            return sql.expr(s)

        return @fullNameFromString(s)

    format: (v) ->
        return v.toSql(@) if v instanceof SqlToken
        return @literal(v)

    literal: (v) ->
        unless v?
            return 'NULL'

        if _.isString(v)
            return "'" + v.replace(/'/g, "''") + "'"

        if _.isArray(v)
            return _.map(v, (i) => @literal(i)).join(', ')

        if _.isDate(v)
            return @date(v)

        if _.isBoolean(v)
            return if v then '1' else '0'

        return v.toString()


    _numberAppendZero: (n,size) ->
        n = n.toString()
        while n.length < size
            n = '0' + n
        return n

    date: (dt) ->
        y = dt.getFullYear()
        m = @_numberAppendZero( dt.getMonth()+1, 2)
        d = @_numberAppendZero( dt.getDate(), 2)

        h = @_numberAppendZero( dt.getHours(), 2)
        min = @_numberAppendZero( dt.getMinutes(), 2)
        s = @_numberAppendZero( dt.getSeconds(), 2)
        mis = @_numberAppendZero( dt.getMilliseconds(), 3)

        return "'#{y}-#{m}-#{d} #{h}:#{min}:#{s}.#{mis}'"


    _appendRawNames: (q,separator) ->
        str = ''
        firstTime = true
        for col of q
            str += separator if !firstTime
            firstTime = false
            str += @rawName({name: q[col]})
        str

    _termToSql: (t) ->
        if t.op == 'equals'
            separator = ' = '
            separator = ' LIKE ' if typeof t.right != 'number'
            left = @rawName({name: t.left})
            right = t.right
            right = "'"+t.right+"'" if typeof t.right != 'number'
            console.log(left + separator + right)
            return left + separator + right

    _appendAndTerms: (terms) ->
        a = []
        for t of terms
            a.push(@_termToSql(terms[t]))
        '(' + a.join(' AND ') + ')'



    select: (q) ->
        query = "SELECT "

        if q.columns
            query += @_appendRawNames(q.columns,', ')
        
        if q.tables
            query += " FROM "   
            query += @_appendRawNames(q.tables,', ')

        if q.whereClause
            query += " WHERE "
            query += @_appendAndTerms(q.whereClause.expr.terms)

        return query;
        

module.exports = MysqlFormatter














