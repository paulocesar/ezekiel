_ = require('underscore')
F = require('functoids/src')
sql = require('../sql')
schemer = require('../schema')
{ SqlJoin, SqlFrom, SqlToken, SqlRawName, SqlFullName } = sql

rgxParseName = ///
    ( [^.]+ )   # anything that's not a .
    \.?         # optional . at the end
///g

rgxSingleQuote = /'/g

rgxStar = /^(?:(.+)\.)?\*$/
rgxExpression = /[()\+\*\-/]/
rgxPatternMetaChars = /[%_[]/g

operatorAliases = {
    contains: 'LIKE'
    startsWith: 'LIKE'
    endsWith: 'LIKE'
    equals: '='
    '!=': '<>'
    notIn: 'NOT IN'
}

class SqlFormatter
    constructor: (@schema) ->
        @sources = []

    f: (v) ->
        return v.toSql(@) if v instanceof SqlToken
        return @literal(v)

    literal: (v) ->
        unless v?
            return 'NULL'

        if _.isString(v)
            return "'" + v.replace(rgxSingleQuote, "''") + "'"

        if _.isArray(v)
            return _.map(v, (i) => @literal(i)).join(', ')

        if _.isDate(v)
            return @date(v)

        if _.isBoolean(v)
            return if v then '1' else '0'

        return v.toString()

    date: (d) ->
        F.demandDate(d)

        pad = (n) -> F.padLeft(n, 2, '0')

        y = F.padLeft(d.getFullYear(), 4, '0')
        m = pad(d.getMonth() + 1)
        day = pad(d.getDate())
        h = pad(d.getHours())
        mm = pad(d.getMinutes())
        s = pad(d.getSeconds())
        ms = F.padLeft(d.getMilliseconds(), 3, '0')

        return "'#{y}-#{m}-#{day} #{h}:#{mm}:#{s}.#{ms}'"

    parens: (contents) -> "(#{contents.toSql(@)})"

    and: (terms) ->
        t = _.map(terms, @f, @)
        return "(#{t.join(" AND " )})"

    or: (terms) ->
        t = _.map(terms, @f, @)
        return "(#{t.join(" OR " )})"

    rawName: (n) -> @fullNameFromString(n.name).toSql(@)

    fullName: (m) -> @joinNameParts(m.parts)

    joinNameParts: (names) -> _.map(names, (p) -> "[#{p}]").join(".")

    fullNameFromString: (s) ->
        parts = []
        while (match = rgxParseName.exec(s))
            parts.push(match[1])

        return new SqlFullName(parts)

    delimit: (s) -> "[#{s}]"

    star: (s) ->
        if (s.table?)
            token = @tokenizeTable(s.table)
            return @_doStar(token)
        else
            if _.every(@sources, (s) -> not s._schema?)
                return '*'

            parts = for s in @sources
                @_doStar(s._token)

            return parts.join(', ')

    _doStar: (tableToken) ->
        schema = tableToken._schema
        if schema?
            # special case to avoid needless table prefix
            if @sources.length == 1 && @sources[0]._schema == schema
                prefix = ''
            else
                prefix = @f(tableToken) + '.'

            parts = for c in schema.columns
                "#{prefix}#{@delimit(c.name)} as #{@delimit(c.property)}"

            return parts.join(', ')
        else
            return @f(tableToken) + '.*'

    doAliasedColumn: (c) ->
        atom = F.firstOrSelf(c)
        alias = F.secondOrNull(c)
        t = @tokenizeColumn(atom)
        return @doAliasedToken(t, alias)

    doNameList: (names, separator = ', ') ->
        r = Array(names.length)
        for n, i in names
            r[i] = @delimit(n.name ? n)
        return r.join(separator)

    # A column might be an actual table column, but it could also be an expression,
    # SQL literal, subquery, etc.
    doColumnAtom: (atom) ->
        t = @tokenizeColumn(atom)
        return @_doToken(t)

    _doToken: (token) ->
        schema = token._schema
        if (schema?)
            # MUST: we assume the column is a DB object at this point. We'll need to handle
            # virtual tables, columns, etc. one day
            if (p = token.prefix())
                return @joinNameParts([p, schema.name])
            else
                return @delimit(schema.name)
        else
            return @f(token)

    doAliasedToken: (token, alias) ->
        t = @_doToken(token)
        schema = token._schema
        alias ?= schema?.sqlAlias()

        if not alias? || schema?.name == alias
            return t
        else
            return t + " as " + @delimit(alias)

    naryOp: (op, atoms) ->
        switch op
            when 'isNull'
                pieces = ("#{@doColumnAtom(a)} IS NULL" for a in atoms)
            when 'isntNull', 'isNotNull'
                pieces = ("#{@doColumnAtom(a)} IS NOT NULL" for a in atoms)
            when 'isGood'
                pieces = []
                for a in atoms
                    s = @doColumnAtom(a)
                    pieces.push("#{s} IS NOT NULL AND LEN(RTRIM(LTRIM(#{s}))) > 0")

        return pieces.join(' AND ')

    binaryOp: (left, op, right) ->
        F.demandNotNil(left, "left")
        F.demandNotNil(op, "op")
        F.demandNotNil(right, "right")

        l = @doColumnAtom(left)
        
        sqlOp = operatorAliases[op] ? op.toUpperCase()
        
        switch op
            when 'in', 'notIn' then r = "(#{@f(right)})"
            when 'between' then r = "#{@f(right[0])} AND #{@f(right[1])}"
            when 'contains'
                r = @_doPatternMatch(right, '%', '%')
            when 'startsWith'
                r = @_doPatternMatch(right, '', '%')
            when 'endsWith'
                r = @_doPatternMatch(right, '%')
            else
                rightToken = @tokenizeRhs(right)
                r = @_doToken(rightToken)

        return "#{l} #{sqlOp} #{r}"

    _doPatternMatch: (rhs, prologue = '', epilogue = '') ->
        t = @tokenizeRhs(rhs)

        if sql.isLiteral(t)
            p = F.undelimit(@f(rhs), "''")
            p = @_escapePatternMetaChars(p)
            return "'#{prologue}#{p}#{epilogue}'"
        else
            p = @_doToken(t)
            if prologue
                p = "'#{prologue}' + #{p}"
            if epilogue
                p = "#{p} + '#{epilogue}'"
            return p

    _escapePatternMetaChars: (s) -> s.replace(rgxPatternMetaChars, '[$&]')

    functionCall: (call) ->
        switch call.name
            when 'now' then return 'GETDATE()'
            when 'utcNow' then return 'GETUTCDATE()'
            when 'trim'
                prologue = "RTRIM(LTRIM("
                epilogue = "))"
            else
                prologue = "#{call.name.toUpperCase()}("
                epilogue = ")"

        if call.args.length == 0
            return prologue + epilogue

        @doList(call.args, @doColumnAtom, ', ', prologue, epilogue)


    doList: (collection, fn = @f, separator = ', ', prologue = '', epilogue = '') ->
        return '' unless collection?.length > 0
        results = (fn.call(@, i) for i in collection)
        return prologue + results.join(separator) + epilogue

    _doTables: -> @doList((s for s in @sources when s instanceof SqlFrom))

    _doJoins: -> @doList((s for s in @sources when s instanceof SqlJoin), @f, ' ')

    from: (f) ->
        token = f._token
        schema = f._schema
        return @doAliasedToken(f._token, f.alias)

    join: (j) ->
        str = " #{j.type} JOIN #{@from(j)} ON #{@f(j.predicate)}"

    # This method tokenizes *all* SQL atoms that users pass to us as columns, table names, WHERE
    # clauses, and so on. It helps to see an example:
    #
    # sql.select('firstName', 'lastName', sql.literal('Random String')
    #   .from('fighters')
    #   .where(country: 'USA', firstName: sql.startsWith('Jon')
    #
    # All of those arguments will end up here in tokenizeAtom(). Users can give us pre-built tokens
    # directly using the sql.* functions, like sql.literal() and sql.startsWith() above. Life is
    # easy when they give us these ready made tokens. All we must do is find a table or column
    # schema for the token, when applicable, but the atom itself is already tokenized.
    #
    # But when they give us a string, we have two choices:
    #
    # 1) Assume it must be a SQL name or expression.  This is how we treat columns and the property
    # name in a where predicate object (the left-hand side, LHS, like country and firstName above).
    # In this case, we try a little parsing to decide what the string is.
    #
    # 2) Assume it must be a string literal, which happens for 'USA' and 'Jon' above (the right-hand
    # side, RHS, in a predicate object). We DO NOT try to be smart or fancy at all here. It's
    # a literal.
    #
    # This makes the query building safe and reliable. Sometimes it feels like these rules could be
    # relaxed. For example:
    #
    # sql.from('fighters').where("LEN(LastName)": '>': 'LEN(FirstName)')
    # 
    # Here LEN(LastName) is in case 1) above and gets parsed correctly as an expression.
    # However, LEN(FirstName) is in case 2) and gets treated as a string literal.  It might seem
    # like a good idea to get fancy and try parsing the RHS of a where predicate, but that way
    # madness lies. A simple query like:
    #
    # sql.from('fighters').where(lastName: queryString)
    #
    # would open users up to huge security holes, random bugs and ill defined behavior as we
    # parsed their string values into arbitrary SQL expressions. It's easy enough for users to ask
    # for an expression, but they must be explicit: sql.expression('LEN(FirstName)')
    #
    # Likewise, relaxing 1) above when we have schema is seductive, as in:
    #
    # sql.from('fighters').select('lastName', 'someRandomString')
    #
    # Thanks to the schema, we know 'someRandomString' is not a column, and we're tempted to treat
    # it as a string literal. I mean, we pretty much KNOW this query will blow up if we submit
    # 'someRandomString' as a column name. So, why not help the user out a little?
    #
    # We opt for exploding, and here's why. 90% of the time the user mispelled a real column and
    # it's better to blow up, since returning the literal will confuse the hell out of them.  10% of
    # the time the user wants a string literal in the result set, in which case they must do
    # sql.literal(s) to get it, which is easy enough. But then there's the real killer: if users
    # give us all sorts of crazy strings without sql.literal(), sometimes these strings WILL match
    # up with column names, in unintended ways, again opening a wide door to calamity.  Imagine the
    # fun exploits as people submit "user.password" into various web inputs trying to strike gold ;)
    #
    # So, there. Simple and explicit take the cake.
    tokenizeAtom: (atom, fnFindSchema, fnParseString) ->
        if atom instanceof SqlRawName
            token = @fullNameFromString(atom.name)
        else if _.isString(atom) && fnParseString?
            token = fnParseString.call(@, atom)
        else token = atom

        if token instanceof SqlFullName && fnFindSchema?
            token._schema = fnFindSchema.call(@, token)

        return token

    tokenizeTable: (atom) -> @tokenizeAtom(atom, @findTableSchema, @parseNameOrExpression)

    tokenizeColumn: (atom, fnFindSchema = @findColumnSchema) ->
        @tokenizeAtom(atom, fnFindSchema, @parseNameOrExpression)

    tokenizeRhs: (atom) -> @tokenizeAtom(atom, @findColumnSchema)

    parseNameOrExpression: (s) ->
        if rgxStar.test(s)
            tableName = s.match(rgxStar)[1]
            return sql.star(tableName)

        if rgxExpression.test(s)
            return sql.expr(s)

        return @fullNameFromString(s)

    findTableSchema: (token) ->
        unless @schema? && token instanceof SqlFullName
            return null

        return @schema.tablesByMany[token.tip()] ? null

    findColumnSchema: (token) ->
        unless @schema? && token instanceof SqlFullName
            return

        table = token.prefix()
        if table?
            return @schema.tablesByMany[table]?.columnsByProperty[token.tip()]

        for t in @sources when t._schema?
            column = t._schema.columnsByProperty[token.tip()]
            if column?
                return column

    _addSources: (a, type) ->
        for o in a
            s = if o instanceof type then o else new type(o)

            token = @tokenizeTable(s.atom)
            s._token = token
            s._schema = token._schema

            @sources.push(s)

    _deductJoinPredicates: ->
        cnt = 1
        while cnt > 0
            cnt = 0
            for j in @sources when j instanceof SqlJoin && j.predicate == null
                cnt++ if @_findFkForJoin(j)

        # SHOULD: throw if there are still unresolved JOINS
        return

    _findFkForJoin: (j) ->
        unless j._schema?
            msg = "Unable to build predicate for #{j} because it is not backed " +
                "by a table schema"
            throw new Error(msg)

        t = j._schema

        viableSources =
            (s for s in @sources when s._schema? && (s instanceof SqlFrom || s.predicate?))

        parents =
            _.filter t.foreignKeys, (fk) =>
                _.some viableSources, (s) =>
                    s._schema == fk.parentTable

        if (parents.length > 0)
            # MAY: get smarter about picking which PK to use
            return @_joinByFk(j, parents[0])

        children =
            _.filter t.incomingFKs, (fk) =>
                _.some viableSources, (s) =>
                    s._schema == fk.table

        if (children.length > 0)
            return @_joinByFk(j, children[0])

        return false

    _joinByFk: (j, fk) ->
        childAlias = fk.table.many
        parentAlias = fk.parentTable.many
        parentKey = fk.parentKey

        terms = (for c, i in fk.columns
            [sql.name(childAlias, c.property), sql.name(parentAlias, parentKey.columns[i].property)]
        )
        j.predicate = sql.and(terms...)

        return true

    select: (q) ->
        @_addSources(q.tables, SqlFrom)
        @_addSources(q.joins, SqlJoin)
        @_deductJoinPredicates()

        ret = "SELECT "
        if (q.cntTake)
            ret += "TOP #{q.cntTake} "

        @columns = if q.columns.length > 0 then q.columns else [sql.star()]
        ret += @doList(@columns, @doAliasedColumn)
        ret += " FROM #{@_doTables()}" if q.tables.length > 0

        ret += @_doJoins()
        ret += @where(q)
        ret += @groupBy(q)
        ret += @orderBy(q)
        return ret

    where: (c) -> if c.whereClause? then " WHERE #{(c.whereClause.toSql(@))}"  else ''

    groupBy: (c) -> @doList(c.groupings, @doColumnAtom, ', ', ' GROUP BY ')

    orderBy: (c) -> @doList(c.orderings, @ordering, ', ', ' ORDER BY ')

    ordering: (o) ->
        s = @doColumnAtom(F.firstOrSelf(o))
        dir = if F.secondOrNull(o) == 'DESC' then 'DESC' else 'ASC'

        "#{s} #{dir}"

    _doTargetTable: (name) ->
        token = @tokenizeTable(name)
        schema = token._schema

        if schema?
            @sources.push(_schema: schema, _token: token)
            @targetSchema = schema

        return @_doToken(token)

    insert: (stmt) ->
        targetTable = @_doTargetTable(stmt.targetTable)

        names = []
        values = []
        @fillNamesAndValues(stmt.values, names, values)

        ret = [
            "INSERT #{targetTable}"
            "(" + names.join(", ") + ")"
        ]

        output = @addOutputColumns(ret, stmt.outputColumns)
        if output?
            ret.unshift(output.table)
            ret.push(output.clause)

        ret.push("VALUES (" + values.join(", ") + ")")
        ret.push(output.select) if output?

        return ret.join(' ')

    update: (stmt) ->
        targetTable = @_doTargetTable(stmt.targetTable)

        names = []
        values = []
        @fillNamesAndValues(stmt.values, names, values)

        ret = [
            "UPDATE #{targetTable} SET "
            ("#{name} = #{values[i]}" for name, i in names).join(", ")
            @where(stmt)
        ]

        return ret.join('')

    upsert: (stmt) ->
        targetTable = @_doTargetTable(stmt.targetTable)
        names = []
        values = []
        @fillNamesAndValues(stmt.values, names, values)
        
        eq = (c) -> "target.#{c} = source.#{c}"
        onColumns = (@doColumnAtom(c) for c in stmt.onColumns)
        onClauses = (eq(c) for c in onColumns).join(" AND ")
        updates = (eq(c) for c in names when !_.contains(onColumns, c)).join(", ")

        ret = [
            "MERGE #{targetTable} AS target"
            "USING (SELECT " + values.join(', ') + ")"
            "AS source (" + names.join(', ') + ")"
            "ON (#{onClauses})"
            "WHEN MATCHED THEN"
            "  UPDATE SET #{updates}"
            "WHEN NOT MATCHED THEN"
            "  INSERT (" + names.join(", ") + ")"
            "  VALUES (" + values.join(", ") + ")"
        ]

        output = @addOutputColumns(ret, stmt.outputColumns)
        if output?
            ret.unshift(';')
            ret.unshift(output.table)
            ret.push(output.clause)

        @addSemicolon(ret)

        ret.push(output.select) if output?

        return ret.join('\n')

    addSemicolon: (a) ->
        F.demandGoodArray(a)
        a[a.length-1] += ";"

    addOutputColumns: (a, outputColumns) ->
        F.demandGoodArray(a, 'a')
        return unless outputColumns?

        unless @targetSchema?
            F.throw("You must have a target table schema to use output columns")
       
        columns = [].concat(outputColumns)
        tokens = []
        for columnAlias in columns
            columnSchema = @targetSchema.columnsByProperty[columnAlias]
            unless columnSchema?
                F.throw("Could not find schema for output column with alias #{columnAlias}."
                    "Each output column must be backed by a column schema.")

            t = sql.name([columnSchema.name])
            t._schema = columnSchema

            tokens.push(t)

        tableName = @nameTableVariable("Outputs")
        tableVariable = @createTableForColumns(tableName, (t._schema for t in tokens))

        outputClause = "OUTPUT " + ("inserted." + @_doToken(t) for t in tokens).join(", ") +
            " INTO #{tableName}"

        selectClause = "SELECT " + (@doAliasedToken(t) for t in tokens).join(", ") +
            " FROM #{tableName}"

        return {table: tableVariable, clause: outputClause, select: selectClause}

    fillNamesAndValues: (data, names, values) ->
        for k, v of data
            c = @_doInsertUpdateColumn(k)
            continue unless c
            names.push(c)
            values.push(@f(v))

        return

    _doInsertUpdateColumn: (atom, skipTrouble = true) ->
        token = @tokenizeColumn(atom)
        schema = token._schema
        # MUST: change this behavior. This seemed useful at one point, but really it masks
        # a nasty error if the caller misspells a column name
        if @targetSchema? && skipTrouble && (not schema? || schema.isReadOnly)
            return false

        return @_doToken(token)

    delete: (stmt) ->
        ret = "DELETE FROM #{@_doTargetTable(stmt.targetTable)}"
        ret += @where(stmt)
        return ret

p = SqlFormatter.prototype
p.format = p.f

module.exports = SqlFormatter

require('./bulk-formatter')
require('./schema-formatter')
