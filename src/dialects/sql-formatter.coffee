_ = require('more-underscore/src')
sql = require('../sql')
{ SqlJoin, SqlFrom, SqlToken, SqlRawName, SqlFullName } = sql

rgxParseName = ///
    ( [^.]+ )   # anything that's not a .
    \.?         # optional . at the end
///g

rgxStar = /^(?:(.+)\.)?\*$/
rgxExpression = /[()\+\*\-/]/
rgxPatternMetaChars = /[%_[]/g

operatorAliases = {
    contains: 'LIKE'
    startsWith: 'LIKE'
    endsWith: 'LIKE'
    equals: '='
    '!=': '<>'
}

class SqlFormatter
    constructor: (@schema) ->
        @sources = []

    f: (v) ->
        return v.toSql(@) if v instanceof SqlToken
        return @literal(v)

    literal: (l) ->
        unless l?
            return 'NULL'

        if (_.isString(l))
            return "'" + l.replace("'","''") + "'"

        if (_.isArray(l))
            return _.map(l, (i) => @literal(i)).join(', ')

        return l.toString()

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
            return @_doStar(s.table)
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
        atom = _.firstOrSelf(c)
        alias = _.secondOrNull(c)
        t = @tokenizeColumn(atom)
        return @doAliasedToken(t, alias)

    doOutputColumn: (output, defaultPrefix) ->
        atom = _.firstOrSelf(output)
        t = @tokenizeColumn(atom, @findOutputColumnSchema)

        if atom != t && t instanceof SqlFullName
            t.setDefaultPrefix(defaultPrefix)

        alias = _.secondOrNull(output)
        return @doAliasedToken(t, alias)

    # A column might be an actual table column, but it could also be an expression,
    # SQL literal, subquery, etc.
    _doColumnAtom: (atom) ->
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
                pieces = ("#{@_doColumnAtom(a)} IS NULL" for a in atoms)
            when 'isntNull', 'isNotNull'
                pieces = ("#{@_doColumnAtom(a)} IS NOT NULL" for a in atoms)
            when 'isGood'
                pieces = []
                for a in atoms
                    s = @_doColumnAtom(a)
                    pieces.push("#{s} IS NOT NULL AND LEN(RTRIM(LTRIM(#{s}))) > 0")

        return pieces.join(' AND ')

    binaryOp: (left, op, right) ->
        l = @_doColumnAtom(left)
        
        sqlOp = operatorAliases[op] ? op.toUpperCase()
        
        switch op
            when 'in' then r = "(#{@f(right)})"
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
            p = _.undelimit(@f(rhs), "''")
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

        @doList(call.args, @_doColumnAtom, ', ', prologue, epilogue)


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

    tokenizeColumn: (atom, fnFindSchema = @findColumnSchema) ->
        @tokenizeAtom(atom, fnFindSchema, @parseNameOrExpression)

    tokenizeRhs: (atom) -> @tokenizeAtom(atom, @findColumnSchema)

    parseNameOrExpression: (s) ->
        if rgxStar.test(s)
            tableName = s.match(rgxStar)[1]
            if tableName?
                table = @fullNameFromString(tableName)
                table.schema = @findTableSchema(table)
            
            return sql.star(table)

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

    findOutputColumnSchema: (token) ->
        unless @targetSchema? && token instanceof SqlFullName
            return

        if token.prefix() in ['inserted', 'deleted', null]
            token._schema = @targetSchema.columnsByProperty[token.tip()]

    _addSources: (a, type) ->
        for o in a
            s = if o instanceof type then o else new type(o)

            token = @tokenizeAtom(s.atom, @findTableSchema, @parseNameOrExpression)
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

    groupBy: (c) -> @doList(c.groupings, @_doColumnAtom, ', ', ' GROUP BY ')

    orderBy: (c) -> @doList(c.orderings, @ordering, ', ', ' ORDER BY ')

    ordering: (o) ->
        s = @_doColumnAtom(_.firstOrSelf(o))
        dir = if _.secondOrNull(o) == 'DESC' then 'DESC' else 'ASC'

        "#{s} #{dir}"

    _doTargetTable: (name) ->
        token = @tokenizeAtom(name, @findTableSchema)
        schema = token._schema

        if schema?
            @sources.push(_schema: schema, _token: token)
            @targetSchema = schema

        return @_doToken(token)

    insert: (stmt) ->
        ret = "INSERT #{@_doTargetTable(stmt.targetTable)}"
        names = []
        values = []
        for k, v of stmt.values
            c = @_doInsertUpdateColumn(k)
            continue unless c
            names.push(c)
            values.push(@f(v))

        ret += " (#{names.join(', ')}) "
        if stmt.outputColumns?
            outputs = (@doOutputColumn(o, 'inserted') for o in [].concat(stmt.outputColumns))
            ret += "OUTPUT #{outputs.join(', ')} "

        ret += "VALUES (#{values.join(', ')})"

    update: (stmt) ->
        ret = "UPDATE #{@_doTargetTable(stmt.targetTable)} SET "

        values = []
        for k, v of stmt.values
            c = @_doInsertUpdateColumn(k)
            continue unless c
            values.push("#{c} = #{@f(v)}")

        ret += values.join(', ')

        ret += @where(stmt)
        return ret

    _doInsertUpdateColumn: (atom) ->
        alias = _.secon
        token = @tokenizeColumn(atom)
        schema = token._schema
        # MUST: change this behavior. This seemed useful at one point, but really it masks
        # a nasty error if the caller misspells a column name
        if @targetSchema? && (not schema? || schema.isReadOnly)
            return false

        return @_doToken(token)

    delete: (stmt) ->
        ret = "DELETE FROM #{@_doTargetTable(stmt.targetTable)}"
        ret += @where(stmt)
        return ret

p = SqlFormatter.prototype
p.format = p.f

module.exports = SqlFormatter
