_ = require('underscore')
F = require('functoids/src')
sql = require('../sql')
{ SqlJoin, SqlFrom, SqlToken, SqlRawName, SqlFullName } = sql
SqlFormatter = require('./sql-formatter')

schema = {
    createTempTable: (table) ->
        throw new Error('createTempTable: you must provide a table') unless table?

        lines = Array(table.columns.length + 2)
        i = 0

        lines[i++] = "CREATE TABLE #{@delimit(table.name)} ("

        for c in table.columns
            lines[i++] = "  #{@defineColumn(c)},"

        pk = table.pk
        if pk?
            # we ignore the PK name here and let the DB generate a name,
            # because while temp tables exist only in one session, the PK
            # name must still be unique across sessions, and you can imagine how this
            # will blow up in people's faces.
            cluster = if pk.isClustered then 'CLUSTERED ' else ''
            key = "  PRIMARY KEY #{cluster}(#{@doNameList(pk.columns)})"
            lines[i++] = key
        else
            lines[i-1] = lines[i-1].slice(0, -1)


        # FKs in temp tables don't make sense, and are disallowed by most RDMBs afaik,
        # so let's not do them
        lines[i++] = ");\n"

        return lines.join('\n')

    createTableForColumns: (tableName, columns) ->
        F.demandGoodString(tableName, "tableName")
        F.demandGoodArray(columns, "columns")

        lines = (@defineColumn(c) for c in columns)

        lines.unshift("DECLARE #{tableName} TABLE (")
        lines.push(")")

        return lines.join("\n")

    defineColumn: (c) ->
        throw new Error('defineColumn: you must provide a column') unless c?

        type = c.dbDataType
        if c.maxLength?
            type += "(#{c.maxLength})"

        nullable = if c.isNullable then "NULL" else "NOT NULL"
        
        return "#{@delimit(c.name)} #{type} #{nullable}"

    nameTempTable: (baseName) ->
        F.demandGoodString(baseName, "baseName")
        return '#' + baseName

    nameTableVariable: (baseName) ->
        F.demandGoodString(baseName, "baseName")
        return '@' + baseName
}

_.extend(SqlFormatter.prototype, schema)
