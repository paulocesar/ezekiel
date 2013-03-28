_ = require('underscore')
F = require('functoids/src')
sql = require('../sql')
{ SqlJoin, SqlFrom, SqlToken, SqlRawName, SqlFullName } = sql
SqlFormatter = require('./sql-formatter')
schemer = require('../schema')

bulk = {
    merge: (merge) ->
        unless merge?.targetTable?
            throw new Error('you must provide a targetTable')

        rows = merge.rows
        F.demandNonEmptyArray(rows, 'merge.rows')

        target = @tokenizeTable(merge.targetTable)
        @table = target._schema
        unless @table?
            e = "merge: could not find schema for table #{target}."
            throw new Error(e)

        o = @table.classifyRowsForMerging(rows)
        @idx = 0
        size = o.cntRows + 16
        @lines = Array(size)
        @_addBulkInserts(o.inserts)

        for keyName, rows of o.updatesByKey
            @_addBulkUpdates(keyName, rows)

        for keyName, rows of o.mergesByKey
            @_addBulkMerges(keyName, rows)

        @lines.length = @idx
        return @lines.join('\n')

    _addBulkInserts: (rows) ->
        return if _.isEmpty(rows)
        F.throw("not implemented")

    _addBulkUpdates: (keyName, rows) ->
        return if _.isEmpty(rows)
        F.throw("not implemented")

    _addBulkMerges: (keyName, rows) ->
        return if _.isEmpty(rows)
        key = @table.db.constraintsByName[keyName]

        cntValuesByColumn = {}
        columns = []

        for c in @table.columns
            if c.isReadOnly && !key.contains(c)
                continue

            columns.push(c)
            cntValuesByColumn[c.property] = 0

        for r in rows
            for c in columns
                cntValuesByColumn[c.property]++ if c.property of r

        tempTableColumns = []
        for c in columns
            cntValues = cntValuesByColumn[c.property]
            continue if cntValues == 0

            nullable = cntValues < rows.length
            tempColumn = {
                name: c.name, property: c.property, isNullable: nullable,
                dbDataType: c.dbDataType, maxLength: c.maxLength
            }
            tempTableColumns.push(tempColumn)

        tempTableName = @nameTempTable('BulkMerge')

        tempTable = schemer.table(name: tempTableName).addColumns(tempTableColumns)
        tempTable.primaryKey(columns: _.pluck(key.columns, 'name'), isClustered: true)

        @addLine(@createTempTable(tempTable))
        @addLine(@_firstInsertLine(tempTable))

        for r in rows
           @addLine(@_insertValues(tempTable, r))

        n = @idx-1
        @lines[n] = @lines[n].slice(0, -1) + ';\n'

        @addLine(@doTableMerge(@table, tempTable))
        @addLine("DROP TABLE " + @delimit(tempTableName) + ";")

    addLine: (l) -> @lines[@idx++] = l

    doTableMerge: (target, source) ->
        t = (c) => "target." + @delimit(c.name)
        s = (c) => "source." + @delimit(c.name)
        eq = (c) =>
            lhs = t(c)
            rhs = if c.isNullable then "COALESCE(#{s(c)}, #{t(c)})" else s(c)
            return lhs + " = " + rhs

        onClauses = (eq(c) for c in source.pk.columns).join(" AND ")
        updates = (eq(c) for c in source.columns when !c.isPartOfKey).join(", ")
        insertValues = (s(c) for c in source.columns).join(", ")

        a = [
            "MERGE #{@delimit(target.name)} as target",
            "USING #{@delimit(source.name)} as source"
            "ON (#{onClauses})"
            "WHEN MATCHED THEN"
            "  UPDATE SET #{updates}"
            "WHEN NOT MATCHED THEN"
            "  INSERT (#{@doNameList(source.columns)})"
            "  VALUES (#{insertValues});"
        ]

        return a.join('\n')

    _firstInsertLine: (table) ->
        columns = _.pluck(table.columns, 'property').join(',')
        "INSERT #{@delimit(table.name)} (#{columns}) VALUES"

    _insertValues: (table, row) ->
        values = Array(table.columns.length)
        for c, i in table.columns
            v = row[c.property]
            values[i] = if v? then @f(v) else 'NULL'

        return "(#{values.join(',')}),"
}

_.extend(SqlFormatter.prototype, bulk)
