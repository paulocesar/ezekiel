_ = require('more-underscore/src')
sql = require('../sql')
{ SqlJoin, SqlFrom, SqlToken, SqlRawName, SqlFullName } = sql
SqlFormatter = require('./sql-formatter')
schemer = require('../schema')

bulk = {
    merge: (merge) ->
        unless merge?.targetTable?
            throw new Error('merge: you must provide a targetTable')

        rows = merge.rows
        unless _.isArray(rows) && !_.isEmpty(rows)
            throw new Error('merge: you must provide a non-empty array of rows to be merged')

        target = @tokenizeTable(merge.targetTable)
        @table = target._schema
        unless @table?
            e = "merge: could not find schema for table #{target}."
            throw new Error(e)

        o = @table.classifyRowsForMerging(rows)
        @idx = 0
        size = o.cntRows + 16
        @lines = Array(size)
        @cleanup = []
        @_addBulkInserts(o.inserts)
        for keyName, rows of o.updatesByKey
            @_addBulkUpdates(keyName, rows)

        for keyName, rows of o.mergesByKey
            @_addBulkMerges(keyName, rows)

        for c in @cleanup
            @lines[@idx++] = c

        @lines.length = @idx
        return @lines.join('\n')

    _addBulkInserts: (rows) ->
        return if _.isEmpty(rows)
        # MUST: implement

    _addBulkUpdates: (keyName, rows) ->
        return if _.isEmpty(rows)
        # MUST: implement

    _addBulkMerges: (keyName, rows) ->
        return if _.isEmpty(rows)
        idxCreateTempTable = @idx++
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
            cntValues = cntValuesByColumn[c.name]
            continue if cntValues == 0

            nullable = cntValues < rows.length
            tempColumn = {
                name: c.name, property: c.property, isNullable: nullable,
                dbDataType: c.dbDataType, maxLength: c.maxLength
            }
            tempTableColumns.push(tempColumn)

        tempTableName = @nameTempTable('BulkMerge')

        tempTable = schemer.table(name: tempTableName)
            .addColumns(tempTableColumns)
            .primaryKey(columns: _.pluck(key.columns, 'name'), isClustered: true)

        @lines[@idx++] = @createTempTable(tempTable)
        @lines[@idx++] = @_firstInsertLine(tempTable)

        for r in rows
           @lines[@idx++] = @_insertValues(tempTable, r)

        n = @idx-1
        @lines[n] = @lines[n].slice(0, -1) + ';'
        return @lines.join('\n')

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
