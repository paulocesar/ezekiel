_ = require('more-underscore/src')
sql = require('../sql')
{ SqlJoin, SqlFrom, SqlToken, SqlRawName, SqlFullName } = sql
SqlFormatter = require('./sql-formatter')

bulk = {
    merge: (merge) ->
        unless merge?.targetTable?
            throw new Error('merge: you must provide a targetTable')

        rows = merge?.rows
        unless rows? && _.isArray(rows) && !_.isEmpty(rows)
            throw new Error('merge: you must provide a non-empty array of rows to be merged')

        target = @tokenizeTable(merge.targetTable)
        @table = target._schema
        unless @table?
            e = "merge: could not find schema for table #{target}."
            throw new Error(e)

        o = @table.classifyRowsForMerging(rows)
        @idx = 0
        size = o.cntRows + 16
        lines = Array(size)
        cleanup = []
        @_addBulkInserts(o.inserts, lines, cleanup)
        for keyName, rows of o.updatesByKey
            @_addBulkUpdates(keyName, rows, lines, cleanup)

        for keyName, rows of o.mergesByKey
            @_addBulkMerges(keyName, rows, lines, cleanup)

        for c in cleanup
            lines[@idx++] = c

        lines.length = @idx
        return lines.join('\n')

    _addBulkInserts: (rows, lines, cleanup) ->
        return if _.isEmpty(rows)
        # MUST: implement

    _addBulkUpdates: (keyName, rows, lines, cleanup) ->
        return if _.isEmpty(rows)
        # MUST: implement

    _addBulkMerges: (keyName, rows, lines, cleanup) ->
        return if _.isEmpty(rows)
        idxCreateTempTable = @i++
        key = @table.db.constraintsByName[keyName]

        cntValuesByColumn = {}
        columns = []

        for c in @table.columns
            if c.isReadOnly && !key.contains(c)
                continue

            columns.push(c)
            cntValuesByColumn[c.name] = 0

        tempName = @nameTempTable('BulkMerge')

        for r in rows
           lines[@idx++] = @_doInsert(tempName, key, r, columns, cntValuesByColumn)

        tempTableColumns = []
        for c in columns
            cntValues = cntValuesByColumn[c.name]
            continue if cntValues == 0

            nullable = cntValues < rows.length
            tempColumn = {
                name: c.name, isNullable: nullable, dbDataType: c.dbDataType,
                maxLength: c.maxLength
            }
            tempTableColumns.push(tempColumn)





    _doInsert: (tempName, key, row, columns, cntValuesByColumn) ->
        names = []
        values = []
        for c in @table.columns
            v = row[c.property]
            continue unless v?

            names.push(@delimit(c.name))
            values.push(@f(v))
            cntValuesByColumn[c.name]++

        return "INSERT #{tempName} (#{names.join(',')}) VALUES (#{values.join(',')})"
}

_.extend(SqlFormatter.prototype, bulk)
