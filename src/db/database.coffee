_ = require('underscore')

dbObjects = require('./index')
{ DbObject, Table, Column, Key, ForeignKey } = dbObjects
{ SqlToken } = require('../sql')

class Database extends DbObject
    constructor: (@config = {}) ->
        @tables = []
        @tablesByName = {}
        @tablesByAlias = {}
        @constraintsByName = {}
        @name = @config.database

        @adapter = @utils = null

    run: (stmt, callback) ->
        @execute(stmt, { onDone: () -> callback(null) }, callback)

    scalar: (query, callback) ->
        opt = {
            rowShape: 'array'
            onAllRows: (rows) ->
                if (rows.length != 1)
                    e = "Expected query #{query} to return 1 row, " +
                        "but it returned #{rows.length} rows."
                    callback(e)

                callback(null, rows[0][0])
        }
        @execute(query, opt, callback)

    _generateSql: (o) ->
        if (o.stmt instanceof SqlToken)
            o.stmt = @format(o.stmt)

    format: (sql) ->
        f = new @Formatter(@)
        return f.format(sql)

    execute: (query, opt, callback) ->
        if (_.isString(query) || query instanceof SqlToken)
            o = { stmt: query }
            _.defaults(o, opt)
        else
            o = _.defaults({}, opt, query)

        @_generateSql(o)
        o.onError ?= (e) -> callback(e)
        @adapter.execute(o)

    array: (query, callback) ->
        a = []
        opt = {
            rowShape: 'array'
            onRow: (row) -> a.push(row[0])
            onDone: () -> callback(null, a)
        }
        @execute(query, opt, callback)

    allRows: (query, callback) ->
        opt = { onAllRows: (rows) -> callback(null, rows) }
        @execute(query, opt, callback)

    loadSchema: (schema) ->
        @addSchemaItems(Table, schema.tables, @)
        @addSchemaItems(Column, schema.columns)
        @addSchemaItems(Key, schema.keys)
        @addSchemaItems(ForeignKey, schema.foreignKeys)

        @addKeyColumns(schema.keyColumns)

    addSchemaItems: (constructor, list, parent) ->
        for i in list
            p = parent ? @tablesByName[i.tableName]
            # If parent is a view, then we don't have it it, so bail out. MUST: handle views
            # in the future
            return unless p?
            new constructor(p, i)

    addKeyColumns: (list) ->
        for i in list
            c = @constraintsByName[i.constraintName]
            c.addColumn(i)

module.exports = dbObjects.Database = Database
