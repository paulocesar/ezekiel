_ = require('more-underscore/src')

dbObjects = require('../schema')
{ DbSchema, DbObject, Table, Column, Key, ForeignKey } = dbObjects
{ SqlToken } = require('../sql')

class Database
    constructor: (@config = {}) ->
        @schema = new DbSchema()
        @name = @config.database
        @adapter = @utils = null

    run: (stmt, cb) ->
        @execute(stmt, { onDone: () -> cb(null) }, cb)

    scalar: (query, cb) -> @_selectOneRow(query, 'array', false, cb)
    tryScalar: (query, cb) -> @_selectOneRow(query, 'array', true, cb)

    oneRow: (query, cb) -> @_selectOneRow(query, 'object', false, cb)
    tryOneRow: (query, cb) -> @_selectOneRow(query, 'object', true, cb)

    _selectOneRow: (query, rowShape, allowEmpty, cb) ->
        opt = {
            rowShape: rowShape
            onAllRows: (rows) ->
                if rows.length == 0
                    if !allowEmpty
                        e  = "No data returned for query #{query}. Expected 1 row."
                        return cb(e)
                    else
                        return cb(null, null)

                if (rows.length != 1)
                    e = "Too many rows returned for query #{query}. Expected 1 row " +
                        "but got #{rows.length}"
                    return cb(e)

                v = (if rowShape == 'array' then rows[0][0] else rows[0])
                cb(null, v)
        }
        @execute(query, opt, cb)

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

    loadSchema: (schema) -> @schema.load(schema)

module.exports = dbObjects.Database = Database
