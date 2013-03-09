_ = require('more-underscore/src')

dbObjects = require('../schema')
{ DbSchema } = dbObjects
{ SqlToken } = require('../sql')
TableGateway = require('./table-gateway')

class Database
    constructor: (@config = {}) ->
        @schema = new DbSchema()
        @name = @config.database
        @adapter = @utils = null
        @tableGateways = {}
        @tableGatewayPrototypes = {}
        @activeRecordPrototypes = {}
        @context = {}

    getTableGateway: (alias) ->
        gw = @tableGateways[alias]
        return gw if gw?

        proto = @tableGatewayPrototypes[alias]
        unless proto?
            e = "Could not find a table gateway prototype for alias #{alias}. Make sure " +
                "you have loaded a schema and check spelling and capitalization. " +
                "You can inspect the tableGatewayPrototypes " +
                "property to see the available prototypes keyed by alias. Good luck."
            throw new Error(e)

        gw = Object.create(proto)
        gw.db = @
        return (@tableGateways[alias] = gw)

    newContext: (context) ->
        newDb = Object.create(@)
        newDb.context = context
        newDb.tableGateways = {}
        return newDb

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

    loadSchema: (schema) ->
        @schema.load(schema)

        for t in @schema.tables
            gw = new TableGateway(null, t)
            alias = t.alias
            @tableGatewayPrototypes[alias] = gw

            continue if @[alias]?

            do (alias) =>
                Object.defineProperty(@, alias, {
                    get: () -> @getTableGateway(alias)
                    configurable: false
                })

        return @schema

module.exports = dbObjects.Database = Database
