_ = require('more-underscore/src')

dbObjects = require('../schema')
{ DbSchema } = dbObjects
{ SqlToken } = require('../sql')
TableGateway = require('./table-gateway')
ActiveRecord = require('./active-record')

class Database
    constructor: (@config = {}) ->
        @schema = new DbSchema()
        @name = @config.database
        @adapter = @utils = null
        @tableGateways = {}
        @tableGatewayPrototypes = {}
        @activeRecordPrototypes = {}
        @context = {}

    getTableGateway: (many) ->
        gw = @tableGateways[many]
        return gw if gw?

        proto = @getProtoOrThrow('tableGatewayPrototypes', many)

        gw = Object.create(proto)
        gw.db = @
        return (@tableGateways[many] = gw)

    newObject: (one) ->
        proto = @getProtoOrThrow('activeRecordPrototypes', one)
        gw = @getTableGateway(proto.schema.many)

        ar = Object.create(proto)
        ar.attach(gw)
        return ar

    getProtoOrThrow: (propertyName, key) ->
        proto = @[propertyName][key]
        return proto if proto?

        e = "Could not find an entry in #{propertyName} for #{key}. Make sure you " +
            "have loaded a schema into this database instance and check spelling and " +
            "capitalization. You can inspect the #{propertyName} property to see the available " +
            "prototypes. Good luck."

        throw new Error(e)

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
            @tableGatewayPrototypes[t.many] = new TableGateway(null, t)
            @activeRecordPrototypes[t.one] = new ActiveRecord(null, t)

            @makeGatewayAccessor(t.many, @getTableGateway)

        return @schema

    makeGatewayAccessor: (many) ->
        return if @[many]?

        Object.defineProperty(@, many, {
            get: () -> @getTableGateway(many)
            configurable: false
        })

module.exports = dbObjects.Database = Database
