_ = require('more-underscore/src')

dbObjects = require('../schema')
{ DbSchema } = dbObjects
{ SqlToken } = require('../sql')
TableGateway = require('./table-gateway')
ActiveRecord = require('./active-record')
queryBinder = require('./query-binder')

# MUST: take schema as config object or never again. Ezekiel will take care of reading schema files
# or loading meta data from the database.
#
class Database
    constructor: (@config = {}) ->
        @schema = @config.schema ? null
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

    noData: (stmt, cb) ->
        @execute(stmt, { onDone: () -> cb(null) }, cb)

    # MUST: add oneObject(), tryOneObject(), streamRows(), streamObjects()

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
        f = new @Formatter(@schema)
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

    bindOrCall: (q, fn, cb) ->
        return @[fn](q, cb) if cb?
        return queryBinder.bind(q, @, fn)

    loadSchema: (schema) ->
        @schema = schema.finish()

        for t in @schema.tables
            @tableGatewayPrototypes[t.many] = new TableGateway(null, t)
            @activeRecordPrototypes[t.one] = new ActiveRecord(null, t)
            @makeAccessors(t)

        return @schema

    makeAccessors: (t) ->
        unless t.many of @
            Object.defineProperty(@, t.many, {
                configurable: false, get: () -> @getTableGateway(t.many)
            })

        unless t.one of @
            getter = @makeObjectGetter(t)
            Object.defineProperty(@, t.one, {
                configurable: false, get: () -> getter
            })

    makeObjectGetter: (t) ->
        () ->
            o = @newObject(t.one)
            return o if arguments.length == 0

            arg = arguments[0]
            return o.setMany(arg) if _.isObject(arg)

module.exports = dbObjects.Database = Database
