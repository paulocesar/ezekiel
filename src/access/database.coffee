_ = require('underscore')
F = require('functoids/src')

dbObjects = require('../schema')
{ DbSchema } = dbObjects
{ SqlToken, SqlSelect } = require('../sql')
TableGateway = require('./table-gateway')
ActiveRecord = require('./active-record')
queryBinder = require('./query-binder')


msgPassActiveRecordType =
    "If you want objects from a query that lacks source table information, like a raw string " +
    "or SqlSelect without tables, you must provide the desired object type as a parameter. " +
    "For example: db.oneObject('SELECT * FROM customers WHERE Id = 1', 'customer', cb)"

newActiveRecord = (proto, tableGateway) ->
    ar = Object.create(proto)
    ar.attach(tableGateway)
    return ar
    
# MUST: take schema as config object or never again. Ezekiel will take care of reading schema files
# or loading meta data from the database.
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
        gw = @getTableGateway(proto._schema.many)
        return newActiveRecord(proto, gw)

    getProtoOrThrow: (propertyName, key) ->
        proto = @[propertyName][key]
        return proto if proto?

        F.throw("Could not find an entry in #{propertyName} for #{key}. Make sure"
            "you have loaded a schema into this database instance and check spelling and"
            "capitalization. You can inspect the #{propertyName} property to see the available"
            "prototypes. Good luck.")

    newContext: (context) ->
        newDb = Object.create(@)
        newDb.context = context
        newDb.tableGateways = {}
        return newDb

    run: (stmt, cb) ->
        @execute(stmt, { onDone: () -> cb(null) }, cb)

    noData: (stmt, cb) ->
        @execute(stmt, { onDone: () -> cb(null) }, cb)

    # MUST: add tryOneObject(), streamRows(), streamObjects()

    scalar: (query, cb) -> @_selectOneRow(query, 'array', false, cb)
    tryScalar: (query, cb) -> @_selectOneRow(query, 'array', true, cb)

    oneRow: (query, cb) -> @_selectOneRow(query, 'object', false, cb)
    tryOneRow: (query, cb) -> @_selectOneRow(query, 'object', true, cb)

    oneObject: (query, typeOrCb, cbOrNull) ->
        wrapper = @_buildRowWrapper(query, typeOrCb, cbOrNull)
        return @oneRow(query, wrapper)

    _buildRowWrapper: (query, typeOrCb, cbOrNull) ->
        cb = cbOrNull ? typeOrCb
        if cbOrNull?
            one = typeOrCb
            proto = @getProtoOrThrow('activeRecordPrototypes', one)
            gw = @getTableGateway(proto.schema.many)
        else
            gw = @_tableGatewayFromQuery(query)
            proto = @getProtoOrThrow('activeRecordPrototypes', gw.schema.one)

        # MUST: make sure result set covers at least one key in schema, throw
        # otherwise

        return (err, data) ->
            return cb(err) if err

            if _.isArray(data)
                for row, i in data
                    data[i] = newActiveRecord(proto, gw).setPersisted(row)
                result = data
            else
                result = newActiveRecord(proto, gw).setPersisted(data)
            
            cb(null, result)

    _tableGatewayFromQuery: (query) ->
        unless query instanceof SqlSelect
            F.throw("Cannot find source table for query '#{query}'"
                "because it is not an instance of SqlSelect.", msgPassActiveRecordType)

        many = query.tables[0]
        unless many?
            F.throw("#{query} does not have any source tables.", msgPassActiveRecordType)

        many = F.firstOrSelf(many)
        unless _.isString(many)
            F.throw("#{query} has #{many} as its first table, which is not a"
                "string, so I don't know which object type you want.", msgPassActiveRecordType)

        gw = @getTableGateway(many)
        return gw

    _selectOneRow: (query, rowShape, allowEmpty, cb) ->
        opt = {
            rowShape: rowShape
            onAllRows: (rows) ->
                if rows.length == 0
                    if !allowEmpty
                        e  = "_selectOneRow: No data returned for query #{query}. Expected 1 row."
                        return cb(e)
                    else
                        return cb(null, null)

                if (rows.length != 1)
                    e = "_selectOneRow: Too many rows returned for query #{query}. " +
                        "Expected 1 row but got #{rows.length}"
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

    allObjects: (query, typeOrCb, cbOrNull) ->
        wrapper = @_buildRowWrapper(query, typeOrCb, cbOrNull)
        @allRows(query, wrapper)

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

            gw = @getTableGateway(t.many)
            return gw.findOne.apply(gw, arguments)

module.exports = dbObjects.Database = Database
