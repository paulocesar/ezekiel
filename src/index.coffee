_ = require('underscore')
A = require('async')
F = require('functoids/src')
fs = require('fs')
P = require('path')

engines = require('./engines.json')
Database = require('./access/database')
DbSchema = require('./schema/db-schema')


# We don't want users to instantiate Database directly for a number of reasons, but the fundamental
# point is that we cannot guarantee a valid working instance without doing async work. Also, we may
# want to check things like db version before exposing certain functionality. Loading a schema is
# another thing some people want before they get their db instance.
#
# On the error front, we could have an instantaneous error (bad connection) or an error when first
# trying to connect (requires async work). So new Database() is just not nice, it would make it
# a mess for the caller to correctly check for errors. We go for this little factory instead, and as
# a bonus we can implement methods that create a DB before connecting to it, stuff like that.
#
# Database can still be instantiated directly in tests and by advanced users.
# Schema can be true to load directly from database, string with file path to load from file, or
# schema object directly.
# processSchema is a function that gets a shot at the schema before the DB binds it. This is useful
# for specifying custom one/many names for tables, property names for columns, etc.

exampleConfig = """
{
    connection: {
        host: "127.0.0.1",
        userName: "gonzo",
        password: "l33t0",
    },

    schema: true,
    processSchema: fnMyNamingConvention,
    require: '/'
}
"""


validateConfig = (config) ->
    e = []
    if !config.connection?
        e.push("missing DB connection information")
    else
        validateConnection(config.connection, e)

    for b in ["schema", "testConnection"]
        if config[b]? && !_.isBoolean(config[b])
            e.push("the #{b} flag must be a boolean")

    for f in ["processSchema", "processCatalog"]
        if config[f]? && !_.isFunction(config[f])
            e.push("#{f} must be a function")

    if config.require? && !F.isGoodString(config.require)
        e.push("require must be a non-empty string")

    if config.schema == false
        for dep in ["processSchema", "require"]
            if config[dep]?
                e.push("#{dep} cannot be used if schema is set to false")

    return unless e.length > 0

    errors = e.join(", ")
    s = if e.length > 1 then "s" else ""
    msg = "Configuration error#{s}: #{errors}. Here's an example config:" + exampleConfig
    throw new Error(msg)
    
validateConnection = (conn, e = []) ->
    for f in ["host", "userName", "password"]
        unless conn[f]?
            e.push("missing #{f}")
            continue
        e.push("#{f} must be a non-empty string") unless F.isGoodString(conn[f])

    return e

removeExtensions = (fileNames) -> (P.basename(f, P.extname(f)) for f in fileNames)

resolveRequires = (config, cb) ->
    arPath = P.resolve(config.require, 'activeRecords')
    gwPath = P.resolve(config.require, 'tableGateways')

    tasks = {
        activeRecords: (cb) -> fs.readdir(arPath, cb)
        tableGateways: (cb) -> fs.readdir(gwPath, cb)
    }

    A.parallel tasks, (err, results) ->
        return cb(err) if err?

        o = { arPath, gwPath }
        o.activeRecords = removeExtensions(results.activeRecords)
        o.tableGateways = removeExtensions(results.tableGateways)
         
        cb(null, o)

doRequires = (db, requires) ->
    return unless requires?

    for name in requires.tableGateways
        proto = db.tableGatewayPrototypes[name]
        continue unless proto?

        path = P.resolve(requires.gwPath, name)
        fn = require(path)

        unless _.isFunction(fn)
            F.throw("#{path} did not export a function for extending table gateway '#{name}'")

        fn(proto, proto.schema)

    for name in requires.activeRecords
        table = db.schema.tablesByOne[name]
        continue unless table?

        gw = db.tableGatewayPrototypes[table.many]
        path = P.resolve(requires.arPath, name)
        fn = require(path)

        unless _.isFunction(fn)
            F.throw("#{path} did not export a function for extending ActiveRecord '#{name}'")

        fn(gw.arProto, db.schema)

e = ezekiel = {
    connect: (config, cb) ->
        F.demandHash(config, 'config')
        F.demandFunction(cb, 'cb')

        if !config.connection? && config.host?
            config = { connection: config }

        validateConfig(config)

        connection = config.connection
        db = new Database(connection)

        db.adapter = ezekiel.getAdapter(connection)

        dialect = ezekiel.getDialect(connection, db)
        db.utils = new dialect.Utils(db)
        db.Formatter = dialect.Formatter

        tasks = {}
        if config.schema == false && config.testConnection != false
            tasks.sanity = (cb) -> db.scalar("SELECT 1", cb)

        if config.schema != false
            tasks.schema = (cb) -> ezekiel.loadSchema(db, config, cb)
       
        if config.require?
            tasks.requires = (cb) -> resolveRequires(config, cb)

        A.parallel tasks, (err, results) ->
            return cb(err) if err?
            doRequires(db, results.requires)
            cb(null, db)

        return

    loadSchema: (db, config, cb) ->
        db.utils.buildFullSchema (err, dataDictionary) ->
            return cb(err) if err?
            s = new DbSchema()
            s.loadDataDictionary(dataDictionary)
            config.processSchema?(s)
            db.loadSchema(s)
            return cb(null, s)

    getAdapter: (connection) ->
        name = 'tedious'
        path = "./adapters/#{name}"
        adapter = require(path)
        return new adapter(connection)

    getDialect: (connection, db) ->
        name = connection.dialect ? engines[connection.engine].dialect
        path = "./dialects/#{name}"
        return require(path)
}

module.exports = ezekiel
ezekiel.sql = require('./sql')
