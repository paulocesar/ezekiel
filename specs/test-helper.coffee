util = require('util')
path = require('path')
_ = require('underscore')
F = require('functoids/src')
A = async = require('async')

testConfig = require('./config.json')
sourceFolder = path.resolve(__dirname, '../src')
requireSrc = (pathToFile) -> require(path.resolve(sourceFolder, pathToFile))

ezekiel = requireSrc()
sql = requireSrc('sql')
SqlFormatter = requireSrc('dialects/sql-formatter')
Database = requireSrc('access/database')
DbSchema = requireSrc('schema/db-schema')

debug = false
defaultEngine = 'mssql'

blankDb = () ->
    db = new Database({ database: 'blank' })
    db.Formatter = SqlFormatter
    return db

aliasedDb = () ->
    db = new Database({database: 'alias'})
    db.Formatter = SqlFormatter
    db.loadSchema(newCookedSchema())
    return db

schemaDb = () ->
    db = new Database( {database: 'withSchema' })
    db.Formatter = SqlFormatter
    db.loadSchema(newRawSchema())
    return db

assertSqlFormatting = (schema, sql, expected, debug) ->
    f = new SqlFormatter(schema)
    ret = f.format(sql)
    if (ret != expected) || debug
        console.log("\n--- Return ---")
        console.log(ret)
        console.log("--- Expected ---")
        console.log(expected)

    ret.should.eql(expected)

connectToDb = (cb) ->
    ezekiel.connect(testConfig.databases['mssql'], (err, database) ->
        if (err)
            F.throw('Could not connect to DB while testing:', err)

        cb(database)
    )

metaData = null
getMetaData = () ->
    return metaData if metaData?
    return (metaData = require('./data/metadata'))

rawSchema = null
getRawSchema = () ->
    return rawSchema if rawSchema?
    return (rawSchema = newRawSchema())

newRawSchema = () ->
    s = new DbSchema()
    s.loadDataDictionary(getMetaData())
    return s.finish()

cookedSchema = null
getCookedSchema = () ->
    return cookedSchema if cookedSchema?
    return (cookedSchema = newCookedSchema())

newCookedSchema = () ->
    s = new DbSchema()
    s.loadDataDictionary(getMetaData())
    return cookSchema(s)

cookSchema = (s) ->
    for t in s.tables
       t.one = F.toLowerInitial(F.toSingular(t.name))
       t.many = F.toLowerInitial(F.toPlural(t.name))
       for c in t.columns
           c.property = F.toLowerInitial(c.name)

    return s.finish()

module.exports = {
    testConfig
    requireSrc
    testData: require('./data/test-data')
    defaultDbConfig: testConfig.databases[defaultEngine]

    assertSqlFormatting
    assertSql: (sql, expected, debug) -> assertSqlFormatting(null, sql, expected, debug)
    assertSchemaSql: (sql, expected, debug) -> assertSqlFormatting(getRawSchema(), sql, expected, debug)
    assertAlias: (sql, expected, debug) -> assertSqlFormatting(getCookedSchema(), sql, expected, debug)

    dump: (o, depth = 5) -> console.log(util.inspect(o, true, depth, true))

    connectToDb

    getMetaData
    getRawSchema
    newRawSchema
    getCookedSchema
    newCookedSchema
    cookSchema

    blankDb
    schemaDb
    aliasedDb,

    # useful modules: Underscore, async, functoids
    _
    async
    A
    F
}
