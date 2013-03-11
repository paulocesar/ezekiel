util = require('util')
path = require('path')
_ = require('more-underscore/src')
async = require('async')

testConfig = require('./config.json')
sourceFolder = path.resolve(__dirname, '../src')
requireSrc = (pathToFile) -> require(path.resolve(sourceFolder, pathToFile))

ezekiel = requireSrc()
sql = requireSrc('sql')
SqlFormatter = requireSrc('dialects/sql-formatter')
Database = requireSrc('access/database')
DbSchema = requireSrc('schema/db-schema')

debug = false
sharedDb = null
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
        console.log("--- Return ---")
        console.log(ret)
        console.log("---")
        console.log("--- Expected ---")
        console.log(expected)
        console.log("---")

    ret.should.eql(expected)

connectToDb = (cb) ->
    ezekiel.connect(testConfig.databases['mssql'], (err, database) ->
        if (err)
            throw new Error('Could not connect to DB while testing: ' + err)

        cb(database)
    )

before((done) ->
    connectToDb((database) ->
        sharedDb = database
        done()
    )
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
    s.load(getMetaData())
    return s.finish()

cookedSchema = null
getCookedSchema = () ->
    return cookedSchema if cookedSchema?
    return (cookedSchema = newCookedSchema())

newCookedSchema = () ->
    s = new DbSchema()
    s.load(getMetaData())
    return cookSchema(s)

cookSchema = (s) ->
    for t in s.tables
       t.one = _.chain(t.name).toSingular().toLowerInitial().value()
       t.many = _.chain(t.name).toPlural().toLowerInitial().value()
       for c in t.columns
           c.property = _.toLowerInitial(c.name)

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

    getSharedDb: (engine = defaultEngine) -> sharedDb
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

    _,
    async
}
