util = require('util')
path = require('path')
_ = require('more-underscore/src')
async = require('async')

sql = require('../src/sql')

testConfig = require('./config.json')
sourceFolder = path.resolve(__dirname, '../src')
requireSrc = (pathToFile) -> require(path.resolve(sourceFolder, pathToFile))

SqlFormatter = requireSrc('dialects/sql-formatter')
ezekiel = requireSrc()
Database = requireSrc('access/database')

debug = false
sharedDb = null
defaultEngine = 'mssql'

newSchema = -> {
    tables: [
        { name: 'Customers', }
        { name: 'Orders' }
    ]
    columns: [
        { tableName: 'Customers', name: 'Id', position: 1 }
        { tableName: 'Customers', name: 'FirstName', position: 2 }
        { tableName: 'Customers', name: 'LastName', position: 3 }
    ]
    keys: [
        { type: 'PRIMARY KEY', tableName: 'Customers', name: 'PK_Customers' }
    ]
    foreignKeys: []
    keyColumns: [
        { columnName: 'Id', tableName: 'Customers', position: 1, constraintName: 'PK_Customers' }
    ]
}

newAliasedSchema = -> {
    tables: [
        { name: 'Customers', alias: 'tblCustomers' }
        { name: 'Orders', alias: 'tblOrders' }
    ]
    columns: [
        { tableName: 'Customers', name: 'Id', alias: 'colId', position: 1 }
        { tableName: 'Customers', name: 'FirstName', alias: 'colFirstName', position: 2 }
        { tableName: 'Customers', name: 'LastName', alias: 'colLastName', position: 3 }
    ]
    keys: []
    foreignKeys: []
    keyColumns: []
}

blankDb = () ->
    db = new Database({ database: 'blank' })
    db.Formatter = SqlFormatter
    return db

aliasedDb = () ->
    db = new Database({database: 'alias'})
    db.Formatter = SqlFormatter
    db.loadSchema(newAliasedSchema())
    return db

schemaDb = () ->
    db = new Database( {database: 'withSchema' })
    db.Formatter = SqlFormatter
    db.loadSchema(newSchema())
    return db

assertSqlFormatting = (db, sql, expected, debug) ->
    f = new SqlFormatter(db)
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

module.exports = {
    testConfig
    requireSrc
    defaultDbConfig: testConfig.databases[defaultEngine]

    assertSqlFormatting
    assertSql: (sql, expected, debug) -> assertSqlFormatting(blankDb(), sql, expected, debug)
    assertSchemaSql: (sql, expected, debug) -> assertSqlFormatting(schemaDb(), sql, expected, debug)
    assertAlias: (sql, expected, debug) -> assertSqlFormatting(aliasedDb(), sql, expected, debug)

    dump: (o, depth = 5) -> console.log(util.inspect(o, true, depth, true))

    getSharedDb: (engine = defaultEngine) -> sharedDb
    connectToDb

    newSchema
    newAliasedSchema
    blankDb
    schemaDb
    aliasedDb,

    _,
    async
}
