util = require('util')
sql = require('../src/sql')
path = require('path')

testConfig = require('./config.json')
sourceFolder = path.resolve(__dirname, '../src')
requireSrc = (pathToFile) -> require(path.resolve(sourceFolder, pathToFile))

SqlFormatter = requireSrc('dialects/sql-formatter')
ezekiel = requireSrc()
Database = requireSrc('db/database')

debug = false
sharedDb = null
defaultEngine = 'mssql'

newSchema = () -> {
    tables: [
        { name: 'customers', }
        { name: 'orders' }
    ]
    columns: [
        { tableName: 'customers', name: 'id', position: 1 }
        { tableName: 'customers', name: 'firstName', position: 2 }
    ]
    keys: [
        { type: 'PRIMARY KEY', tableName: 'customers', name: 'PK_Customers' }
    ]
    foreignKeys: []
    keyColumns: [
        { columnName: 'id', tableName: 'customers', position: 1, constraintName: 'PK_Customers' }
    ]
}

newAliasedSchema = () -> {
    tables: [
        { name: 'Customers', alias: 'customer' }
        { name: 'Orders', alias: 'order' }
    ]
    columns: [
        { tableName: 'Customers', name: 'id', alias: 'CustomerId', position: 1 }
        { tableName: 'Customers', name: 'FirstName', alias: 'CustomerFirstName', position: 2 }
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
        console.log("'#{ret}'")
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
    testConfig: testConfig
    requireSrc: requireSrc
    defaultDbConfig: testConfig.databases[defaultEngine]

    assertSql: (sql, expected, debug) -> assertSqlFormatting(blankDb(), sql, expected, debug)
    assertSchemaSql: (sql, expected, debug) -> assertSqlFormatting(schemaDb(), sql, expected, debug)
    assertAlias: (sql, expected, debug) -> assertSqlFormatting(aliasedDb(), sql, expected, debug)

    inspect: (o, depth = 5) -> console.log(util.inspect(o, true, depth, true))

    getSharedDb: (engine = defaultEngine) -> sharedDb
    connectToDb: connectToDb

    newSchema: newSchema
    newAliasedSchema: newAliasedSchema
    blankDb: blankDb
    schemaDb: schemaDb
    aliasedDb: aliasedDb
}
