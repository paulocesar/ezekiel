should = require('should')
_ = require('underscore')
h = require('../test-helper')

utils = null

checkTables = (tables) ->
    names = _.map(tables, ((t) -> t.name))
    names.should.eql(['Customers', 'OrderLines', 'Orders', 'Products'])

checkColumns = (columns) ->
    columns.length.should.eql(12)
    for c in columns
        should.exist(c.name)
        should.exist(c.tableName)

checkKeys = (keys) ->
    keys.length.should.eql(6)
    for k in keys
        should.exist(k.name)
        should.exist(k.tableName)

checkForeignKeys = (foreignKeys) ->
    foreignKeys.length.should.eql(3)
    for fk in foreignKeys
        should.exist(fk.name)
        should.exist(fk.parentKeyName)
        should.exist(fk.tableName)

checkKeyColumns = (keyColumns) ->
    keyColumns.length.should.eql(10)
    for c in keyColumns
        should.exist(c.constraintName)
        should.exist(c.tableName)
        should.exist(c.columnName)
        should.exist(c.position)


describe('TsqlUtils', () ->
    before(() ->
        utils = h.getSharedDb('mssql').utils
    )

    it('should create a date now', (done) ->
        utils.dbNow((err, date) ->
            x = new Date(date)
            date.toTimeString().should.eql(x.toTimeString())
            done()
        )
    )

    it('should create a date now on utc', (done) ->
        utils.dbUtcNow((err, date) ->
            x = new Date(date)
            date.toTimeString().should.eql(x.toTimeString())
            done()
        )
    )

    it('should check the offset to utc', (done) ->
        utils.dbUtcOffset((err, offset) ->
            offset.should.a('number')
            done()
        )
    )

)

describe('utils functions', () ->
    it('reads tables', (done) ->
        utils.getTables((err, tables) ->
            checkTables(tables)
            done()
        )
    )

    it('reads all columns in the database', (done) ->
        utils.getColumns((err, columns) ->
            checkColumns(columns)
            done()
        )
    )

    it('reads primary keys and unique constraints', (done) ->
        utils.getKeys((err, keys) ->
            checkKeys(keys)
            done()
        )
    )

    it('reads foreign keys', (done) ->
        utils.getForeignKeys((err, foreignKeys) ->
            checkForeignKeys(foreignKeys)
            done()
        )
    )

    it('reads key columns', (done) ->
        utils.getKeyColumns((err, keyColumns) ->
            checkKeyColumns(keyColumns)
            done()
        )
    )

    it('reads all metadata', (done) ->
        utils.buildFullSchema((err, m) ->
            checkTables(m.tables)
            checkColumns(m.columns)
            checkKeys(m.keys)
            checkForeignKeys(m.foreignKeys)
            checkKeyColumns(m.keyColumns)
            done()
        )
    )
)
