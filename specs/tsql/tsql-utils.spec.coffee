should = require('should')
_ = require('underscore')
h = require('../test-helper')

utils = null

meta = h.getMetaData()

checkTables = (tables) ->
    actual = _.map(tables, ((t) -> t.name))
    expected = _.map(meta.tables, ((t) -> t.name))
    actual.should.eql(expected)

checkColumns = (columns) ->
    expected = meta.columns
    columns.length.should.eql(expected.length)
    for c in columns
        should.exist(c.name)
        should.exist(c.tableName)

checkKeys = (keys) ->
    expected = meta.keys
    keys.length.should.eql(expected.length)
    for k in keys
        should.exist(k.name)
        should.exist(k.tableName)

checkForeignKeys = (foreignKeys) ->
    expected = meta.foreignKeys
    foreignKeys.length.should.eql(expected.length)
    for fk in foreignKeys
        should.exist(fk.name)
        should.exist(fk.parentKeyName)
        should.exist(fk.tableName)

checkKeyColumns = (keyColumns) ->
    expected = meta.keyColumns
    keyColumns.length.should.eql(expected.length)
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

    it 'should retrieve SQL options', (done) ->
        utils.getOptions (err, opt) ->
            return done(err) if err
            opt.should.eql([ 'ANSI_WARNINGS', 'ANSI_PADDING', 'ANSI_NULLS', 'ARITHABORT',
                'QUOTED_IDENTIFIER', 'ANSI_NULL_DFLT_ON', 'CONCAT_NULL_YIELDS_NULL' ])
            done()

)

describe('Schema functions', () ->
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
            #s = JSON.stringify(m, null, 4)
            #require('fs').writeFileSync('./metadata.json', s)
            checkTables(m.tables)
            checkColumns(m.columns)
            checkKeys(m.keys)
            checkForeignKeys(m.foreignKeys)
            checkKeyColumns(m.keyColumns)

            done()
        )
    )
)
