h = require('../test-helper')

should = require('should')
{ Table, Column } = h.requireSrc('db')

testDb = null
withDbAndSchema = (f) -> f(h.blankDb(), h.newSchema())

describe('Database schema handling', () ->
    it 'can load schema from a database', (done) ->
        h.connectToDb((freshDb) ->
            testDb = freshDb

            testDb.utils.buildFullSchema( (err, s) ->
                throw new Error(err) if err
                testDb.loadSchema(s)

                done()
            )
        )
    

    it 'loads schema correctly', () ->
        testDb.tables.length.should.eql(4)
    

    it 'detects clashes in column positions', () ->
        withDbAndSchema((db, s) ->
            s.columns[1].position = 1
            (() -> db.loadSchema(s)).should.throw(/Expected position/)
        )
    

    it 'throws if name and alias are missing', ->
        withDbAndSchema((db, s) ->
            s.tables[0].name = null
            (() -> db.loadSchema(s)).should.throw(/must provide/)
        )
    

    it 'allows tables with only an alias', ->
        withDbAndSchema((db, s) ->
            s.tables.push({ alias: 'FakePlasticTable' })
            db.loadSchema(s)
            db.tables.length.should.eql(3)

            t = db.tablesByAlias.FakePlasticTable
            t.should.be.an.instanceof(Table)
            should.not.exist(t.name)
            t.alias.should.eql('FakePlasticTable')

            c = new Column(t, { name: 'Id' })
            c.table.should.eql(t)
            t.columns.length.should.eql(1)
        )
    

    it 'throws if a name clashes', ->
        db = h.schemaDb()
        clash = () -> db.tablesByName.Customers.name = 'Orders'
        clash.should.throw(/it is already taken/)


    it 'throws if an alias clashes', ->
        db = h.schemaDb()
        clash = () -> db.tablesByName.Customers.alias = 'Orders'
        clash.should.throw(/it is already taken/)


    it 'keeps the aliases hash tidy', ->
        a = h.aliasedDb().tablesByAlias
        Object.keys(a).sort().should.eql(['tblCustomers', 'tblOrders'])

        a.tblCustomers.alias = 'cust'
        Object.keys(a).sort().should.eql(['cust', 'tblOrders'])


    it 'knows if a column is the full primary key', ->
        db = h.schemaDb()
        c = db.tablesByAlias.Customers
        c.columnsByAlias.Id.isFullPrimaryKey().should.be.true
    

    it 'knows that a column in a composite PK is not the full PK', ->
        withDbAndSchema((db, s) ->
            s.keyColumns.push({
                columnName: 'Id', tableName: 'Customers', position: 2,
                constraintName: 'PK_Customers'
            })

            db.loadSchema(s)
            c = db.tablesByAlias.Customers
            c.columnsByAlias.Id.isFullPrimaryKey().should.be.false
        )
    
)
