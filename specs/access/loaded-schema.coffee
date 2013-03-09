h = require('../test-helper')
require('../load-schema')

TableGateway = h.requireSrc('access/table-gateway')

db = schema = tables = null

before () ->
    db = h.db
    schema = db.schema
    tables = schema.tablesByAlias

customerGateway = () -> new TableGateway(db, tables.Customers)
assertCustomerOne = (done) -> (err, row) ->
    return done(err) if err
    row.Id.should.eql(1)
    done()

assertGetTableGateway = (alias) ->
    gw = db.getTableGateway(alias)
    assertGateway(gw, alias)

assertGatewayProperty = (alias) ->
    gw = db[alias]
    assertGateway(gw, alias)

assertGateway = (gw, alias) ->
    gw.should.be.an.instanceof(TableGateway)
    gw.db.should.eql(db)
    gw.table.alias.should.eql(alias)
    gw.table.should.eql(tables[alias])

describe 'Database with loaded schema', () ->
    it 'Returns Table Gateways via getTableGateway()', () ->
        assertGetTableGateway(alias) for alias of tables

    it 'Exposes gateways via property', () ->
        assertGatewayProperty(alias) for alias of tables

    it 'Supports creation of a new context', () ->
        c = { loginId: 100 }
        newDb = db.newContext(c)
        newDb.context.should.eql(c)
        for k, v of db
            continue if k in ['context', 'tableGateways']
            v.should.eql(newDb[k])
