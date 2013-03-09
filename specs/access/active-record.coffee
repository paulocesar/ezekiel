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

describe 'TableGateway', () ->
    it 'Can be instantiated', () -> customerGateway()

    it 'Finds one row', (done) ->
        g = customerGateway()
        g.findOne(1, assertCustomerOne(done))

    it 'Can postpone query execution', (done) ->
        g = customerGateway()
        g.findOne(1).run(assertCustomerOne(done))

    it 'Is accessible via database property', (done) ->
        db.Customers.findOne(1, assertCustomerOne(done))
