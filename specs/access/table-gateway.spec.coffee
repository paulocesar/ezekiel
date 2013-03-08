h = require('../test-helper')
require('../load-schema')

TableGateway = h.requireSrc('access/table-gateway')

db = schema = tables = null

before () ->
    db = h.db
    schema = db.schema
    tables = schema.tablesByAlias

customerGateway = () -> new TableGateway(db, tables.Customers)

describe 'TableGateway', () ->
    it 'Can be instantiated', () -> customerGateway()

    it 'Can find one row', (done) ->
        g = customerGateway()
        g.findOne 1, (err, row) ->
            return done(err) if err

            row.Id.should.eql(1)
            done()
