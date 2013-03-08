h = require('../test-helper')
require('../load-schema')

{ DbSchema, Table, Column } = h.requireSrc('schema')

schema = null
tables = null
customers = null
before () ->
    schema = h.db.schema
    tables = schema.tablesByAlias
    customers = tables.Customers


assertUniqLine = (a) ->
    a.should.not.be.empty
    a[0].name.should.eql('UQ_OrderId_LineId')

describe 'Key', () ->
    it 'Wraps arguments into a key-value hash', () ->
        customers.pk.wrapValues(42).should.eql({Id: 42})

    it 'Throws if the values given to wrapValues() have the wrong shape', ->
        f = () -> customers.pk.wrapValues(20, 10)
        f.should.throw(/do not match the shape/)

    it 'Wraps values for a composite key', () ->
        lines = tables.OrderLines
        unique = lines.keys[1]
        unique.wrapValues(10, 20).should.eql({OrderId: 10, LineNumber: 20})

        f = () -> unique.wrapValues(55)
        f.should.throw(/do not match the shape/)
