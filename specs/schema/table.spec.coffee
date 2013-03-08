h = require('../test-helper')
require('../load-schema')

{ DbSchema, Table, Column } = h.requireSrc('schema')

schema = null
tables = null
before () ->
    schema = h.db.schema
    tables = schema.tablesByAlias

assertUniqLine = (a) ->
    a.should.not.be.empty
    a[0].name.should.eql('UQ_OrderId_LineId')

describe 'Table', () ->
    it 'Finds single keys by their shape', () ->
        customers = tables.Customers

        customers.getKeysWithShape('fonzo').should.eql([])
        customers.getKeysWithShape(10).should.eql([customers.pk])

        orders = tables.Orders
        orders.getKeysWithShape(1).should.eql([orders.pk])

        orderLines = tables.OrderLines
        orderLines.getKeysWithShape(1).should.eql([orderLines.pk])

    it 'Finds composite keys by the types of its values', () ->
        orderLines = tables.OrderLines

        assertUniqLine(orderLines.getKeysWithShape(10, 10))
        assertUniqLine(orderLines.getKeysWithShape([10, 10]))

