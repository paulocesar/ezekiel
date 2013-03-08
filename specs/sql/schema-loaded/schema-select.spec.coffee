h = require('../../test-helper')
require('../../load-schema')
sql = h.requireSrc('sql')

db = null

before(-> db = h.db)

assert = (sql, e, debug = false) -> h.assertSqlFormatting(db, sql, e, debug)

describe 'SqlSelect with loaded DB schema', () ->
    it 'can build the JOIN predicate for a child', ->
        q = sql.select('FirstName', 'OrderDate').from('Customers').join('Orders')

        e = "SELECT [FirstName], [OrderDate] FROM [Customers] INNER JOIN [Orders] " +
            "ON ([Orders].[CustomerId] = [Customers].[Id])"

        assert(q, e)

    it 'can build the JOIN predicate for a parent', ->
        q = sql.select('FirstName', 'OrderDate').from('Orders').join('Customers')

        e = "SELECT [FirstName], [OrderDate] FROM [Orders] INNER JOIN [Customers] " +
            "ON ([Orders].[CustomerId] = [Customers].[Id])"

        assert(q, e)
