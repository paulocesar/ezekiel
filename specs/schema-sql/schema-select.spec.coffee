h = require('../test-helper')
require('../load-schema')
sql = h.requireSrc('sql')

db = null

before(-> db = h.db)

assert = (sql, e, debug = false) -> h.assertSqlFormatting(db, sql, e, debug)

describe 'SqlSelect with loaded DB schema', () ->
    it 'Can deduct JOIN predicates', ->
        q = sql.select('FirstName', 'OrderDate').from('Customers').join('Orders')


        e = "SELECT [FirstName], [OrderDate] FROM [Customers] INNER JOIN [Orders] " +
            "ON ([Orders].[CustomerId] = [Customers].[Id])"

        assert(q, e)
