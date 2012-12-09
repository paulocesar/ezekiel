h = require('../test-helper')
sql = h.requireSrc('sql')

describe('SQL Functions', () ->
    it 'supports nullary functions in columns and where clauses', ->
        q = sql.select(sql.now(), sql.utcNow())
            .from('customers')
            .where([sql.now(), '>': sql.utcNow()])

        e = "SELECT GETDATE(), GETUTCDATE() FROM [customers] WHERE GETDATE() > GETUTCDATE()"
        h.assertSql(q, e)

    it 'supports unary functions in columns', ->
        q = sql.select(sql.abs('price'), sql.max('price'), sql.avg('price'),
                sql.sum('price'), sql.len('productName'), sql.trim('productName'))
            .from('customers')

        e = "SELECT ABS([price]), MAX([price]), AVG([price]), SUM([price]), " +
            "LEN([productName]), RTRIM(LTRIM([productName])) FROM [customers]"

        h.assertSql(q, e)

    it 'supports n-ary functions in columns and where clauses', ->
        fillIn = 'He Who Must Not Be Named'
        name = sql.coalesce('lastName', 'firstName', sql.literal(fillIn))

        q = sql.select(name).from('customers').where([name, fillIn])

        e = "SELECT COALESCE([lastName], [firstName], 'He Who Must Not Be Named') FROM " +
            "[customers] WHERE COALESCE([lastName], [firstName], 'He Who Must Not Be Named') " +
            "= 'He Who Must Not Be Named'"

        h.assertSql(q, e)
)
