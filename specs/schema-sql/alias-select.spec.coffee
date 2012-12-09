h = require('../test-helper')
sql = h.requireSrc('sql')

describe('SqlSelect with aliased schema', () ->
    it('Has the right aliases in place', ->
        d = h.aliasedDb()
        Object.keys(d.tablesByAlias).sort().should.eql(['customer', 'order'])
    )


    it('Handles basic SELECT', ->
        s = sql.select('CustomerId', 'CustomerFirstName').from('customer')
        expected = "SELECT [id] as [CustomerId], [FirstName] as [CustomerFirstName] FROM " +
            "[Customers] as [customer]"
        h.assertAlias(s, expected)
    )

    it('Handles SQL prefixes', ->
        s = sql.select('customer.CustomerId', 'CustomerFirstName').from('customer')
        expected = "SELECT [customer].[id] as [CustomerId], [FirstName] as [CustomerFirstName] FROM " +
            "[Customers] as [customer]"
        h.assertAlias(s, expected)
    )

    it('Handles SELECT with WHERE clause', ->
        s = sql.select('CustomerId', 'CustomerFirstName').from('customer')
            .where({CustomerFirstName: 'Bilbo'})
        expected = "SELECT [id] as [CustomerId], [FirstName] as [CustomerFirstName] FROM " +
            "[Customers] as [customer] WHERE [FirstName] = 'Bilbo'"
        h.assertAlias(s, expected)
    )

    it('Aliases in ORDER BY', ->
        s = sql.select('CustomerId').from('customer').orderBy('CustomerFirstName')
        expected = "SELECT [id] as [CustomerId] FROM [Customers] as [customer] " +
            "ORDER BY [FirstName] ASC"
        h.assertAlias(s, expected)
    )
)
