h = require('../test-helper')
sql = h.requireSrc('sql')

describe('SqlSelect with aliased schema', () ->
    it 'has the right aliases in place', ->
        d = h.aliasedDb()
        Object.keys(d.tablesByAlias).sort().should.eql(['tblCustomers', 'tblOrders'])

    it 'does basic SELECT', ->
        s = sql.select('colId', 'colFirstName').from('tblCustomers')
        expected = "SELECT [Id] as [colId], [FirstName] as [colFirstName] FROM " +
            "[Customers] as [tblCustomers]"
        h.assertAlias(s, expected)
    

    it 'does SQL prefixes', ->
        s = sql.select('tblCustomers.colId', 'colFirstName').from('tblCustomers')

        expected = "SELECT [tblCustomers].[Id] as [colId], [FirstName] as " +
             "[colFirstName] FROM [Customers] as [tblCustomers]"

        h.assertAlias(s, expected)
    

    it 'does SELECT with WHERE clause', ->
        s = sql.select('colId', 'colFirstName').from('tblCustomers')
            .where({colFirstName: 'Bilbo'})
        expected = "SELECT [Id] as [colId], [FirstName] as [colFirstName] FROM " +
            "[Customers] as [tblCustomers] WHERE [FirstName] = 'Bilbo'"
        h.assertAlias(s, expected)
    

    it 'does ORDER BY', ->
        s = sql.select('colId').from('tblCustomers').orderBy('colFirstName')
        expected = "SELECT [Id] as [colId] FROM [Customers] as [tblCustomers] " +
            "ORDER BY [FirstName] ASC"
        h.assertAlias(s, expected)

    it 'aliases SQL * operator', ->
        s = sql.select('*').from('tblCustomers')
        expected = "SELECT [Id] as [colId], [FirstName] as [colFirstName], " +
            "[LastName] as [colLastName] FROM [Customers] as [tblCustomers]"
        h.assertAlias(s, expected)
    
)
