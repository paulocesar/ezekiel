h = require('../test-helper')
sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

describe('SqlInsert with aliased schema', () ->
    it 'handles basic INSERT', ->
        s = sql.insert('tblCustomers', { colFirstName: 'Albus', colLastName: 'Potter' })

        e = "INSERT [Customers] ([FirstName], [LastName]) VALUES ('Albus', 'Potter')"

        h.assertAlias(s, e)
    
)
