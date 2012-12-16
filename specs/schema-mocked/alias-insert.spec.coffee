h = require('../test-helper')
sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

describe('SqlInsert with aliased schema', () ->
    it('Handles basic INSERT', ->
        s = sql.insert('customer', { CustomerFirstName: 'Albus', CustomerLastName: 'Potter' })

        e = "INSERT [Customers] ([FirstName], [LastName]) VALUES ('Albus', 'Potter')"

        h.assertAlias(s, e)
    )
)
