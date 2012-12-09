h = require('../test-helper')
sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

describe('SqlInsert with aliased schema', () ->
    it('Handles basic INSERT', ->
        s = sql.insert('customer', { customerFirstName: 'Albus' },
            { customerLastName: 'Potter' })

        e = "INSERT [Customers]"

        h.assertAlias(s, e)
    )
)
