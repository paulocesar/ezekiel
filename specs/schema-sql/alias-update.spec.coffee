h = require('../test-helper')
sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

describe('SqlUpdate with aliased schema', () ->
    it('Handles basic UPDATE', ->
        s = sql.update('customer', { customerFirstName: 'Lucius' },
            { customerLastName: 'Malfoy' })

        e = "UPDATE [Customers] SET [customerFirstName] = 'Lucius' " +
            "WHERE [customerLastName] = 'Malfoy'"

        h.assertAlias(s, e)
    )
)
