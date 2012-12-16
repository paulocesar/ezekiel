h = require('../test-helper')
sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

describe('SqlDelete with aliased schema', () ->
    it('Handles basic DELETE', ->
        s = sql.delete('customer',
            { customerLastName: 'Fletcher', customerFirstName: 'Mundungus'})

        e = "DELETE FROM [Customers] WHERE ([customerLastName] = 'Fletcher' " +
            "AND [customerFirstName] = 'Mundungus')"
    
        h.assertAlias(s, e)
    )
)
