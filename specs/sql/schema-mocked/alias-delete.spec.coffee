h = require('../../test-helper')
sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

describe('SqlDelete with aliased schema', () ->
    it 'handles basic DELETE', ->
        s = sql.delete('tblCustomers',
            { colLastName: 'Fletcher', colFirstName: 'Mundungus'})

        e = "DELETE FROM [Customers] WHERE ([LastName] = 'Fletcher' " +
            "AND [FirstName] = 'Mundungus')"
    
        h.assertAlias(s, e)
    
)
