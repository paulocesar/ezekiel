h = require('../../test-helper')
sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

describe('SqlDelete with aliased schema', () ->
    it 'handles basic DELETE', ->
        s = sql.delete('fighters',
            { lastName: 'Test', firstName: 'Unit'})

        e = "DELETE FROM [Fighters] WHERE ([LastName] = 'Test' " +
            "AND [FirstName] = 'Unit')"
    
        h.assertAlias(s, e)
    
)
