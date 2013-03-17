h = require('../../test-helper')
sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

describe 'SqlUpdate with aliased schema', () ->
    it 'handles basic UPDATE', ->
        s = sql.update('fighters', { firstName: 'Chael' }, { lastName: 'Sonnen' })
        e = "UPDATE [Fighters] SET [FirstName] = 'Chael' WHERE [LastName] = 'Sonnen'"
        h.assertAlias(s, e)

    it 'filters out properties that do not correspond to columns', ->
        s = sql.update('fighters', { firstName: 'Rodrigo', houseKeeping: true },
            { lastName: 'Minotauro', weightInLb: ">": 205 })

        e = "UPDATE [Fighters] SET [FirstName] = 'Rodrigo' WHERE ([LastName] = 'Minotauro' " +
            "AND [WeightInLb] > 205)"

        h.assertAlias(s, e)
