h = require('../test-helper')
sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

describe 'SqlUpdate with mocked schema', () ->
    it 'filters out properties that do not correspond to columns', ->
        s = sql.update('Customers', { FirstName: 'Lucius', houseKeeping: true },
            { LastName: 'Malfoy' })

        e = "UPDATE [Customers] SET [FirstName] = 'Lucius' WHERE [LastName] = 'Malfoy'"

        h.assertSchemaSql(s, e)
