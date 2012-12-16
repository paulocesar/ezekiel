h = require('../test-helper')
sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

describe('SqlUpdate with aliased schema', () ->
    it 'handles basic UPDATE', ->
        s = sql.update('customer', { CustomerFirstName: 'Lucius' },
            { customerLastName: 'Malfoy' })

        e = "UPDATE [Customers] SET [FirstName] = 'Lucius' " +
            "WHERE [customerLastName] = 'Malfoy'"

        h.assertAlias(s, e)

    it 'filters out properties that do not correspond to columns', ->
        s = sql.update('customer', { CustomerFirstName: 'Lucius', houseKeeping: true },
            { customerLastName: 'Malfoy' })

        e = "UPDATE [Customers] SET [FirstName] = 'Lucius' " +
            "WHERE [customerLastName] = 'Malfoy'"

        h.assertAlias(s, e)
)
