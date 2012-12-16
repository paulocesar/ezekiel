h = require('../test-helper')
sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

describe 'SqlUpdate with aliased schema', () ->
    it 'handles basic UPDATE', ->
        s = sql.update('tblCustomers', { colFirstName: 'Lucius' }, { colLastName: 'Malfoy' })

        e = "UPDATE [Customers] SET [FirstName] = 'Lucius' WHERE [LastName] = 'Malfoy'"

        h.assertAlias(s, e)

    it 'filters out properties that do not correspond to columns', ->
        s = sql.update('tblCustomers', { colFirstName: 'Lucius', houseKeeping: true },
            { colLastName: 'Malfoy' })

        e = "UPDATE [Customers] SET [FirstName] = 'Lucius' WHERE [LastName] = 'Malfoy'"

        h.assertAlias(s, e)
