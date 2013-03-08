h = require('../../test-helper')
sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

describe 'SqlInsert with mocked schema', () ->
    it 'filters out properties that do not correspond to columns', ->
        s = sql.insert('Customers', { FirstName: 'Albus', LastName: 'Potter', etl: true })

        e = "INSERT [Customers] ([FirstName], [LastName]) VALUES ('Albus', 'Potter')"

        h.assertSchemaSql(s, e)
