h = require('../../test-helper')
sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

describe 'SqlInsert with aliased schema', () ->
    it 'handles basic INSERT', ->
        s = sql.insert('fighters', { firstName: 'Erick', lastName: 'Silva' })
        e = "INSERT [Fighters] ([FirstName], [LastName]) VALUES ('Erick', 'Silva')"
        h.assertAlias(s, e)

    it 'filters out properties that do not correspond to columns', ->
        s = sql.insert('fighters', { firstName: 'Erick', lastName: 'Silva', etl: true })
        e = "INSERT [Fighters] ([FirstName], [LastName]) VALUES ('Erick', 'Silva')"
        h.assertAlias(s, e)
