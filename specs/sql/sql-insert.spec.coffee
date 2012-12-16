h = require('../test-helper')

{ SqlInsert } = sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

describe('SqlInsert', () ->
    it('works for a basic statement', ->
        h.assertSql(sql.insert("MyTable"), 'INSERT [MyTable] () VALUES ()', false)
    )

    it 'handles nulls as values', ->
        q = sql.insert('table', { id: 10, name: null })
        h.assertSql(q, 'INSERT [table] ([id], [name]) VALUES (10, NULL)', false)
)
