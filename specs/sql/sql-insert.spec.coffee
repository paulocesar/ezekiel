h = require('../test-helper')

{ SqlInsert } = sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

describe('SqlInsert', () ->
    it('works for a basic statement', ->
        h.assertSql(sql.insert("MyTable"), 'INSERT [MyTable] () VALUES ()', false)
    )
)
