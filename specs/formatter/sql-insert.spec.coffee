h = require('../test-helper')

{ SqlInsert } = sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

describe 'SqlInsert', () ->
    it 'handles nulls as values', ->
        q = sql.insert('table', { id: 10, name: null })
        h.assertSql(q, 'INSERT [table] ([id], [name]) VALUES (10, NULL)', false)

    it 'accepts an OUTPUT column for id retrieval', ->
        q = sql.insert('fighters', { lastName: 'Liddell' }).output("id")
        h.assertSql(q, "INSERT [fighters] ([lastName]) OUTPUT [inserted].[id] VALUES ('Liddell')")

    it 'accepts aliases and expressions in OUTPUT', ->
        q = sql.insert('fighters', { lastName: 'Liddell' })
            .output("id", [42, 'bomba'], "LEN(lastName)", [sql.coalesce('lastName', 'firstName')])

        e = "INSERT [fighters] ([lastName]) OUTPUT [inserted].[id], 42 as [bomba], LEN(lastName), " +
            "COALESCE([lastName], [firstName]) VALUES ('Liddell')"

        h.assertSql(q, e)

    it 'Understands inserted.* in OUTPUT', ->
        q = sql.insert('fighters', { lastName: 'Liddell' })
            .output("inserted.id")

        e = "INSERT [fighters] ([lastName]) OUTPUT [inserted].[id] VALUES ('Liddell')"

        h.assertSql(q, e)
