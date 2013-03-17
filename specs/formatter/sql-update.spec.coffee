h = require('../test-helper')
{ SqlUpdate } = sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

describe('SqlUpdate', () ->
    it('works for a basic statement', ->

        u = sql.update("MyTable", { name: 'Gonzo' }, {id: 10})
        h.assertSql(u, "UPDATE [MyTable] SET [name] = 'Gonzo' WHERE [id] = 10", false)
    )
)
