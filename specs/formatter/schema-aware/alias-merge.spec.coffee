h = require('../../test-helper')
{ SqlToken } = sql = h.requireSrc('sql')

assert = (sql, e, debug = false) -> h.assertAlias(sql, e, debug)

schema = h.getCookedSchema()
tables = schema.tablesByMany
fighters = tables.fighters

describe 'SqlMerge with aliased schema', () ->
    it 'does a basic merge', ->
        rows = h.testData.newData()
        s = sql.merge('fighters').using(rows)

        expected = "SELECT [Id] as [id], [FirstName] as [firstName] FROM " +
            "[Fighters] as [fighters]"

        assert(s, expected)
