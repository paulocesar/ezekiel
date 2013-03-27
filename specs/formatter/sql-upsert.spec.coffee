h = require('../test-helper')
sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

expected = """
MERGE [MyTable] AS target
USING (SELECT 10, 'Gonzo')
AS source ([id], [name])
ON (target.[id] = source.[id])
WHEN MATCHED THEN
  UPDATE SET target.[name] = source.[name]
WHEN NOT MATCHED THEN
  INSERT ([id], [name])
  VALUES (10, 'Gonzo');
"""


describe('SqlUpsert', () ->
    it('works for a basic statement', ->

        u = sql.upsert("MyTable", { id: 10, name: 'Gonzo' }, "id")
        h.assertSql(u, expected, false)
    )
)
