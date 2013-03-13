h = require('../test-helper')
{ SqlExpression, SqlRawName, SqlFullName } = sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

sys = require('sys')

f = new SqlFormatter(h.blankDb())

describe('SqlFormatter', () ->
    it('parses SQL names', ->
        f.fullNameFromString("a.b.c").parts.should.eql(['a', 'b', 'c'])
        f.fullNameFromString("foo.bar").parts.should.eql(['foo', 'bar'])
        f.fullNameFromString("some.Table").parts.should.eql(['some', 'Table'])
    )

    it('Emits SQL names correctly', ->
        multi = sql.name(["Db", "Schema", "Table"])
        multi.toSql(f).should.eql("[Db].[Schema].[Table]")

        noPrefix = sql.name("Users")
        noPrefix.toSql(f).should.eql("[Users]")

        withPrefix = sql.name("O.Name")
        withPrefix.toSql(f).should.eql("[O].[Name]")
    )

    it('Can tell expressions from names', ->
        f.parseNameOrExpression("Qty * Price").should.be.an.instanceOf(SqlExpression)
        f.parseNameOrExpression("Foobar").should.be.an.instanceOf(SqlFullName)
    )
)
