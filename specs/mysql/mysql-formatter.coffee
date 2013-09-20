h = require('../test-helper')
{ SqlExpression, SqlRawName, SqlFullName } = sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/mysql/mysql-formatter')

f = new SqlFormatter(h.blankDb())


describe 'SqlFormatter', () ->
    it 'parses SQL names', ->
        f.fullNameFromString("a.b.c").parts.should.eql(['a', 'b', 'c'])
        f.fullNameFromString("foo.bar").parts.should.eql(['foo', 'bar'])
        f.fullNameFromString("some.Table").parts.should.eql(['some', 'Table'])

    it 'emits SQL names correctly', ->
        multi = sql.name(["Db", "Schema", "Table"])
        multi.toSql(f).should.eql("[Db].[Schema].[Table]")

        noPrefix = sql.name("Users")
        noPrefix.toSql(f).should.eql("[Users]")

        withPrefix = sql.name("O.Name")
        withPrefix.toSql(f).should.eql("[O].[Name]")

    it 'can tell expressions from names', ->
        f.parseNameOrExpression("Qty * Price").should.be.an.instanceOf(SqlExpression)
        f.parseNameOrExpression("Foobar").should.be.an.instanceOf(SqlFullName)

    it 'escapes single quotes to avoid SQL injections', ->
        expected = "'foo'' SELECT * FROM hax0r'"
        f.format("foo' SELECT * FROM hax0r").should.eql(expected)

    it 'formats literals correctly', ->
        f.format("foo").should.eql("'foo'")
        f.format(54.4983).should.eql("54.4983")

        d = new Date(2013, 2, 25, 17, 5, 10, 22)
        f.format(d).should.eql("'2013-03-25 17:05:10.022'")