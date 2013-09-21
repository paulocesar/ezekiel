h = require('../test-helper')
{ SqlExpression, SqlRawName, SqlFullName, SqlSelect } = sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/mysql/mysql-formatter')

f = new SqlFormatter(h.blankDb())


describe 'SqlFormatter', () ->
    it 'parses SQL names', ->
        f.fullNameFromString("a.b.c").parts.should.eql(['a', 'b', 'c'])
        f.fullNameFromString("foo.bar").parts.should.eql(['foo', 'bar'])
        f.fullNameFromString("some.Table").parts.should.eql(['some', 'Table'])

    it 'emits SQL names correctly', ->
        multi = sql.name(["Db", "Schema", "Table"])
        multi.toSql(f).should.eql("`Db`.`Schema`.`Table`")

        noPrefix = sql.name("Users")
        noPrefix.toSql(f).should.eql("`Users`")

        withPrefix = sql.name("O.Name")
        withPrefix.toSql(f).should.eql("`O`.`Name`")

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



    # SELECT

    it 'should select item', ->
        expected = "SELECT `name`, `table`.`id` FROM `table`"
        sql.select('name','table.id').from('table').toSql(f).should.be.eql(expected)

        expected = "SELECT `firstName` FROM `users` "
        expected += "WHERE (`age` = 10 AND `lastName` LIKE '%harry%')"
        sql.select('firstName').from('users').where({age: 10, lastName: '%harry%'})
            .toSql(f).should.be.eql(expected)

    it 'should limit the search', ->
        expected = "SELECT `name` FROM `users` LIMIT 5"
        sql.select('name').from('users').limit(5).toSql(f).should.be.eql(expected)



    # DELETE

    it 'should return a delete query', ->
        expected = "DELETE FROM `users` WHERE (`age` = 10 AND `age2` = 20)"
        sql.delete('users').where({age: 10, age2: 20}).toSql(f).should.be.eql(expected)




