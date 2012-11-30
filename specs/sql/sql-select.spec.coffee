h = require('../test-helper')
sql = h.requireSrc('sql')

describe('SqlSelect', () ->
    it('detects expressions as columns', ->
        s = sql.from(['customers', 'C']).select( ["LEN(LastName)", "LenLastName"] )
        h.assertSql(s, "SELECT LEN(LastName) as [LenLastName] FROM [customers] as [C]")
    )

    it('handles two objects ANDed', () ->
        s = sql.from('users')
                .select("login", [ 'zid', 'id' ], [ 'zname', 'name' ])
                .where({ age: 22, name: 'deividy' })
                .and({ test: 123, testing: 1234 })

        exp = "SELECT [login] as [login], [zid] as [id], [zname] as [name] "
        exp += "FROM [users] as [users] "
        exp += "WHERE ([age] = 22 AND [name] = 'deividy' "
        exp += "AND ([test] = 123 AND [testing] = 1234))"

        h.assertSql(s, exp, false)
    )

    it('supports table aliases', () ->
        s = sql.from(['users', 'u'])
                .select("login", [ 'zid', 'id' ], [ 'zname', 'name' ])
                .where({ age: 22, name: 'deividy' })
                .or({ test: 123, testing: 1234 })

        exp = "SELECT [login] as [login], [zid] as [id], [zname] as [name] "
        exp += "FROM [users] as [u] "
        exp += "WHERE (([age] = 22 AND [name] = 'deividy') "
        exp += "OR ([test] = 123 AND [testing] = 1234))"

        h.assertSql(s, exp)
    )

    it('supports JOINS', () ->
        s = sql.from(['users', 'u'])
            .select("login", [ 'zid', 'id' ], [ 'zname', 'name' ])
            .join("messages", { "U.id": sql.name("Messages.UserId")})

        exp = "SELECT [login] as [login], [zid] as [id], [zname] as [name] "
        exp += "FROM [users] as [u] INNER JOIN [messages] as [messages] ON "
        exp += "[U].[id] = [Messages].[UserId]"

        h.assertSql(s, exp, false)
    )

    it('supports ORDER BY and GROUP BY', () ->
        s = sql.from(['users', 'u'])
            .select("login")
            .orderBy('name', 'LEN(LastName)', ["JoinDate", 'DESC'])
            .groupBy('City', 'Foo + Bar')

        exp = "SELECT [login] as [login] FROM [users] as [u] "
        exp += "GROUP BY [City], Foo + Bar "
        exp += "ORDER BY [name] ASC, LEN(LastName) ASC, [JoinDate] DESC"

        h.assertSql(s, exp, false)
    )

    it('can be instantiated via sql.select', () ->
        s = sql.select('FirstName', 'LastName').from('Customers')
        exp = "SELECT [FirstName] as [FirstName], [LastName] as [LastName] FROM [Customers] as [Customers]"

        h.assertSql(s, exp, false)
    )
)
