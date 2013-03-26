h = require('../test-helper')
{ SqlPredicate } = sql = h.requireSrc('sql')

describe('SqlPredicate', () ->
    it 'builds a predicate from an object', ->
        p = SqlPredicate.wrap({firstName: 'Ron', lastName: 'Weasley'})
        e = "([firstName] = 'Ron' AND [lastName] = 'Weasley')"
        h.assertSql(p, e)

    it 'builds OR clauses with sql.or()', ->
        p = sql.or({lastName: 'Riddle'}, {lastName: 'Marvolo'})
        e = "([lastName] = 'Riddle' OR [lastName] = 'Marvolo')"
        h.assertSql(p, e)

    it 'detects a SQL expression used as a column', () ->
        p = sql.predicate({ "Qty * Price": { ">": 100 } })
        exp = "Qty * Price > 100"
        h.assertSql(p, exp)


    it 'allows a SQL name in the RHS', () ->
        p = sql.predicate( { "LastName": sql.name("FirstName")} )
        exp = "[LastName] = [FirstName]"
        h.assertSql(p, exp)


    it 'allows explicit SQL tokens in the LHS', () ->
        p = sql.predicate([sql.expr("LEN(LastName)"), { ">": 10 }])
        exp = "LEN(LastName) > 10"
        h.assertSql(p, exp)


    it 'handles two objects ANDed', () ->
        p = sql.predicate({ age: 22, name: 'deividy' })
        p.and({ test: 123, testing: 1234 })

        exp = "([age] = 22 AND [name] = 'deividy'"
        exp += " AND ([test] = 123 AND [testing] = 1234))"

        h.assertSql(p, exp)


    it 'accepts where(object) followed by .or(object) then .and(object)', () ->
        p = sql.predicate({ age: 22, name: 'deividy' })
        p.or({ test: 123, testing: 1234 }).and({ login: 'root'})

        exp = "((([age] = 22 AND [name] = 'deividy') OR ([test] = 123 AND [testing] = 1234))"
        exp += " AND [login] = 'root')"

        h.assertSql(p, exp)


    it 'transforms JS array of numbers into SQL IN operator', () ->
        p = sql.predicate({ age: [22, 30, 40] , name: 'deividy' })
        exp = "([age] IN (22, 30, 40) AND [name] = 'deividy')"
        
        h.assertSql(p, exp)


    it 'transforms JS array of strings into SQL IN operator', () ->
        p = sql.predicate({ last: ['Granger', 'Baggins'] , name: 'Hermione' })
        exp = "([last] IN ('Granger', 'Baggins') AND [name] = 'Hermione')"
        
        h.assertSql(p, exp)


    it 'supports ad-hoc SQL operators like >=', () ->
        p = sql.predicate({ age: { '>=': 18 } , name: 'deividy' })
        exp = "([age] >= 18 AND [name] = 'deividy')"
        h.assertSql(p, exp)


    it 'supports SQL BETWEEN operator', () ->
        p = sql.predicate({ age: { 'between': [18, 23] } , name: 'deividy' })
        exp = "([age] BETWEEN 18 AND 23 AND [name] = 'deividy')"

        h.assertSql(p, exp)


    it 'supports multiple operators for a single column, plus .or() and .and()', () ->
        p = sql.predicate({ age: { ">": 18, "<": 25 }, name: 'deividy' })
        p.or({ test: { "between": [18,25] }, testing: 1234 }).and({ login: 'root'})

        exp = "((([age] > 18 AND [age] < 25 AND [name] = 'deividy') "
        exp += "OR ([test] BETWEEN 18 AND 25 AND [testing] = 1234)) "
        exp += "AND [login] = 'root')"

        h.assertSql(p, exp)


    it 'accepts a sql.or() term', () ->
        p = sql.predicate(sql.or({ age: 22, name: 'deividy' }, { age: 18, login: 'deividy' }))
        exp = "(([age] = 22 AND [name] = 'deividy') OR ([age] = 18 AND [login] = 'deividy'))"

        h.assertSql(p, exp)


    it 'can AND together two OR groups, and use parens appropriately', () ->
        p = sql.predicate(sql.or({ age: 22, name: 'deividy' }, { age: 18, login: 'deividy' }))
        p.and( sql.or({ login: 'test', pass: 12 }, { login: 'test123', pass: 123 }) )
        
        exp = "((([age] = 22 AND [name] = 'deividy') OR ([age] = 18 AND [login] = 'deividy')) "
        exp += "AND (([login] = 'test' AND [pass] = 12) OR ([login] = 'test123' AND [pass] = 123)))"
        h.assertSql(p, exp)


    it 'accepts raw SQL and can .and() it with another clause', () ->
        p = sql.predicate("id = 1 AND test = 2")
        p.and({ name: 'test' })

        exp = "((id = 1 AND test = 2) AND [name] = 'test')"
        h.assertSql(p, exp)


    it 'accepts raw SQL followed by .and() then .or()', () ->
        p = sql.predicate("id = 1 AND test = 2")
        p.and({ name: 'test' }).or("login = 'test'")
        exp = "(((id = 1 AND test = 2) AND [name] = 'test') OR (login = 'test'))"
        h.assertSql(p, exp)


    it 'accepts raw SQL and objects mixed in OR array', () ->
        p = sql.predicate(sql.or({id: 10, name: 'Deividy'}, "FOOBAR LIKE '%gonzo%'"))
        exp = "(([id] = 10 AND [name] = 'Deividy') OR (FOOBAR LIKE '%gonzo%'))"
        h.assertSql(p, exp)


    it 'protects against SQL injections', () ->
        p = sql.predicate({ login: "HAX0R '-- SELECT * FROM users" })

        exp = "[login] = 'HAX0R ''-- SELECT * FROM users'"
        h.assertSql(p, exp)

    it 'transforms JS null into sql NULL', ->
        p = sql.predicate( {name: null})
        exp = "[name] IS NULL"
        h.assertSql(p, exp)

)
