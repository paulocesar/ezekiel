h = require('../test-helper')

{ SqlPredicate } = sql = h.requireSrc('sql')
SqlFormatter = h.requireSrc('dialects/sql-formatter')

sys = require('sys')

f = new SqlFormatter()

assert = (sqlWhere, expected) ->
    ret = sqlWhere.toSql(f)
    ret.should.eql(expected)

describe('SqlPredicate', () ->
    it('builds a predicate from an object', ->
        p = SqlPredicate.wrap({firstName: 'Ron', lastName: 'Weasley'})
    )

    it('detects a SQL expression used as a column', () ->
        p = sql.predicate({ "Qty * Price": { ">": 100 } })
        exp = "Qty * Price > 100"
        assert(p, exp)
    )

    it('allows a SQL name in the RHS', () ->
        p = sql.predicate( { "LastName": sql.name("FirstName")} )
        exp = "[LastName] = [FirstName]"
        assert(p, exp)
    )

    it('allows explicit SQL tokens in the LHS', () ->
        p = sql.predicate([sql.expr("LEN(LastName)"), { ">": 10 }])
        exp = "LEN(LastName) > 10"
        assert(p, exp)
    )

    it('handles two objects ANDed', () ->
        p = sql.predicate({ age: 22, name: 'deividy' })
        p.and({ test: 123, testing: 1234 })

        exp = "([age] = 22 AND [name] = 'deividy'"
        exp += " AND ([test] = 123 AND [testing] = 1234))"

        assert(p, exp)
    )

    it('accepts where(object) followed by .or(object) then .and(object)', () ->
        p = sql.predicate({ age: 22, name: 'deividy' })
        p.or({ test: 123, testing: 1234 }).and({ login: 'root'})

        exp = "((([age] = 22 AND [name] = 'deividy') OR ([test] = 123 AND [testing] = 1234))"
        exp += " AND [login] = 'root')"

        assert(p, exp)
    )

    it('transforms JS array of numbers into SQL IN operator', () ->
        p = sql.predicate({ age: [22, 30, 40] , name: 'deividy' })
        exp = "([age] IN (22, 30, 40) AND [name] = 'deividy')"
        
        assert(p, exp)
    )

    it('transforms JS array of strings into SQL IN operator', () ->
        p = sql.predicate({ last: ['Granger', 'Baggins'] , name: 'Hermione' })
        exp = "([last] IN ('Granger', 'Baggins') AND [name] = 'Hermione')"
        
        assert(p, exp)
    )

    it('supports ad-hoc SQL operators like >=', () ->
        p = sql.predicate({ age: { '>=': 18 } , name: 'deividy' })
        exp = "([age] >= 18 AND [name] = 'deividy')"
        assert(p, exp)
    )

    it('supports SQL BETWEEN operator', () ->
        p = sql.predicate({ age: { 'between': [18, 23] } , name: 'deividy' })
        exp = "([age] BETWEEN 18 AND 23 AND [name] = 'deividy')"

        assert(p, exp)
    )

    it('supports multiple operators for a single column, plus .or() and .and()', () ->
        p = sql.predicate({ age: { ">": 18, "<": 25 }, name: 'deividy' })
        p.or({ test: { "between": [18,25] }, testing: 1234 }).and({ login: 'root'})

        exp = "((([age] > 18 AND [age] < 25 AND [name] = 'deividy') "
        exp += "OR ([test] BETWEEN 18 AND 25 AND [testing] = 1234)) "
        exp += "AND [login] = 'root')"

        assert(p, exp)
    )

    it('accepts a sql.or() term', () ->
        p = sql.predicate(sql.or({ age: 22, name: 'deividy' }, { age: 18, login: 'deividy' }))
        exp = "(([age] = 22 AND [name] = 'deividy') OR ([age] = 18 AND [login] = 'deividy'))"

        assert(p, exp)
    )

    it('can AND together two OR groups, and use parens appropriately', () ->
        p = sql.predicate(sql.or({ age: 22, name: 'deividy' }, { age: 18, login: 'deividy' }))
        p.and( sql.or({ login: 'test', pass: 12 }, { login: 'test123', pass: 123 }) )
        
        exp = "((([age] = 22 AND [name] = 'deividy') OR ([age] = 18 AND [login] = 'deividy')) "
        exp += "AND (([login] = 'test' AND [pass] = 12) OR ([login] = 'test123' AND [pass] = 123)))"
        assert(p, exp)
    )

    it('accepts raw SQL and can .and() it with another clause', () ->
        p = sql.predicate("id = 1 AND test = 2")
        p.and({ name: 'test' })

        exp = "((id = 1 AND test = 2) AND [name] = 'test')"
        assert(p, exp)
    )

    it('accepts raw SQL followed by .and() then .or()', () ->
        p = sql.predicate("id = 1 AND test = 2")
        p.and({ name: 'test' }).or("login = 'test'")
        exp = "(((id = 1 AND test = 2) AND [name] = 'test') OR (login = 'test'))"
        assert(p, exp)
    )

    it('accepts raw SQL and objects mixed in OR array', () ->
        p = sql.predicate(sql.or({id: 10, name: 'Deividy'}, "FOOBAR LIKE '%gonzo%'"))
        exp = "(([id] = 10 AND [name] = 'Deividy') OR (FOOBAR LIKE '%gonzo%'))"
        assert(p, exp)
    )

    it('protects against SQL injections', () ->
        p = sql.predicate({ login: "HAX0R '-- SELECT * FROM users" })

        exp = "[login] = 'HAX0R ''-- SELECT * FROM users'"
        assert(p, exp)
    )
)
