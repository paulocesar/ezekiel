h = require('../test-helper')
{ SqlToken, SqlExpression, SqlRawName, SqlFullName } = sql = h.requireSrc('sql')

describe('SqlToken', () ->
    it('Handles raw names and full names', ->
        sql.name('Some.Table').should.be.an.instanceOf(SqlRawName)
        sql.name(["SomeDb", "SomeSchema", "SomeTable"]).should.be.an.instanceOf(SqlFullName)
    )
)
