h = require('../test-helper')
sql = h.requireSrc('sql')
testDb = null

describe('Database using sql.* tokens', () ->
    before((done) ->
        h.connectToDb((db) ->
            testDb = db
            testDb.loadSchema(h.newAliasedSchema())
            done()
        )
    )

    it('Performs a SELECT query against Customers', (done) ->
        s = sql.select('CustomerId', 'CustomerFirstName').from('customer')
        testDb.allRows(s, (err, rows) ->
            if err?
                throw new Error(err)
            rows.should.be.instanceOf(Array)
            done()
        )
    )
)
