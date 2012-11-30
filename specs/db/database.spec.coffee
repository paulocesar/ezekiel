h = require('../test-helper')
fs = require('fs')

env = "development"
database = null

describe('Database', () ->
    it('should have a configuration object', () ->
        h.testConfig.databases.should.be.a('object')
    )

    it('should instantiate the Database class correctly', () ->
        database = h.getSharedDb()
        database.should.be.a('object')
    )

    it('should execute a simple query', (done) ->
        query = "SELECT 1"
        database.run(query, (data) ->
            done()
        )
    )

    it('should get query rows', (done) ->
        stmt = "SELECT 1 AS test"
        database.allRows(stmt, (err, data) ->
            data.should.eql([{test:1}])
            done()
        )
    )
)
