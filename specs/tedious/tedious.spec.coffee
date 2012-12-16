tedious = require('tedious')
{ _, async } = h = require('../test-helper')
TediousAdapter = h.requireSrc('adapters/tedious')

config = h.testConfig.databases.mssql
adapter = new TediousAdapter(config)

execute = (q, cb) ->
    adapter.execute({
        stmt: q,
        onError: cb,
        onAllRows: (rows) -> cb(null, rows)
    })

describe('tedious Adapter', () ->
    it('says the app database exists', (done) ->
        adapter.doesDatabaseExist(config.database, (db) ->
            db.should.be.true
            done()
        )
    )

    it 'can recover from connection errors', (done) ->
        async.series [
            (cb) -> execute('SELECT BAD SQL', (err, results) -> cb(null, err))
            (cb) -> execute("SELECT 'Bazinga' as result", cb)
        ], (err, results) ->
            results[1][0].result.should.eql('Bazinga')
            done(err)

    it('says an inexistent db does not exist', (done) ->
        adapter.doesDatabaseExist('Random3490', (db) ->
            db.should.be.false
            done()
        )
    )

    db_name = "test_db_#{Date.now()}"
    it('should create a database', (done) ->
        adapter.createDatabase(db_name, (dn) ->
            done()
        )
    )

    it('should check if the created database exists', (done) ->
        adapter.doesDatabaseExist(db_name, (db) ->
            db.should.be.ok
            done()
        )
    )

    it('should drop the created database', (done) ->
        adapter.dropDatabase(db_name, (dn) ->
            done()
        )
    )


    it('should handle requests in parallel', (done) ->
        async.parallel({
            one: (cb) -> execute("SELECT 1", cb)
            two: (cb) -> execute("SELECT 2", cb)
            three: (cb) -> execute("SELECT 3", cb)
        }, (err, results) ->
            done()
        )
    )
)
