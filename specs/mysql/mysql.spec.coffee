h = require('../test-helper')

config = h.testConfig.databases.mysql
adapter = null

describe('Mysql Adapter', () ->
    it('Should instantiate succesfully', () ->
        mysql = h.requireSrc('adapters/mysql')
        adapter = new mysql(config)
    )

    it('says the app db exists', (done) ->
        adapter.doesDatabaseExist(config.database, (db) ->
            db.should.be.true
            done()
        )
    )

    it('says an inexistent db does not exist', (done) ->
        adapter.doesDatabaseExist('RandomEasterEgg', (db) ->
            db.should.be.false
            done()
        )
    )

)