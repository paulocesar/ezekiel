h = require('../test-helper')
ezekiel = h.requireSrc()


failConnection = {
    engine: 'mssql'
    host: '127.0.0.1'
    port: '15909'
    userName: 'foo'
    password: 'bar'
    database: 'NoneReally'
    pooling: false
}


describe 'Ezekiel', () ->
    it 'throws error on failed connection', (done) ->
        ezekiel.connect(failConnection, (err, db) ->
            db.scalar("SELECT 42", (err, r) ->
                console.log(err)
                done()
            )
        )
        
