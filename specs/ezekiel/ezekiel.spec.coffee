h = require('../test-helper')
ezekiel = h.requireSrc()
should = require('should')
path = require('path')

failConnection = {
    engine: 'mssql'
    host: '127.0.0.1'
    port: '15909'
    userName: 'foo'
    password: 'bar'
    database: 'NoneReally'
    pooling: false
}

conn = h.defaultDbConfig
fn = () ->

fighter = null

specRoot = path.resolve(__dirname, '..')

badConfigs = [
    [ null, /a hash/ ]
    [ {}, /a hash/ ]
    [ { schema: true }, /error: missing DB connection/ ]
    [ { host: 'foo' }, /errors: missing userName, missing password/ ]
    [ { connection: conn, processSchema: 'foobar' }, /processSchema/ ]
    [ { connection: conn, schema: false, processSchema: fn },
        /processSchema cannot be used/ ]
]


describe 'Ezekiel', () ->
    it 'throws error on bad configurations', () ->
        for c in badConfigs
            config = c[0]
            msg = c[1]
            f = () -> ezekiel.connect config, (db) ->
            f.should.throw(msg)

    it 'loads schema by default', (done) ->
        ezekiel.connect conn, (err, db) ->
            return done(err) if err
            h.assertLoadedSchema(db.schema, done)

    it 'requires active record and gateway modules', (done) ->
        config = { connection: conn, require: specRoot, processSchema: h.cookSchema }

        ezekiel.connect config, (err, db) ->
            return done(err) if err
            fighter = db.fighter( { lastName: 'Machida', firstName: 'Lyoto' })
            fighter.sayHi().should.eql("Hi, my name is Lyoto Machida")
            h.assertLoadedSchema(db.schema, done)

    it 'sends error if require path is wrong', (done) ->
        config = { connection: conn, require: "/foobar5280", processSchema: h.cookSchema }

        ezekiel.connect config, (err, db) ->
            err.should.match(/ENOENT/)
            done()


    it 'test async get property', (done) ->
        fighter.fullName (fullName) ->
            fullName.should.be.equal("Lyoto Machida")
            done()

    it 'test async set property', (done) ->
        fighter.fullName { firstName: 'Anderson', lastName: 'Silva' }, (err) ->
            fighter.sayHi().should.eql("Hi, my name is Anderson Silva")
            done()
            

