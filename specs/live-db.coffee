h = require('./test-helper')
DbSchema = h.requireSrc('schema/db-schema')
SqlFormatter = h.requireSrc('dialects/sql-formatter')
sql = h.requireSrc('sql')

data = require('./data/test-data.coffee')

cleanTestData = (cb) ->
    q = "DELETE Fighters
        DBCC CHECKIDENT('Fighters', RESEED, 0) "

    for f in data.fighters
        formatter = new SqlFormatter(h.getCookedSchema())
        insert = sql.insert('fighters', f)
        q += (formatter.format(insert) + " ")

    h.liveDb.noData(q, cb)

before (done) ->
    h.cleanTestData = cleanTestData

    h.connectToDb (freshDb) ->
        freshDb.utils.buildFullSchema (err, s) ->
            done(err) if err
            s = new DbSchema(s)
            h.cookSchema(s)
            freshDb.loadSchema(s)
            h.liveDb = h.db = freshDb
            cleanTestData(done)

describe 'Live DB test helper', () ->
    it 'Cleans test data', () ->
