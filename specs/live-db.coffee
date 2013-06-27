h = require('./test-helper')
DbSchema = h.requireSrc('schema/db-schema')
SqlFormatter = h.requireSrc('dialects/sql-formatter')
sql = h.requireSrc('sql')
ezekiel = h.requireSrc()

data = require('./data/test-data.coffee')

cleanTestData = (cb) ->
    q = """
        DELETE Fighters
        DBCC CHECKIDENT('Fighters', RESEED, 0)

        DELETE Events
        DBCC CHECKIDENT('Events', RESEED, 0)

        DELETE Promotions
        DBCC CHECKIDENT('Promotions', RESEED, 0)
    """

    formatter = new SqlFormatter(h.getCookedSchema())

    tables = [ 'fighters', 'promotions', 'events' ]
    for table in tables
        for d in data[table]
            insert = sql.insert(table, d)
            q += (formatter.format(insert) + " ")

    h.liveDb.noData(q, cb)

before (done) ->
    h.cleanTestData = cleanTestData

    config = {
        processSchema: h.cookSchema
        connection: h.defaultDbConfig
    }

    ezekiel.connect config, (err, freshDb) ->
        return done(err) if err?
        h.liveDb = h.db = freshDb
        cleanTestData(done)

describe 'Live DB test helper', () ->
    it 'Cleans test data', () ->
