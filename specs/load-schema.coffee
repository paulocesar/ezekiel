h = require('./test-helper')

before (done) ->
    h.connectToDb (freshDb) ->
        freshDb.utils.buildFullSchema (err, s) ->
            done(err) if err
            freshDb.loadSchema(s)
            h.db = freshDb
            done()
