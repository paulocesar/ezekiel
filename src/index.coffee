_ = require('more-underscore/src')

engines = require('./engines.json')
Database = require('./access/database')

# We don't want users to instantiate Database directly for a number of reasons, but the fundamental
# point is that we cannot guarantee a valid working instance without doing async work. Also, we may
# want to check things like db version before exposing certain functionality. Loading a schema is
# another thing some people want before they get their db instance.
#
# On the error front, we could have an instantaneous error (bad config) or an error when first
# trying to connect (requires async work). So new Database() is just not nice, it would make it
# a mess for the caller to correctly check for errors. We go for this little factory instead, and as
# a bonus we can implement methods that create a DB before connecting to it, stuff like that.
#
# Database can still be instantiated directly in tests and for advanced users.

e = ezekiel = {
    connect: (config, cb) ->
        # MUST: test that config is OK, test first connection, load schema when appropriate
        db = new Database(config)

        db.adapter = e.getAdapter(config)

        dialect = e.getDialect(config, db)
        db.utils = new dialect.Utils(db)
        db.Formatter = dialect.Formatter

        cb(null, db)

    getAdapter: (config) ->
        name = 'tedious'
        path = "./adapters/#{name}"
        adapter = require(path)
        return new adapter(config)

    getDialect: (config, db) ->
        name = config.dialect ? engines[config.engine].dialect
        path = "./dialects/#{name}"
        return require(path)
}

module.exports = ezekiel
ezekiel.sql = require('./sql')
