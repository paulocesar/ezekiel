class DatabaseUtils
    constructor: (@db) ->

    dbNow: (callback) -> @db.scalar(@stmts.dbNow, callback)
    dbUtcNow: (callback) -> @db.scalar(@stmts.dbUtcNow, callback)
    dbUtcOffset: (callback) -> @db.scalar(@stmts.dbUtcOffset, callback)

module.exports = DatabaseUtils
