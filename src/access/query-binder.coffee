_ = require('more-underscore/src')

dbObjects = require('./index')
{ DbObject, Table, Column, Key, ForeignKey } = dbObjects
{ SqlToken } = require('../sql')

boundFunctions = {
    scalar: (cb) -> @db.scalar(@, cb)
    tryScalar: (cb) -> @db.tryScalar(@, cb)
    oneRow: (cb) -> @db.oneRow(@, cb)
    tryOneRow: (cb) -> @db.tryOneRow(@, cb)
    array: (cb) -> @db.array(@, cb)
}

module.exports = {
    boundFunctions,

    bind: (q, db, defaultFn) ->
        bound = Object.create(q)
        bound.db = db
        _.extend(bound, boundFunctions)
        bound.run = b[defaultFn] if defaultFn?
        return bound

    bindError: (q, err) ->
        b = Object.create(q)
        raiseError = (q, cb) -> cb(err)

        for name, f in boundFunctions
            b[name] = raiseError

        return b
}
