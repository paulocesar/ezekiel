_ = require('underscore')

boundFunctions = {
    scalar: (cb) -> @db.scalar(@, cb)
    tryScalar: (cb) -> @db.tryScalar(@, cb)
    oneRow: (cb) -> @db.oneRow(@, cb)
    tryOneRow: (cb) -> @db.tryOneRow(@, cb)
    allRows: (cb) -> @db.allRows(@, cb)

    oneObject: (typeOrCb, cbOrNull) -> @db.oneObject(@, typeOrCb, cbOrNull)
    allObjects: (typeOrCb, cbOrNull) -> @db.allObjects(@, typeOrCb, cbOrNull)
    array: (cb) -> @db.array(@, cb)
}

module.exports = {
    boundFunctions,

    bind: (q, db, defaultFn) ->
        bound = Object.create(q)
        bound.db = db
        _.extend(bound, boundFunctions)
        bound.run = bound[defaultFn] if defaultFn?
        return bound

    bindError: (q, err) ->
        b = Object.create(q)
        raiseError = (q, cb) -> cb(err)

        for name, f in boundFunctions
            b[name] = raiseError

        return b
}
