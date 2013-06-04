_ = require('underscore')
F = require('functoids/src')
{ SqlSelect } = require('../sql')

boundFunctions = {
    scalar: (cb) -> @db.scalar(@, cb)
    tryScalar: (cb) -> @db.tryScalar(@, cb)
    oneRow: (cb) -> @db.oneRow(@, cb)
    tryOneRow: (cb) -> @db.tryOneRow(@, cb)
    allRows: (cb) -> @db.allRows(@, cb)

    oneObject: (cb) -> @db.oneObject(@, @gw.sqlAlias, cb)
    allObjects: (cb) -> @db.allObjects(@, @gw.sqlAlias, cb)
    array: (cb) -> @db.array(@, cb)
}

class BoundSelect extends SqlSelect
    constructor: (@gw) -> super()

    toString: () -> @db.format(@)

    tryCall: (fnName, cb) ->
        fn = @[fnName]

        unless fn?
            F.throw("Cannot call unknown function #{fnName}")

        return if cb? then fn.call(@, cb) else @

_.extend(BoundSelect::, boundFunctions)

Object.defineProperty(BoundSelect::, "db", {
    get: () -> @gw.db
})

module.exports = {
    boundFunctions,

    BoundSelect,

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
