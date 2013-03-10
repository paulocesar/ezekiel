_ = require('more-underscore/src')

{ SqlToken } = sql = require('../sql')
queryBinder = require('./query-binder')

class TableGateway
    constructor: (@db, @schema) ->

    findOne: () ->
        cb = _.lastIfFunction(arguments)
        keyValues = _.unwrapArgs(arguments, cb?)
         
        q = sql.from(@schema.many)

        if _.isObject(keyValues)
            q.where(keyValues)
        else
            keys = @schema.getKeysWithShape(keyValues)
            if keys.length == 0
                error = "Could not find viable key in #{@schema} " +
                    "to be compared against values #{keyValues}"
                return @doError(error, cb)
            else if keys.length > 1
                error = "More than one key in #{@schema} " +
                    " can be compared against values #{keyValues}"
                return @doError(error, cb)

            q.where(keys[0].wrapValues(keyValues))

        return @bindOrCall(q, 'oneRow', cb)

    bindOrCall: (q, fn, cb) ->
        return @db[fn](q, cb) if cb?
        return queryBinder.bind(q, @db, fn)

    doError: (msg, cb) ->
        return cb(msg) if cb?
        return queryBinder.bindError(@, msg)
    

module.exports = TableGateway
