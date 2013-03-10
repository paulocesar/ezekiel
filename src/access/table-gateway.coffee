_ = require('more-underscore/src')

{ SqlToken } = sql = require('../sql')
queryBinder = require('./query-binder')

class TableGateway
    constructor: (@db, @schema) ->

    handle: () -> @schema.many
    toString: () -> "#{@handle()} TableGateway"

    findOne: () ->
        cb = _.lastIfFunction(arguments)
        keyValues = _.unwrapArgs(arguments, cb?)
        throw new Error('You must provide key values') unless keyValues?
         
        q = sql.from(@handle())

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

        return @db.bindOrCall(q, 'oneRow', cb)

    insertOne: (values, cb = null) ->
        throw new Error('You must provide a values object') unless values?

        q = sql.insert(@handle(), values)
        # MUST: inspect table, see if there's an identity column, act appropriately to retrieve
        # newly inserted identity.
        return @db.bindOrCall(q, 'noData', cb)
         
    count: (cb = null) ->
        q = sql.from(@handle()).select(sql.count(1))
        return @db.bindOrCall(q, 'scalar', cb)

    doError: (msg, cb) ->
        return cb(msg) if cb?
        return queryBinder.bindError(@, msg)
    

module.exports = TableGateway
