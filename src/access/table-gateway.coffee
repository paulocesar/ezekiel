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
                return @bindError(error, cb)
            else if keys.length > 1
                error = "More than one key in #{@schema} " +
                    " can be compared against values #{keyValues}"
                return @bindError(error, cb)

            q.where(keys[0].wrapValues(keyValues))

        return @db.bindOrCall(q, 'oneRow', cb)

    insertOne: (values, cb = null) ->
        throw new Error('You must provide a values object') unless values?

        q = sql.insert(@handle(), values)
        # MUST: inspect table, see if there's an identity column, act appropriately to retrieve
        # newly inserted identity.
        return @db.bindOrCall(q, 'noData', cb)

    updateOne: (values) ->
        throw new Error('You must provide update values') unless values?
        cb = _.lastIfFunction(arguments)
        predicate = arguments[1] if (cb? && cb != arguments[1])

        if predicate?
            covered = @schema.coversSomeKey(predicate)
            return @demandCoverage(values, 'updateMany', cb) unless covered

            s = sql.update(@handle(), values, predicate)
            return @db.bindOrCall(s, 'noData', cb)

        # Ok, now we have work. Caller was lazy and just threw us just one object, which hopefully
        # has keys along with values being updated. If the caller provides the value for a read-only
        # DB key (eg, an identity column), then that key is used as the only predicate, and
        # we try to update everything else - even a value passed in for a non-read-only unique key.
        # This is nice because it's common to have a read-only identity PK, but a UNIQUE constraint
        # on some other fields that might be changed every once in a while. This logic lets callers
        # handle that pretty easily.

        return # we don't like work
         
    count: (cb = null) ->
        q = sql.from(@handle()).select(sql.count(1))
        return @db.bindOrCall(q, 'scalar', cb)

    demandCoverage: (values, suggestion, cb) ->
        e = "Could not find a key in #{@schema} whose values are fully specified in " +
            "#{values}. If you want to work on multiple rows, please use #{suggestion}()".
        return @bindError(e, cb)

    bindError: (msg, cb) ->
        return cb(msg) if cb?
        return queryBinder.bindError(@, msg)
    
module.exports = TableGateway
