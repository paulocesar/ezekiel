_ = require('more-underscore/src')

{ SqlToken } = sql = require('../sql')
queryBinder = require('./query-binder')

class TableGateway
    constructor: (@db, @schema) ->

    handle: () -> @schema.many
    toString: () -> "<TableGateway to #{@handle()}>"

    findOne: () -> @doOne(@_find, arguments, 'find')
    deleteOne: () -> @doOne(@_delete, arguments, 'delete')

    _find: (predicate, cb) ->
        q = sql.from(@handle()).where(predicate)
        return @db.bindOrCall(q, 'oneRow', cb)

    _delete: (predicate, cb) ->
        s = sql.delete(@handle(), predicate)
        return @db.bindOrCall(s, 'noData', cb)

    doOne: (fn, args, opName, queryArgument = null) ->
        cb = _.lastIfFunction(args)
        keyValues = _.unwrapArgs(args, cb?)

        unless keyValues?
            throw new Error('You must provide key values as arguments to #{opName}One()')

        if _.isObject(keyValues)
            covered = @schema.coversSomeKey(keyValues)
            if covered
                return fn.call(@, keyValues, cb, queryArgument)
            else
                e = "Could not find a key in #{@schema} whose values are fully specified in " +
                    "#{keyValues}. If you want to work on multiple rows, please use " +
                    "#{opName}Many()"
                return @bindError(e, cb)

        keys = @schema.getKeysWithShape(keyValues)

        if keys.length == 0
            e = "Could not find viable key in #{@schema} to be compared against " +
                "values #{keyValues}"
            return @bindError(e, cb)
        else if keys.length > 1
            e = "More than one key in #{@schema} can be compared against values #{keyValues}"
            return @bindError(e, cb)

        predicate = keys[0].wrapValues(keyValues)
        return fn.call(@, predicate, cb, queryArgument)

    insertOne: (values, cb = null) ->
        throw new Error('You must provide a values object') unless values?

        # MUST: inspect table, see if there's an identity column, act appropriately to retrieve
        # newly inserted identity. Check if a value for a read-only column was given and treat it
        # as error.

        q = sql.insert(@handle(), values)
        return @db.bindOrCall(q, 'noData', cb)

    updateOne: (updateValues, args...) ->
        unless _.isObject(updateValues)
            e = "The first argument to updateOne() must be an object containing the values " +
                "to be updated in #{@schema}"
            throw new Error(e)

        cb = _.lastIfFunction(args)
        cntKeyValues = if cb? then args.length - 1 else args.length
        if (cntKeyValues > 0)
            return @doOne(@_update, args, 'update', updateValues)

        # Ok, now we have work. Caller was lazy and threw us just one object, which must
        # has keys along with values being updated. We need to separate keys from values.
        #
        # If the caller provides the value for a read-only DB key (eg, an identity column), then
        # that key is used as the only predicate, and we try to update everything else - even
        # a value passed in for a non-read-only unique key.  This is nice because it's common to
        # have a read-only identity PK, but a UNIQUE constraint on some other fields that might be
        # changed every once in a while. This logic lets callers handle that pretty easily.

        throw new Error("we don't like work")

    _update: (predicate, cb, values) ->
        s = sql.update(@handle(), values, predicate)
        return @db.bindOrCall(s, 'noData', cb)

    count: (cb = null) ->
        q = sql.from(@handle()).select(sql.count(1))
        return @db.bindOrCall(q, 'scalar', cb)

    bindError: (msg, cb) ->
        return cb(msg) if cb?
        return queryBinder.bindError(@, msg)
    
module.exports = TableGateway
