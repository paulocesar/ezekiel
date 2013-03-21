_ = require('underscore')
F = require('functoids/src')

{ SqlToken } = sql = require('../sql')
queryBinder = require('./query-binder')

class TableGateway
    constructor: (@db, @schema) ->

    sqlAlias: () -> @schema.many
    toString: () -> "<TableGateway to #{@sqlAlias()}>"

    findOne: () -> @doOne(@_find, arguments, 'find')
    deleteOne: () -> @doOne(@_delete, arguments, 'delete')

    _find: (predicate, cb) ->
        q = sql.from(@sqlAlias()).where(predicate)
        return @db.bindOrCall(q, 'oneObject', cb)

    _delete: (predicate, cb) ->
        s = sql.delete(@sqlAlias(), predicate)
        return @db.bindOrCall(s, 'noData', cb)

    doOne: (fn, args, opName, queryArgument = null) ->
        cb = F.lastIfFunction(args)
        keyValues = F.unwrapArgs(args, cb?)

        unless keyValues?
            throw new Error('doOne: You must provide key values as arguments to #{opName}One()')

        if _.isObject(keyValues)
            covered = @schema.coversSomeKey(keyValues)
            if covered
                return fn.call(@, keyValues, cb, queryArgument)
            else
                e = "doOne: Could not find a key in #{@schema} whose values are fully specified " +
                    "in #{keyValues}. If you want to work on multiple rows, please use " +
                    "#{opName}Many()"
                return @bindError(e, cb)

        keys = @schema.getKeysWithShape(keyValues)

        if keys.length == 0
            e = "doOne: Could not find viable key in #{@schema} to be compared against " +
                "values #{keyValues}"
            return @bindError(e, cb)
        else if keys.length > 1
            e = "doOne: Multiple keys in #{@schema} can be compared against values #{keyValues}"
            return @bindError(e, cb)

        predicate = keys[0].wrapValues(keyValues)
        return fn.call(@, predicate, cb, queryArgument)

    insertOne: (values, cb = null) ->
        throw new Error('insertOne: You must provide a values object') unless values?

        @schema.demandInsertable(values)

        q = sql.insert(@sqlAlias(), values)
        if @schema.hasReadOnly()
            q.output(@schema.readOnlyProperties())
            fn = 'oneRow'
        else
            fn = 'noData'

        return @db.bindOrCall(q, fn, cb)

    updateOne: (updateValues, args...) ->
        unless _.isObject(updateValues)
            e = "updateOne: The first argument to updateOne() must be an object containing the " +
                "values to be updated in #{@schema}"
            throw new Error(e)

        cb = F.lastIfFunction(args)
        cntKeyValues = if cb? then args.length - 1 else args.length
        if (cntKeyValues > 0)
            return @doOne(@_update, args, 'update', updateValues)

        # Ok, now we have work. The caller was lazy and threw us just one object, which must
        # have keys along with values being updated. We need to separate keys from values.
        #
        # If the caller provides the value for a read-only DB key (eg, an identity column), then
        # that key is used as the only predicate, and we try to update everything else - even
        # a value passed in for a non-read-only unique key.  This is nice because it's common to
        # have a read-only identity PK, but a UNIQUE constraint on some other fields that might be
        # changed every once in a while. This logic lets callers handle that pretty easily.

        throw new Error("we don't like work")

    _update: (predicate, cb, values) ->
        s = sql.update(@sqlAlias(), values, predicate)
        return @db.bindOrCall(s, 'noData', cb)

    selectMany: (predicate, cb) ->
        q = sql.from(@sqlAlias()).where(predicate)
        return @db.bindOrCall(q, 'allObjects', cb)

    count: (cb = null) ->
        q = sql.from(@sqlAlias()).select(sql.count(1))
        return @db.bindOrCall(q, 'scalar', cb)

    bindError: (msg, cb) ->
        return cb(msg) if cb?
        return queryBinder.bindError(@, msg)
    
module.exports = TableGateway
