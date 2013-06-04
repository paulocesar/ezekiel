_ = require('underscore')
F = require('functoids/src')

{ SqlToken } = sql = require('../sql')
{ BoundSelect } = queryBinder = require('./query-binder')

class TableGateway
    constructor: (@db, @schema, @arProto) ->
        @selectClass = class extends BoundSelect

    toString: () -> "<TableGateway to #{@sqlAlias}>"

    newObject: (data) ->
        ar = Object.create(@arProto)
        ar._new(@, data)
        return ar

    _new: (@db) ->

    attach: (data) ->
        if _.isArray(data)
            return (@_attachOne(r) for r in data)
        else
            return @_attachOne(data)

    _attachOne: (data) ->
        ar = Object.create(@arProto)
        ar._load(@, data)
        return ar

    selectOne: (predicate, cb) -> @doOne(@_select, arguments, "select")
    selectMany: (predicate, cb) -> @_query(predicate, "allRows", cb)
    _select: (predicate, cb) -> @_query(predicate, "oneRow", cb)

    findOne: () -> @doOne(@_find, arguments, "find")
    findMany: (predicate, cb) -> @_query(predicate, "allObjects", cb)
    _find: (predicate, cb) -> @_query(predicate, "oneObject", cb)

    deleteOne: () -> @doOne(@_delete, arguments, 'delete')

    _delete: (predicate, cb) ->
        s = sql.delete(@sqlAlias, predicate)
        return @db.bindOrCall(s, 'noData', cb)

    doOne: (fn, args, opName, queryArgument) ->
        cb = F.lastIfFunction(args)
        keyValues = F.unwrapArgs(args, cb?)

        unless keyValues?
            F.throw("You must provide key values as arguments to #{opName}One()")

        if _.isObject(keyValues)
            covered = @schema.coversSomeKey(keyValues)
            if covered
                return fn.call(@, keyValues, cb, queryArgument)
            else
                e = ["Could not find a key in #{@schema} whose values are fully specified"
                    "in #{keyValues}. If you want to work on multiple rows, please use"
                    "#{opName}Many()"].join(' ')
                return @bindError(e, cb)

        keys = @schema.getKeysWithShape(keyValues)

        if keys.length == 0
            e = ["Could not find viable key in #{@schema} to be compared against"
                "values #{keyValues}"].join(' ')
            return @bindError(e, cb)
        else if keys.length > 1
            e = "Multiple keys in #{@schema} can be compared against values #{keyValues}"
            return @bindError(e, cb)

        predicate = keys[0].wrapValues(keyValues)
        return fn.call(@, predicate, cb, queryArgument)

    insertOne: (values, cb) ->
        F.demandNonEmptyObject(values, 'values')

        @schema.demandInsertable(values)

        q = sql.insert(@sqlAlias, values)
        return @doOutputQuery(q, cb)

    upsertOne: (values, cb) ->
        F.demandNonEmptyObject(values, 'values')

        canInsert = @schema.canInsert(values)
        mergeKey = @schema.getBestKeyForMerging(values)

        unless canInsert || mergeKey?
            e = "Cannot upsert values into #{@schema}: " + @schema.getMergeErrors(values)
            return @bindError(e, cb)

        if canInsert && !mergeKey?
           return @insertOne(values, cb)

        if mergeKey? && !canInsert
            return @updateOne(values, cb)

        keyProperties = mergeKey.properties()
        q = sql.upsert(@sqlAlias, values, keyProperties)
        return @doOutputQuery(q, cb)

    doOutputQuery: (q, cb) ->
        if @schema.hasReadOnly()
            q.output(@schema.readOnlyProperties())
            fn = 'oneRow'
        else
            fn = 'noData'

        return @db.bindOrCall(q, fn, cb)

    # updateOne has two usages:
    #
    # updateOne(updateValues, predicate, cb), which separates what's being
    # updated from the values to be used in the WHERE clause
    #
    # updateOne(data, cb), which gives us everything in one object, forcing us
    # to look at the schema and figure out what needs to be done
    #
    # This makes it a little different from the other one() methods
    updateOne: (updateValues, args...) ->
        F.demandNonEmptyObject(updateValues, 'updateValues',
            "be an object containing the values to be updated in #{@schema}")

        cb = F.lastIfFunction(args)
        cntKeyValues = if cb? then args.length - 1 else args.length
        if (cntKeyValues > 0)
            return @doOne(@_update, args, 'update', updateValues)

        # Ok, now we have work. The caller was lazy and threw us just one object, which must
        # have keys along with values being updated. We need to separate keys from values.
        key = @schema.getBestKeyForMerging(updateValues)
        unless key?
            e = ["When passing a single object to updateOne(), it must include values for"
                "at least one key in #{@schema}"].join(' ')
            return @bindError(e, cb)

        keyValues = key.wrapValues(updateValues)
        key.deleteValues(updateValues)

        @_update(keyValues, cb, updateValues)

    _update: (predicate, cb, values) ->
        s = sql.update(@sqlAlias, values, predicate)
        return @db.bindOrCall(s, 'noData', cb)

    deleteMany: (predicate, cb) ->
        s = sql.delete(@sqlAlias).where(predicate)
        return @db.bindOrCall(s, 'noData', cb)

    count: (cb = null) -> @newSelect().select(sql.count(1)).tryCall('scalar', cb)

    all: (cb) -> @newSelect().tryCall('allObjects', cb)

    where: (clause, cb) -> @_query(clause, 'allObjects', cb)
    _query: (predicate, fnName, cb) -> @newSelect().where(predicate).tryCall(fnName, cb)

    newSelect: () ->
        q = new @selectClass(@)
        return q.from(@sqlAlias).select(sql.star(@sqlAlias))

    merge: (data, cb) ->
        F.demandArray(data, 'data')
        cb(null) if _.isEmpty(data)

        s = sql.merge(@sqlAlias).using(data)
        return @db.bindOrCall(s, 'noData', cb)

    bindError: (msg, cb) ->
        return cb(msg) if cb?
        return queryBinder.bindError(@, msg)

Object.defineProperty(TableGateway::, "sqlAlias", {
    get: () -> @schema.many
})
    
module.exports = TableGateway
