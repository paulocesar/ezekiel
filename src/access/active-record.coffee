_ = require('underscore')
F = require('functoids/src')
async = require('async')

{ SqlToken } = sql = require('../sql')
queryBinder = require('./query-binder')

states = [
    {
        name: 'new'
        persist: (cb) -> @_gw.insertOne @_changed, @_makePersistHandler(cb)
        destroy: (cb) -> @throwBadStateFor('destroy')
    }
    {
        name: 'persisted'
        persist: (cb) -> @_gw.updateOne @_changed, @_persisted, @_makePersistHandler(cb)
        destroy: (cb) -> @_gw.deleteOne @_persisted, @_makeDestroyHandler(cb)
    }
    {
        name: 'destroyed'
        persist: (cb) -> @throwBadStateFor('persist')
        destroy: (cb) -> @throwBadStateFor('destroy')
    }
]

class ActiveRecord
    constructor: (@_gw, @_schema) ->
        for c in @_schema.columns
            @_addColumnAccessor(c)

        @_persisted = @_changed = null
        @_asyncProperties = {}

        @_n = 0

    # SHOULD: include id in toString()
    toString: () -> "<ActiveRecord for #{@_schema.one}, #{@_stateName()}>"
    _stateName: () -> states[@_n].name

    _addColumnAccessor: (c) ->
        key = c.property
        return if key of @

        Object.defineProperty(@, key, {
            get: () -> @get(key)
            set: (v) -> @set(key, v)
        })

    loadAsyncProperties: (properties..., callback) ->
        F.demandGoodArray(properties, 'properties')
        F.demandFunction(callback, 'callback')

        tasks = {}
        for property in properties
            return callback("Invalid property #{property}") if !(@_asyncProperties[property]?)
            do (property) =>
                tasks[property] = (data..., cb) => @getAsync(property, cb)
        
        async.series tasks, callback
    
    setPersisting: (data, callback) ->
        F.demandGoodObject(data, 'data')
        F.demandFunction(callback, 'callback')

        tasks = [ ]
        for key, value of data
            if (@_asyncProperties[key]?.set?)
                do (key, value) =>
                    tasks.push (data..., cb) => @[key](value, cb)
                continue

            @[key] = value
        
        if !(@_isDirty())
            # MUST: use series instead of waterfall, remove data... from callback
            # definition
            return async.waterfall(tasks, callback)

        @persist (err) ->
            return callback(err) if err
            async.waterfall(tasks, callback)

    defineAsyncProperty: (key, property) ->
        F.demandGoodString(key, 'key')
        F.demandGoodObject(property, 'property')
    
        @_asyncProperties[key] = property

        # SHOULD: probably remove Object.defineProperty, I don't think we need the async property as
        # an actual JS property
        # SHOULD: demand a getter
        Object.defineProperty(@, key, {
            configurable: property.configurable ? false
            enumerable: property.enumerable ? false
            writable: property.writable ? false

            value: (values..., callback) ->
                return @getAsync(key, callback) if _.isEmpty(values)
                return @setAsync(key, values, callback)
        })

    ignoreForBulk: (columns, ignore = true) ->
        @_schema.ignoreForBulk(columns, ignore)

    # SHOULD: merge getAsync() and get()
    getAsync: (key, callback) ->
        F.demandGoodString(key, 'key')
        F.demandFunction(callback, 'callback')

        if !_.isFunction(@_asyncProperties[key].get)
            return callback("Getter for #{key} not implemented")

        @_asyncProperties[key].get.call(@, callback)

    setAsync: (key, values, callback) ->
        F.demandGoodString(key, 'key')
        F.demandGoodArray(values, 'valeus')
        F.demandFunction(callback, 'callback')

        if !_.isFunction(@_asyncProperties[key].set)
            return callback("Setter for #{key} not implemented")

        @_asyncProperties[key].set.apply(@, values.concat [ callback ])

    get: (key) -> @_changed[key] ? @_persisted[key]
      
    set: (key, value) ->
      if @_persisted[key] == value
        delete @_changed[key]
        return

      @_changed[key] = value
      return @

    # SHOULD: handle async properties here
    setMany: (o) ->
      @set(k, v) for k, v of o
      return @

    _new: (@_gw, data) ->
        @_n = 0
        @_persisted = {}
        @_changed = {}
        @setMany(data) if data?

    _load: (@_gw, data) ->
        @_n = 1
        @_persisted = data
        @_changed = {}
        return @

    throwBadStateFor: (op) ->
      F.throw("#{@} cannot do operation #{op} in state #{@_stateName()}.")

    _makePersistHandler: (cb) ->
        (err, outputValues) =>
            return cb(err) if err

            @_persisted = _.extend(@_persisted, @_changed, outputValues)
            @_n = 1
            @_changed = {}
            cb(null, @)

    _makeDestroyHandler: (cb) ->
        (err) =>
            return cb(err) if err
            @_n = 2
            cb()

    setPersisted: (data) ->
        F.demandNonEmptyObject(data, 'data')

        @throwBadStateFor('setPersisted') if @_n == 2

        unless @_schema.coversSomeKey(data)
            F.throw("Argument 'data' does not cover any keys in #{@_schema}")

        @_persisted = data
        @_n = 1
        return @

    _isDirty: () -> @_n == 0 || !_.isEmpty(@_changed)

    # SHOULD: consider calling cb in next tick, so that we always look async
    # to the caller
    persist: (cb) ->
        return cb(null, @) unless @_isDirty()
        states[@_n].persist.call(@, cb)

    upsert: (cb) ->
        return cb(null, @) unless @_isDirty()
        states[@_n].upsert.call(@, cb)

    destroy: (cb) -> states[@_n].destroy.call(@, cb)

    toJSON: () ->
        return @_persisted if _.isEmpty(@_changed)
        return _.extend({}, @_persisted, @_changed)

Object.defineProperty(ActiveRecord::, "_db", {
    get: () -> @_gw.db
})

Object.defineProperty(ActiveRecord::, "_ctx", {
    get: () -> @_gw.db.context
})


module.exports = ActiveRecord
