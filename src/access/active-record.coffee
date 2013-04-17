_ = require('underscore')
F = require('functoids/src')

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

    get: (key) -> @_changed[key] ? @_persisted[key]
      
    set: (key, value) ->
      if @_persisted[key] == value
        delete @_changed[key]
        return

      @_changed[key] = value
      return @

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

Object.defineProperty(ActiveRecord::, "_db", {
    get: () -> @_gw.db
})

Object.defineProperty(ActiveRecord::, "_ctx", {
    get: () -> @_gw.db.context
})


module.exports = ActiveRecord
