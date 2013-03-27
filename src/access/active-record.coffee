_ = require('underscore')
F = require('functoids/src')

{ SqlToken } = sql = require('../sql')
queryBinder = require('./query-binder')

states = [
    {
        name: 'unknown'
        insert: (cb) -> @_gw.insertOne @_changed, @_makePersistHandler(cb)
        update: (cb) -> @_gw.updateOne @_changed, @_makePersistHandler(cb)
        upsert: (cb) -> @_gw.upsertOne @_changed, @_makePersistHandler(cb)
        delete: (cb) -> @_gw.deleteOne @_changed, @_makeDeleteHandler(cb)
    }
    {
        name: 'persisted'
        insert: (cb) -> @throwBadStateFor('insert')
        update: (cb) -> @_gw.updateOne @_changed, @_persisted, @_makePersistHandler(cb)
        upsert: (cb) -> @_gw.updateOne @_changed, @_persisted, @_makePersistHandler(cb)
        delete: (cb) -> @_gw.deleteOne @_persisted, @_makeDeleteHandler(cb)
    }
    {
        name: 'new'
        insert: (cb) -> @_gw.insertOne @_changed, @_makePersistHandler(cb)
        update: (cb) -> @throwBadStateFor('update')
        upsert: (cb) -> @_gw.insertOne @_changed, @_makePersistHandler(cb)
        delete: (cb) -> @throwBadStateFor('delete')
    }
    {
        name: 'deleted'
        insert: (cb) -> @throwBadStateFor('insert')
        update: (cb) -> @throwBadStateFor('update')
        upsert: (cb) -> @throwBadStateFor('upsert')
        delete: (cb) -> @throwBadStateFor('delete')
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

    attach: (gw, s) ->
        @_gw = gw
        @_init()

    _init: () ->
        @_n = 0
        @_persisted = {}
        @_changed = {}

    throwBadStateFor: (op) ->
      F.throw("#{@} cannot do operation #{op} in state #{@_stateName()}.")

    _makePersistHandler: (cb) ->
        (err, outputValues) =>
            return cb(err) if err

            @_persisted = _.extend(@_persisted, @_changed, outputValues)
            @_n = 1
            @_changed = {}
            cb(null, @)

    _makeDeleteHandler: (cb) ->
        (err) =>
            return cb(err) if err
            @_n = 3
            cb()

    setNew: () ->
        @throwBadStateFor('setNew') if @_n != 0
        @_n = 2
        return @

    setPersisted: (data) ->
        F.demandNonEmptyObject(data, 'data')

        @throwBadStateFor('setPersisted') if @_n == 3

        unless @_schema.coversSomeKey(data)
            F.throw("Argument 'data' does not cover any keys in #{@_schema}")

        @_persisted = data
        @_n = 1
        return @

    _isDirty: () -> @_n in [0,2] || !_.isEmpty(@_changed)

    insert: (cb) -> states[@_n].insert.call(@, cb)

    # SHOULD: consider calling cb in next tick, so that we always look async
    # to the caller
    update: (cb) ->
        return cb(null, @) unless @_isDirty()
        states[@_n].update.call(@, cb)

    upsert: (cb) ->
        return cb(null, @) unless @_isDirty()
        states[@_n].upsert.call(@, cb)

    delete: (cb) -> states[@_n].delete.call(@, cb)

module.exports = ActiveRecord
