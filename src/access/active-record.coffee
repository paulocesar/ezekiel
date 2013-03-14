_ = require('more-underscore/src')

{ SqlToken } = sql = require('../sql')
queryBinder = require('./query-binder')

states = [
    {
        name: 'unknown'
        persist: (gw, cb) -> gw.upsertOne @changed, @makePersistHandler(cb)
        delete: (gw, cb) -> gw.deleteOne @changed, @makeDeleteHandler(cb)
    }
    {
        name: 'loaded'
        persist: (gw, cb) -> gw.updateOne @changed, @loaded, @makePersistHandler(cb)
        delete: (gw, cb) -> gw.deleteOne @loaded, @makeDeleteHandler(cb)
    }
    {
        name: 'new'
        persist: (gw, cb) -> gw.insertOne @changed, @makePersistHandler(cb)
        delete: (gw, cb) -> @demandStateFor('delete', 'not new')
    }
    {
        name: 'deleted'
        persist: (gw, cb) -> @demandStateFor('persist', 'not deleted')
        delete: (gw, cb) -> @demandStateFor('delete', 'not deleted')
    }
]

class ActiveRecordState
    constructor: (@loaded = {}, @changed = {}, @n = 0) ->
    name: () -> states[@n].name
    toString: () -> "<ActiveRecordState: #{@name()}>"

    persist: (gw, cb) -> states[@n].persist.call(@, gw, cb)
    delete: (gw, cb) -> states[@n].delete.call(@, gw, cb)

    get: (key) -> @changed[key] ? @loaded[key]

    set: (key, value) ->
      if @loaded[key] == value
        delete @changed[key]
        return

      @changed[key] = value

    demandStateFor: (op, s) ->
      e = "Cannot do operation #{op} in state #{@name()}. State must be #{s}"
      throw new Error(e)

    makePersistHandler: (cb) ->
        (err, outputValues) =>
            return cb(err) if err

            @loaded = _.extend(@loaded, @changed, outputValues)
            @n = 1
            @changed = {}
            cb(null, outputValues)

    makeDeleteHandler: (cb) ->
        (err) =>
            return cb(err) if err
            @n = 3
            cb()

    load: (o) ->
        @demandStateFor('load', 'not deleted') if @n == 3
        @loaded = o
        @n = 1

    new: (o) ->
        @demandStateFor('new', 'unknown') if @n != 0
        @changed = o
        @n = 2

class ActiveRecord
    constructor: (@gw, @schema, @_s = null) ->
        @_columnAccessors = {}

        for c in @schema.columns
            @addColumnAccessor(c)

    toString: () -> "<ActiveRecord for #{@schema.one}, #{@_stateName()}>"
    _stateName: () -> @_s.name()

    addColumnAccessor: (c) ->
        key = c.property
        return if key of @

        Object.defineProperty(@, key, {
            get: () -> @get(key)
            set: (v) -> @set(key, v)
        })

    get: (property) -> @_s.get(property)

    set: (property, v) ->
      @_s.set(property, v)
      return @

    setMany: (o) ->
      @set(k, v) for k, v of o
      return @

    attach: (gw, s) ->
        @gw = gw
        @_s = s ? new ActiveRecordState()

    load: (o) ->
      @_s.load(o)
      return @

    new: (o) ->
        @_s.new(o)
        return @

    persist: (cb) -> @_s.persist(@gw, cb)
    delete: (cb) -> @_s.delete(@gw, cb)

module.exports = ActiveRecord
