_ = require('more-underscore/src')

{ SqlToken } = sql = require('../sql')
queryBinder = require('./query-binder')

states = [
    {
        name: 'unknown'
        insert: (gw, cb) -> gw.insertOne(@changed, cb)
        update: (gw, cb) -> gw.updateOne(@changed, cb)
        upsert: (gw, cb) -> gw.upsertOne(@changed, cb)
        delete: (gw, cb) -> gw.deleteOne(@changed, cb)
    }
    {
        name: 'loaded'
        insert: (gw, cb) -> @demandState('insert', 'New')
        update: (gw, cb) -> gw.updateOne(@changed, @loaded, cb)
        upsert: (gw, cb) -> gw.updateOne(@changed, @loaded, cb)
        delete: (gw, cb) -> gw.deleteOne(@loaded, cb)
    }
    {
        name: 'new'
        insert: (gw, cb) -> gw.insertOne(@changed, cb)
        update: (gw, cb) -> @demandState('update', 'not new')
        upsert: (gw, cb) -> gw.insertOne(@changed, cb)
        delete: (gw, cb) -> @demandState('delete', 'not new')
    }
    {
        name: 'deleted'
        insert: (gw, cb) -> @demandState('insert', 'not deleted')
        update: (gw, cb) -> @demandState('update', 'not deleted')
        upsert: (gw, cb) -> @demandState('upsert', 'not deleted')
        delete: (gw, cb) -> @demandState('delete', 'not deleted')
    }
]

class ActiveRecordState
    constructor: (@loaded = {}, @changed = {}, @n = 0) ->
    name: () -> states[@n].name
    toString: () -> "<ActiveRecordState: #{@name()}>"

    insert: (gw, cb) -> states[@n].insert.call(@, gw, cb)
    update: (gw, cb) -> states[@n].update.call(@, gw, cb)
    delete: (gw, cb) -> states[@n].delete.call(@, gw, cb)
    upsert: (gw, cb) -> states[@n].upsert.call(@, gw, cb)

    get: (key) -> @changed[key] ? @loaded[key]

    set: (key, value) ->
      if @loaded[key] == value
        delete @changed[key]
        return

      @changed[key] = value

    demandState: (op, s) ->
      e = "Cannot do operation #{op} in state #{@name()}. State must be #{s}"
      throw new Error(e)

    load: (o) ->
        @loaded = o
        @n = 1 unless @n == 3

class ActiveRecord
    constructor: (@gw, @schema, @_s = null) ->
        @_columnAccessors = {}

        for c in @schema.columns
            @addColumnAccessor(c)

    toString: () -> "<ActiveRecord for #{@schema.one}>"

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

    insert: (cb) -> @_s.insert(@gw, cb)
    update: (cb) -> @_s.update(@gw, cb)
    upsert: (cb) -> @_s.upsert(@gw, cb)
    delete: (cb) -> @_s.delete(@gw, cb)

module.exports = ActiveRecord
