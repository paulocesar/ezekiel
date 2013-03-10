_ = require('more-underscore/src')

{ SqlToken } = sql = require('../sql')
queryBinder = require('./query-binder')

states = [
    {
        name: 'Unknown'
        insert: (gw, cb) -> gw.insertOne(@inMemory, cb)
        update: (gw, cb) -> gw.updateOne(@inMemory, cb)
        upsert: (gw, cb) -> gw.upsertOne(@inMemory, cb)
        delete: (gw, cb) -> gw.deleteOne(@inMemory, cb)
    }
    {
        name: 'Persisted'
        insert: (gw, cb) -> @demandState('New')
        update: (gw, cb) -> gw.updateOne(@inMemory, @persisted, cb)
        upsert: (gw, cb) -> gw.updateOne(@inMemory, @persisted, cb)
        delete: (gw, cb) -> gw.deleteOne(@persisted, cb)
    }
    {
        name: 'New'
        insert: (gw, cb) -> gw.insertOne(@inMemory, cb)
        update: (gw, cb) -> @demandState('not New')
        upsert: (gw, cb) -> gw.insertOne(@inMemory, cb)
        delete: (gw, cb) -> @demandState('not New')
    }
    {
        name: 'Deleted'
        insert: (gw, cb) -> @demandState('not deleted')
        update: (gw, cb) -> @demandState('not deleted')
        upsert: (gw, cb) -> @demandState('not deleted')
        delete: (gw, cb) -> @demandState('not deleted')
    }
]

class ActiveRecordState
    constructor: (@old = {}, @inMemory = {}, @state = 0) ->

    loadOldData: (o) ->
        @old = o
        @state = 1 unless @state = 3

    loadNewData: (o) ->
        @new = o

class ActiveRecord
    constructor: (@gw, @schema, @_s = null) ->
        @_columnAccessors = {}

        for c in @schema.columns
            @addColumnAccessor(c)

    addColumnAccessor: (c) ->
        key = c.property
        return if key of @

        Object.defineProperty(@, key, {
            get: () -> @get(key)
            set: (v) -> @set(key, v)
        })

    get: (property) -> @_s.inMemory[property] ? @_s.persisted[property]
    set: (property, v) -> @_s.inMemory[property] = v

    attach: (gw, s) ->
        @gw = gw
        @_s = s ? new ActiveRecordState()

    @demandState: (s) -> throw new Error("needs state #{s}")

    loadOldData: (o) -> @_s.loadOldData(o)
    loadNewData: (o) -> @_s.loadNewData(o)

    insert: (cb) ->
        s = @_s
        states[s.state].insert.call(s, @gw, cb)

    update: (cb) ->
        s = @_s
        states[s.state].update.call(s, @gw, cb)

    upsert: (cb) ->
        s = @_s
        states[s.state].upsert.call(s, @gw, cb)

    delete: (cb) ->
        s = @_s
        states[s.state].delete.call(s, @gw, cb)

module.exports = ActiveRecord
