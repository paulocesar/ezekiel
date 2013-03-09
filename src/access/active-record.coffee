_ = require('more-underscore/src')

{ SqlToken } = sql = require('../sql')
queryBinder = require('./query-binder')

states = [
    'Unknown'
    'Persisted'
    'New'
    'Deleted'
]

class ActiveRecordState
    constructor: (@old = {}, @new = {}, @state = 1) ->

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
        return if @[key]?

        Object.defineProperty(@, key, {
            get: () -> @get(key)
            set: (v) -> @set(key, v)
        })

    get: (property) -> @_s.new[property] ? @_s.old[property]
    set: (property, v) -> @_s.new[property] = v

    attach: (gw, s) ->
        @gw = gw
        @_s = s ? new ActiveRecordState()

    loadOldData: (o) -> @_s.loadOldData(o)
    loadNewData: (o) -> @_s.loadNewData(o)

module.exports = ActiveRecord
