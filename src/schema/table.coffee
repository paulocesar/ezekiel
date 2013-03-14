_ = require('more-underscore/src')
{ DbObject, Column } = dbObjects = require('./index')

class Table extends DbObject
    constructor: (@db, schema) ->
        super(schema)
        @columns = []

        @columnsByName = {}

        @pk = null
        @keys = []
        @foreignKeys = []
        @incomingFKs = []
        @selfFKs = []

        @hasMany = []
        @belongsTo = []

        if @name of @db.tablesByName
          e = "There is already a table named #{@name} in #{@db}"
          throw new Error(e)

        @db.tables.push(@)
        @db.tablesByName[@name] = @
        @many = @one = @name

    sqlAlias: () -> @many

    getKeysWithShape: () ->
        values = _.unwrapArgs(arguments)
        unless values?
            e = "No arguments given. You must provide values to specify the key shape you seek. " +
                "Examples: getKeysWithShape(20), getKeysWithShape('foo', 'bar'), etc."
            throw new Error(e)

        return (k for k in @keys when k.matchesType(values))

    isInsertable: (values, ignoreExtraneous = true) -> @getInsertErrors(values) == null

    hasExtraneous: (values) ->
        for k of values
            return true unless k of @columnsByProperty
        return false

    # no heap allocations if there's no error. This can be called millions of times during an ETL
    getInsertErrors: (values, ignoreExtraneous = true) ->
        sins = null
        for c in @columns
            v = values[c.property]
            if v?
                # commission
                e = c.buildInsertErrorMsg(v)
                (sins ?= []).push(e) if e?
            else
                # ommission
                if c.isRequired && !c.isReadOnly
                    (sins ?= []).push("Missing required #{c}.")

        return sins if ignoreExtraneous

        for k of values
            unless k of @columnsByProperty
                sins ?= []
                sins.push("Extraneous property #{k} doesn't correspond to any columns in #{@}")
    
        return sins

    demandInsertable: (values, ignoreExtraneous = true) ->
        return if @isInsertable(values, ignoreExtraneous)
        errors = @getInsertErrors(values, ignoreExtraneous).join('')
        throw new Error(errors)

    coversSomeKey: (values) -> _.some(@keys, (k) -> k.coveredBy(values))
    hasIdentity: () -> @some('isIdentity')
    hasReadOnly: () -> @some('isReadOnly')
    hasProperty: (p) -> p of @columnsByProperty

    some: (p) -> _.some(@columns, (c) -> c[p])
    column: (schema) -> new Column(@, schema)
    readOnlyProperties: () -> (c.property for c in @columns when c.isReadOnly)

module.exports = dbObjects.Table = Table
