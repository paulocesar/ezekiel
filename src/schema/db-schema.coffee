_ = require('more-underscore/src')
{ DbObject, Table, Column, Key, ForeignKey } = dbObjects = require('./index')

class DbSchema extends DbObject
    constructor: (schema) ->
        @tables = []
        @tablesByName = {}
        @constraintsByName = {}

        @load(schema) if schema?

    # MUST: make sure there's no clash in many
    finish: () ->
        @tablesByMany = {}
        @tablesByOne = {}
        for t in @tables
            @assertUniqHandle(t, t.many, @tablesByMany)
            @tablesByMany[t.many] = t

            @assertUniqHandle(t, t.one, @tablesByOne)
            @tablesByOne[t.one] = t
            t.columnsByProperty = columns = {}
            for c in t.columns
                columns[c.property] = c

        return @

    assertUniqHandle: (t, key, store) ->
        return unless key of store
        e = "Tables #{t} and #{store[key]} have the same handle of #{key}"
        throw new Error(e)

    load: (schema) ->
        @addSchemaItems(Table, schema.tables, @)
        @tables = _.sortBy(@tables, (t) -> t.name)
        @addSchemaItems(Column, schema.columns)
        @addSchemaItems(Key, schema.keys)
        @addSchemaItems(ForeignKey, schema.foreignKeys)

        @addKeyColumns(schema.keyColumns)

    addSchemaItems: (constructor, list, parent) ->
        for i in list
            p = parent ? @tablesByName[i.tableName]
            # If parent is a view, then we don't have it it, so bail out. MUST: handle views
            # in the future
            return unless p?
            new constructor(p, i)

    addKeyColumns: (list) ->
        for i in list
            c = @constraintsByName[i.constraintName]
            unless c?
                console.log(@constraintsByName, i)
            c.addColumn(i)

    table: (schema) -> new Table(@, schema)

module.exports = dbObjects.DbSchema = DbSchema
