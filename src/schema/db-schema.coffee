_ = require('more-underscore/src')
{ DbObject, AliasedObject, Table, Column, Key, ForeignKey } = dbObjects = require('./index')

class DbSchema extends DbObject
    constructor: (schema) ->
        @tables = []
        @tablesByName = {}
        @tablesByAlias = {}
        @constraintsByName = {}

        @load(schema) if schema?

    load: (schema) ->
        @addSchemaItems(Table, schema.tables, @)
        @tables = _.sortBy(@tables, (t) -> t.alias)
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
            c.addColumn(i)

module.exports = dbObjects.DbSchema = DbSchema
