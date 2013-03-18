_ = require('more-underscore/src')
{ DbObject, Table, Column, Key, ForeignKey } = dbObjects = require('./index')

class DbSchema extends DbObject
    constructor: () ->
        @tables = []
        @tablesByName = {}
        @constraintsByName = {}

    finish: () ->
        @tables = _.sortBy(@tables, (t) -> t.name)
        @tablesByMany = {}
        @tablesByOne = {}
        for t in @tables
            @assertUniqHandle(t, t.many, @tablesByMany)
            @tablesByMany[t.many] = t

            @assertUniqHandle(t, t.one, @tablesByOne)
            @tablesByOne[t.one] = t
            t.finish()

        return @

    assertUniqHandle: (t, key, store) ->
        return unless key of store
        e = "Tables #{t} and #{store[key]} have the same handle of #{key}"
        throw new Error(e)

    addTables: () ->
        for a in arguments
            continue if a.db == @

            table = dbObjects.table(a)
            if table.name of @tablesByName
                throw new Error("#{@} already has a table named #{table.name}")

            @tables.push(table)
            @tablesByName[table.name] = table
            table.attach(@)

        return @

    addConstraints: () ->
        for c in arguments
            @constraintsByName[c.name] = c
        return @

    loadDataDictionary: (dataDictionary) ->
        @addTables.apply(@, dataDictionary.tables)
        @_addTableChildren(dataDictionary, 'columns')
        @_addTableChildren(dataDictionary, 'keys')
        @_addTableChildren(dataDictionary, 'foreignKeys')

        for kc in dataDictionary.keyColumns
            c = @constraintsByName[kc.constraintName]
            c.addColumn(kc)

        return @

    _addTableChildren: (dict, property) ->
        list = dict[property]
        for i in list
            table = @tablesByName[i.tableName]
            
            # If parent is a view, then we don't have it, so bail out. MUST: handle views
            # in the future
            return unless table?

            singular = property.slice(0, -1)
            child = dbObjects[singular](i)

            addFnName = 'add' + _.toUpperInitial(property)
            table[addFnName](child)

    table: (schema) ->
        @addTables(schema)
        return _.last(@tables)

module.exports = dbObjects.DbSchema = DbSchema
