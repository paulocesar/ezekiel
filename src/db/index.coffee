_ = require('underscore')

class DbObject
    constructor: (schema) ->
        _.defaults(@, schema)

    toString: () ->
        s = "#{@constructor.name}: name='#{@name}'"
        s += ", alias='#{@alias}'" if (@alias?)
        return s

    addEnforcingPosition: (array, newbie, position = newbie.position) ->
        expectedPosition = array.length + 1

        if expectedPosition != position
            msg = "Cannot add [#{newbie}] to [#{@}]. Expected position to be " +
                "#{expectedPosition} but it was #{position}"
            throw new Error(msg)

        array.push(newbie)

# Many DB objects can have aliases in JavaScript land to allow folks to keep their JS independent of
# the actual database schema. For example, tables, columns, and views can all be aliased.
#
# The 'name' property in an object is ALWAYS its name in the database. The 'alias' property defaults
# to the name, but you're free to change it to whatever naming convention you like best.  It is
# possible to define objects that do not have a name because they don't actually exist in the DB.
# They are called 'virtual'. For example, you could define a virtual column that has an alias and
# whose value is a SQL expression, and this column would not have a name.
#
# However, EVERY object must have an alias, If one is not provided, we set the alias equal to the
# name. This makes it easy to develop and use Ezekiel, because you can always trust that
# alias will be there. If you're not using aliases and/or virtual objects, then no harm done, all
# aliases will equal names, and you're good to go.
class AliasedObject extends DbObject
    constructor: (schema) ->
        unless schema.name? || schema.alias?
            throw new Error("You must provide a name and/or an alias")

        super(schema)
        @alias ?= @name

    updateIndexedProperty: (siblings, prop, newValue) ->
        oldValue = @[prop]
        if (oldValue == newValue)
            return

        clash = siblings[newValue]
        if clash?
            msg = "Can't change #{@} to new #{prop.substring(1)} '#{newValue}' " +
                "because it is already taken by #{clash}"
            throw new Error(msg)

        delete siblings[oldValue] if oldValue?
        siblings[newValue] = @
        @[prop] = newValue

Object.defineProperty(AliasedObject.prototype, 'name', {
    get: () -> @_name
    set: (newName) -> @updateIndexedProperty(@siblingsByName(), '_name', newName)
    enumerable: true, configurable: false
})

Object.defineProperty(AliasedObject.prototype, 'alias', {
    get: () -> @_alias
    set: (newAlias) -> @updateIndexedProperty(@siblingsByAlias(), '_alias', newAlias)
    enumerable: true, configurable: false
})

class Table extends AliasedObject
    constructor: (@db, schema) ->
        super(schema)
        @columns = []

        @columnsByName = {}
        @columnsByAlias = {}

        @keys = []
        @foreignKeys = []
        @incomingFKs = []
        @selfFKs = []

        @hasMany = []
        @belongsTo = []

        @db.tables.push(@)

    siblingsByName: () -> @db.tablesByName
    siblingsByAlias: () -> @db.tablesByAlias

class Column extends AliasedObject
    constructor: (@table, schema) ->
        super(schema)
        @isPartOfKey = false
        @isReadOnly = @isIdentity || @isComputed
        @isRequired = !@isNullable
        if @position?
            @table.addEnforcingPosition(@table.columns, @)
        else
            @table.columns.push(@)

    siblingsByName: () -> @table.columnsByName
    siblingsByAlias: () -> @table.columnsByAlias

class Constraint extends DbObject
    @types = ['PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY']

    constructor: (@table, schema) ->
        _.defaults(@, schema)
        @columns = []
        @isKey = @type != 'FOREIGN KEY'
        @table.db.constraintsByName[@name] = @

    addColumn: (schema) ->
        col = @table.columnsByName[schema.columnName]
        col.isPartOfKey = true if @isKey
        @addEnforcingPosition(@columns, col, schema.position)

    toString: () -> super.toString() + ", type=#{@type}"

# Stolen friends and disease
# Operator, please
# Pass me back to my mind
class Key extends Constraint
    constructor: (@table, schema) ->
        super(@table, schema)
        @table.keys.push(@)

class ForeignKey extends Constraint
    constructor: (@table, schema) ->
        super(@table, schema)

        @parentKey = @table.db.constraintsByName[@parentKeyName]
        @parentTable = @parentKey.table

        if (@table == @parentTable)
            @table.selfFKs.push(@)
            return

        @table.foreignKeys.push(@)

        unless _.contains(@table.belongsTo, @parentTable)
             @table.belongsTo.push(@parentTable)

        unless _.contains(@parentTable.hasMany, @table)
            @parentTable.hasMany.push(@table)

        @parentTable.incomingFKs.push(@)


module.exports = { DbObject, Table, Column, Key, ForeignKey, Constraint }

require('./database')
