_ = require('more-underscore/src')

class DbObject
    constructor: (schema) ->
        unless schema.name?
            throw new Error("You must provide a name for each schema object")

        _.defaults(@, schema)

    toString: () ->
        s = "#{@constructor.name} \"#{@name}\""
        return s

    addEnforcingPosition: (array, newbie, position = newbie.position) ->
        expectedPosition = array.length + 1

        if expectedPosition != position
            msg = "Cannot add [#{newbie}] to [#{@}]. Expected position to be " +
                "#{expectedPosition} but it was #{position}"
            throw new Error(msg)

        array.push(newbie)

# Many DB objects can have handles in JavaScript land to decouple JS code from DB schema. For
# example, tables can have a 'many' handle and a 'one' handle (to be used with collections and
# single objects, respectively). The many handle is used to access the table gateway and in FROM
# / JOIN clauses backed by the schema. The one handle is used to retrieve ActiveRecord instances.
#
# Columns can have a 'property' handle, which is used as the column name in schema-backed queries
# and name of the property encapsulating the column in an ActiveRecord instance.
#
# The 'name' property in a schema object is ALWAYS its name in the database. If no handles are
# provided, the table name is be used for the table in both many and one situations. The column
# name is used as the JS property name. Thus people can be 100% oblivious to the handle mechanism
# and stuff just works. Those who want to be able to say pretty things like:
#
# db.customers.findMany()
# db.customer(100, cb)
#
# Must set many/one in tables and property in columns as they see fit.
#
# The schema API however ALWAYS WORKS WITH NAMES to ensure sanity. Names are immutable.

jsTypes = {
    number:
        matchesType: _.isNumber
        convert: Number
        name: 'Number'

    boolean:
        matchesType: _.isBoolean
        # MUST: ponder the fact that Boolean('false') is true, see if we want to do something
        # about it
        convert: Boolean
        name: 'Boolean'

    string:
        matchesType: _.isString
        convert: String
        name: 'String'

    date:
        matchesType: _.isDate
        # MUST: beef date conversion way up
        convert: (v) -> new Date(Date.parse(v.toString()))
        name: 'Date'
}


dbTypeToJsType = {
    varchar: 'string'
    datetime: 'date'
    int: 'number'
}

class Column extends DbObject
    constructor: (@table, schema) ->
        super(schema)

        t = dbTypeToJsType[@dbDataType]
        @jsType = jsTypes[t]
        @isPartOfKey = false
        @isReadOnly = @isIdentity || @isComputed
        @isRequired = !@isNullable
        if @position?
            @table.addEnforcingPosition(@table.columns, @)
        else
            @table.columns.push(@)

        @property = @name
        @table.columnsByName[@name] = @

    isInsertable: (v) -> @getInsertError(v) == ''

    getInsertError: (v) ->
        if @isIdentity
            return "#{@} is an identity column and cannot be inserted into."
        if @isReadOnly
            return "#{@} is a read-only column and cannot be inserted into."

        # MUST: deal with data types
        return ''

    isFullPrimaryKey: () -> _.isOnlyElement(@table.pk?.columns, @)
    matchesType: (v) -> @jsType.matchesType(v)
    sqlAlias: () -> @property

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
        if @type == 'PRIMARY KEY'
            @table.pk = @

    matchesType: () ->
        keyValues = _.unwrapArgs(arguments)

        unless keyValues?
            e = "You must provide key values to see if their shape matches key #{@}"
            throw new Error(e)

        unless _.isArray(keyValues)
            return @columns.length == 1 && @columns[0].matchesType(keyValues)

        return false unless @columns.length == keyValues.length

        for c, i in @columns
            v = keyValues[i]
            return false unless c.matchesType(v)

        return true

    wrapValues: () ->
        keyValues = _.unwrapArgs(arguments)

        unless keyValues?
            e = "You must provide the key values corresponding to the columns in #{@}"
            throw new Error(e)

        unless @matchesType(keyValues)
            e = "The key values provided (#{keyValues}) do not match the shape of #{@}"
            throw new Error(e)

        o = {}
        for c, i in @columns
            v = if i == 0 then _.firstOrSelf(keyValues) else keyValues[i]
            o[c.property] = v

        return o

    coveredBy: (values) -> _.every(@columns, (c) -> c.property of values)

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



module.exports = { DbObject, Column, Key, ForeignKey, Constraint }

require('./table')
require('./db-schema')
