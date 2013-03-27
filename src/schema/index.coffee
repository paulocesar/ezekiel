_ = require('underscore')
F = require('functoids/src')

class DbObject
    constructor: (meta) ->
        unless meta.name?
            throw new Error("You must provide a name for each database object")

        _.defaults(@, meta)

    toString: () ->
        s = "<#{@constructor.name} #{@name}>"
        return s

    finish: () ->

    pushEnforcingPosition: (array, newbie, position = newbie.position) ->
        expectedPosition = array.length + 1

        if position? && expectedPosition != position
            msg = "Cannot add #{newbie} to #{@}. Expected position to be " +
                "#{expectedPosition} but it was #{position}"
            throw new Error(msg)

        array.push(newbie)

    attach: (table) ->
        if @table?
            throw new Error("attach: #{@} is already attached to #{@table}")

        unless table?
            throw new Error("attach: you must provide a table")

        @table = table

# DB objects can have nicknames in JavaScript land to decouple JS code from DB schema. For example,
# tables can have a 'many' nickname and a 'one' nickname (to be used with collections and single
# objects, respectively). 'many' is used as a SQL alias for the table in schema-aware queries, as
# the name of a TableGateway, etc, while 'one' is used to retrieve ActiveRecord instances.
#
# Columns can have a 'property' nickname, which is used as a SQL alias in schema-aware queries
# and as the name of the property encapsulating the column in an ActiveRecord instance.
#
# The 'name' property in a schema object is ALWAYS its name in the database. If no nicknames are
# provided, the table name is be used for the table in both many and one situations. The column name
# is used as the JS property name. Thus people can be 100% oblivious to the nickname mechanism and
# stuff just works. Those who want to be able to say pretty things like:
#
# db.customers.findMany()
# db.customer(100, cb)
#
# Must set 'many'/'one' in tables and 'property' in columns as they see fit.
#
# The schema API however ALWAYS WORKS WITH NAMES to ensure sanity. Names are immutable.

jsTypes = {
    number:
        matchesType: _.isNumber
        convert: Number
        name: 'Number'
        numeric: true

    boolean:
        matchesType: _.isBoolean
        # MUST: ponder the fact that Boolean('false') is true, see if we want to do something
        # about it
        convert: Boolean
        name: 'Boolean'
        numeric: false

    string:
        matchesType: _.isString
        convert: String
        name: 'String'
        numeric: false

    date:
        matchesType: _.isDate
        # MUST: beef date conversion way up
        convert: (v) -> new Date(Date.parse(v.toString()))
        name: 'Date'
        numeric: false
}


dbTypeToJsType = {
    varchar: 'string'
    datetime: 'date'
    int: 'number'
}

class Column extends DbObject
    constructor: (schema) ->
        super(schema)

        t = dbTypeToJsType[@dbDataType]
        @jsType = jsTypes[t]
        @isPartOfKey = false
        @isReadOnly = @isIdentity || @isComputed
        @isRequired = !@isNullable
        @property ?= @name

    canInsert: (v) -> !@getInsertError(v, false)

    getInsertError: (v, ignoreReadOnly = true, needsMsg = true) ->
        @getUpdateError(v, ignoreReadOnly && !@isPartOfKey, needsMsg)

    # MUST: deal with data types
    getUpdateError: (v, ignoreReadOnly = true, needsMsg = true) ->
        return null if ignoreReadOnly

        e = null
        if @isIdentity
            return true unless needsMsg
            e = "identity column"
        else if @isReadOnly
            return true unless needsMsg
            e = "read-only column"
        else
            return null

        return "Cannot write value #{v} into #{@}: #{e}."

    isFullPrimaryKey: () -> F.isOnlyElement(@table.pk?.columns, @)
    matchesType: (v) -> @jsType.matchesType(v)
    sqlAlias: () -> @property

class Constraint extends DbObject
    @types = ['PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY']

    constructor: (meta) ->
        super(meta)
        @columns ?= []
        @isKey = @type != 'FOREIGN KEY'

    addColumns: () ->
        unless @table?
            throw new Error("addColumns: you must attach #{@} to a table before adding columns")

        for meta in _.flatten(arguments)
            name = if _.isString(meta) then meta else meta.columnName ? meta.name
            unless name?
                throw new Error("addColumns: you must provide a column name")

            col = @table.columnsByName[name]
            unless col?
                throw new Error("addColumns: could not find column #{name} in #{@table}")

            col.isPartOfKey = true if @isKey

            @pushEnforcingPosition(@columns, col, meta.position)

        @isComposite = @columns.length > 1

    attach: (table) ->
        super(table)

        oldColumns = @columns
        @columns = []
        @addColumns(oldColumns)

        @table.db?.addConstraints(@)

# Stolen friends and disease
# Operator, please
# Pass me back to my mind
class Key extends Constraint
    constructor: (schema) ->
        super(schema)
        @isPK = (@type == 'PRIMARY KEY')

    numeric: () ->
        for c in @columns
            return false unless c.jsType.numeric
        return true

    contains: (column) -> _.contains(@columns, column)

    matchesShape: () ->
        keyValues = F.unwrapArgs(arguments)

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
        keyValues = F.unwrapArgs(arguments)

        unless keyValues?
            F.throw("You must provide the key values corresponding to the columns in #{@}")

        if _.isObject(keyValues) && !_.isArray(keyValues)
            o = {}
            for c in @columns
                p = c.property
                unless p of keyValues
                    F.throw("The given object has no property #{p} corresponding to column #{c}")
                o[p] = keyValues[p]

            return o

        unless @matchesShape(keyValues)
            F.throw("The key values provided (#{keyValues}) do not match the shape of #{@}")

        o = {}
        for c, i in @columns
            v = if i == 0 then F.firstOrSelf(keyValues) else keyValues[i]
            o[c.property] = v

        return o

    coveredBy: (values) ->
        for c in @columns
            return false unless values[c.property]?
        return true

    deleteValues: (values) ->
        delete values[c.property] for c in @columns
        return values

    names: () -> _.pluck(@columns, 'name')
    properties: () -> _.pluck(@columns, 'property')

class ForeignKey extends Constraint
    finish: () ->
        db = @table.db
        unless db?
            F.throw("You must attach #{@table} to a database before finishing a ForeignKey")

        @parentKey = db.constraintsByName[@parentKeyName]
        unless @parentKey?
            F.throw("Could not find parent key #{@parentKeyName} for #{@} in #{db}")

        @parentTable = @parentKey.table
        @parentTable.incomingFKs.push(@)

schema = {
    DbObject
    Constraint
    Key
    ForeignKey
    Column

    foreignKey: (meta) -> if meta instanceof ForeignKey then meta else new ForeignKey(meta)
    key: (meta) -> if meta instanceof Key then meta else new Key(meta)
    column: (meta) -> if meta instanceof Column then meta else new Column(meta)
}

module.exports = schema

require('./table')
require('./db-schema')
