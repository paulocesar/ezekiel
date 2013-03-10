_ = require('more-underscore/src')

class DbObject
    constructor: (schema) ->
        unless schema.name?
            throw new Error("You must provide a name for each schema object")

        _.defaults(@, schema)

    toString: () ->
        s = "#{@constructor.name}: name='#{@name}'"
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
# MUST: remove the ability to change alias / name once schema object is instantiated
# New rules will be:
# 0. No name changing ever!
# 1. Name will remain the database name, as we have now
# 2. API into schema will be based on DB NAMES, so that people who don't want to worry about
# aliasing don't even have to think about it. All operations using the schema classes will be keyed
# off NAME.
# 3. Tables will not have an 'alias' anymore. It is so confusing anyway, since it sounds like a SQL
# alias, as in a SELECT statement. Tables will have two properties, 'many', and 'one', to name the
# collections and the active record object. Many will give name to the table gateway and the FROM
# name in SQL queries. One will give name to the active record. Both 'many' and 'one' will default
# to name. Many and one will also be used by ActiveRecord when naming properties for relations.
# 4. For columns, 'alias' will be replaced by 'property'
# 5. No collections by many or one at first. Only by name, which is immutable (see 0), so no need
# for property acrobatics
# 6. Once the DB loads the schema, there'll be collections by many/one names. After you load a
# schema into a DB, however, you can no longer mess with it, so we'll be ok.

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

    isFullPrimaryKey: () -> _.isOnlyElement(@table.pk?.columns, @)

    matchesType: (v) -> @jsType.matchesType(v)

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
