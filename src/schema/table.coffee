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

    coversSomeKey: (values) ->
        _.some(@keys, (k) -> k.coveredBy(values))

    column: (schema) -> new Column(@, schema)


module.exports = dbObjects.Table = Table
