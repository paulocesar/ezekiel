_ = require('more-underscore/src')
{ AliasedObject } = dbObjects = require('./index')

class Table extends AliasedObject
    constructor: (@db, schema) ->
        super(schema)
        @columns = []

        @columnsByName = {}
        @columnsByAlias = {}

        @pk = null
        @keys = []
        @foreignKeys = []
        @incomingFKs = []
        @selfFKs = []

        @hasMany = []
        @belongsTo = []

        @db.tables.push(@)
        @many = @one = @alias

    siblingsByName: () -> @db.tablesByName
    siblingsByAlias: () -> @db.tablesByAlias

    getKeysWithShape: () ->
        values = _.unwrapArgs(arguments)
        unless values?
            e = "No arguments given. You must provide values to specify the key shape you seek. " +
                "Examples: getKeysWithShape(20), getKeysWithShape('foo', 'bar'), etc."
            throw new Error(e)

        return (k for k in @keys when k.matchesType(values))

module.exports = dbObjects.Table = Table
