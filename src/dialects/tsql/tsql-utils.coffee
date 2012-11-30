_ = require('underscore')
async = require('async')
DbUtils = require('../db-utils')

class TsqlUtils extends DbUtils
    constructor: (@db) ->
        @stmts = {
            dbNow: 'SELECT GETDATE()'
            dbUtcNow: 'SELECT GETUTCDATE()'
            dbUtcOffset: "SELECT DATEDIFF(mi, GETUTCDATE(), GETDATE())"
        }

    getTables: (callback) ->
        query =
            "SELECT TABLE_NAME name FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'"

        @db.allRows(query, (err, rows) ->
            if err then callback(err, null)
            callback(null, _.sortBy(rows, (r) -> r.name))
        )

    getColumns: (callback) ->
        query =
            "SELECT
				TABLE_NAME tableName, COLUMN_NAME name, ORDINAL_POSITION position,
				COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsIdentity') isIdentity,
				COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsComputed') isComputed,
				IS_NULLABLE isNullable, DATA_TYPE dbDataType, CHARACTER_MAXIMUM_LENGTH maxLength
			FROM
				INFORMATION_SCHEMA.COLUMNS
			ORDER BY
				TABLE_NAME, ORDINAL_POSITION"

        @db.allRows(query, (err, rows) =>
            callback(err, null) if err
            for r in rows
                r.isNullable = r.isNullable == 'YES'
            callback(null, rows)
        )

    getKeys: (callback) ->
        query = "
            SELECT CONSTRAINT_NAME name, TABLE_NAME tableName, CONSTRAINT_TYPE type
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
            WHERE TABLE_NAME IS NOT NULL AND CONSTRAINT_TYPE <> 'FOREIGN KEY'
        "

        @db.allRows(query, callback)

    getForeignKeys: (callback) ->
        query = "
        SELECT
            FK.CONSTRAINT_NAME name, FK.UNIQUE_CONSTRAINT_NAME parentKeyName,
            C.TABLE_NAME tableName, 'FOREIGN KEY' type
        FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS FK
        INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS C ON FK.CONSTRAINT_NAME = C.CONSTRAINT_NAME
        "

        @db.allRows(query, callback)

    getKeyColumns: (callback) ->
        query = "
            SELECT
                CONSTRAINT_NAME constraintName, TABLE_NAME tableName, COLUMN_NAME columnName,
                ORDINAL_POSITION position 
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
            ORDER BY constraintName, position
        "

        @db.allRows(query, callback)

    buildFullSchema: (callback) ->
        async.parallel({
            tables: (cb) => @getTables(cb)
            columns: (cb) => @getColumns(cb)
            keys: (cb) => @getKeys(cb)
            foreignKeys: (cb) => @getForeignKeys(cb)
            keyColumns: (cb) => @getKeyColumns(cb)
        }, callback)

module.exports = TsqlUtils
