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

    getOptions: (cb) ->
        @db.scalar 'SELECT @@OPTIONS', (err, r) ->
            return cb(err) if err

            opt = []
            opt.push('DISABLE_DEF_CNST_CHK') if (1 & r)
            opt.push('IMPLICIT_TRANSACTIONS') if (2 & r)
            opt.push('CURSOR_CLOSE_ON_COMMIT') if (4 & r)
            opt.push('ANSI_WARNINGS') if (8 & r)
            opt.push('ANSI_PADDING') if (16 & r)
            opt.push('ANSI_NULLS') if (32 & r)
            opt.push('ARITHABORT') if (64 & r)
            opt.push('ARITHIGNORE') if (128 & r)
            opt.push('QUOTED_IDENTIFIER') if (256 & r)
            opt.push('NOCOUNT') if (512 & r)
            opt.push('ANSI_NULL_DFLT_ON') if (1024 & r)
            opt.push('ANSI_NULL_DFLT_OFF') if (2048 & r)
            opt.push('CONCAT_NULL_YIELDS_NULL') if (4096 & r)
            opt.push('NUMERIC_ROUNDABORT') if (8192 & r)
            opt.push('XACT_ABORT') if (16384 & r)

            cb(null, opt)

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
                if r.maxLength == -1
                    r.maxLength = 'max'
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
