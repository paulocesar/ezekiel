mysql = require('mysql')
_ = require('underscore')
poolModule = require('generic-pool')

class MysqlAdapter
    constructor: (config) ->
        @config = _.clone(config)
        @config.user = @config.userName
        @config.database ?= 'master'

        @pool = poolModule.Pool({
            name: 'mysql'
            create: (cb) => @_createConnection(@config, cb)
            destroy: (conn) -> conn.end()
        })

    _createConnection: (options, callback) ->
        conn = new mysql.createConnection(@config)
        conn.connect((err) =>
            if (err)
                callback(err)
            else
                conn.on('error', options.onError ? @onConnectionError)
                conn.on('message', options.onMessage ? @onConnectionMessage)
                callback(null, conn)
        )

    execute: (options) ->
        fnErr = options.onError ? @onExecuteError

        @pool.acquire((err, conn) =>
            if(err)
                fnErr(err)
                return

            stmt = conn.query(options.stmt)

            doRow = options.onRow?
            doAllRows = options.onAllRows?
            rows = [] if doAllRows
            foundRow = false

            if(doRow || doAllRows)
                stmt.on('result', (row) ->
                    foundRow = true
                    rowShape = options.rowShape ? 'object'

                    # TODO: review parse e _typeCast
                    if(rowShape == 'array')
                        out = (row[k] for k of row)
                    else
                        out = row

                    if doRow
                        options.onRow(out,options)

                    rows.push(out)

                    return
                )


            stmt.on('end', () =>
                if doAllRows
                    options.onAllRows(rows,options)

                if options.onDone?
                    options.onDone(rows.length)

                @pool.release(conn)
            )

            stmt.on('error', fnErr)

        )

    onConnectionMessage: (msg) ->

    onConnectionError: (err) ->
        throw new Error(err)

    onExecuteError: (err) ->
        throw new Error(err)

    doesDatabaseExist: (name, callback) ->
        @execute(
            {
                rowShape: 'array'
                master: true
                stmt: "SHOW DATABASES LIKE '#{name}'" #"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '#{name}'"
                onAllRows: (row) -> callback(row[0]?)
            }
        )


module.exports = MysqlAdapter