tedious = require('tedious')

{ Connection, Request } = tedious

_ = require('underscore')
poolModule = require('generic-pool')

class TediousAdapter
    constructor: (config) ->
        @config = { options: {} }
        @config.userName = config.userName
        @config.password = config.password
        @config.server = config.host
        @config.options.database = config.database

        @pool = poolModule.Pool({
            name: 'tedious'
            create: (cb) => @_createConnection(@config, cb)
            destroy: (conn) => conn.close()
        })

    _createConnection: (options, callback) ->
        conn = new Connection(@config)
        conn.on('connect', (err) =>
            if (err)
                callback(err)
            else
                callback(null, conn)
        )

    execute: (options) ->
        fnErr = options.onError ? @onExecuteError

        @pool.acquire((err, conn) =>
            if(err)
                fnErr(err)
                return

            doRow = options.onRow?
            doAllRows = options.onAllRows?
            rows = [] if doAllRows

            request = new Request(options.stmt, (err, rowCount) =>
                if (err)
                    fnErr(err)
                    return

                if doAllRows
                    options.onAllRows(rows, options)

                if options.onDone?
                    options.onDone(rowCount)

                @pool.release(conn)
            )

            if (doRow || doAllRows)
                request.on('row', (columns) ->
                    rowShape = options.rowShape ? 'object'

                    # MUST: convert SQL bit to JS boolean
                    if (rowShape == 'array')
                        out = (col.value for col in columns)
                    else
                        out = {}
                        for col, i in columns
                            v = col.value
                            out[col.metadata.colName ? i] = v

                            if (rowShape == 'mixed')
                                out[i] = v

                    if doRow
                        options.onRow(out, options)

                    if doAllRows
                        rows.push(out)

                    return
                )
                
            conn.execSql(request)
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
                stmt:"SELECT DB_ID('#{name}');"
                onRow: (row) -> callback(row[0]?)
            }
        )

    createDatabase: (name, callback) ->
        @execute(
            {
                master:true
                stmt:"IF (DB_ID('#{name}') IS NULL) CREATE DATABASE #{name};"
                onDone: (done) -> callback(done)
            }
        )

    dropDatabase: (name, callback) ->
        self = @
        @_killDatabaseProcesses(name, (done) ->
            self.execute(
                {
                    master:true
                    stmt:"IF (DB_ID('#{name}') IS NOT NULL) DROP DATABASE #{name};"
                    onDone: (dn) ->
                        return callback(dn)
                }
            )
        )

    _killDatabaseProcesses: (name, callback) ->
        self = @
        @execute(
            {
                master: true
                rowShape: 'array'

                stmt: "
                    SELECT SPId FROM MASTER..SysProcesses WHERE DBId =
                    DB_ID('#{name}') AND cmd <> 'CHECKPOINT'
                "
                onRow: (row) -> self._killProcess(row[0], callback) if row
                onDone: (done) -> callback(done)
            }
        )

    _killProcess: (id, callback) ->
        engine.execute(
            {
                master:true
                stmt:"KILL #{id}"
                onDone: (done) -> callback(done)
            }
        )

module.exports = TediousAdapter
