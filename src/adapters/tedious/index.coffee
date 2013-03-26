tedious = require('tedious')

{ Connection, Request } = tedious

_ = require('underscore')
poolModule = require('generic-pool')

class TediousAdapter
    constructor: (config) ->

        @config = _.pick(config, 'userName', 'password', 'pooling')
        @config.server = config.host
        @config.pooling ?= true

        @config.options = _.pick(config, 'database', 'port', 'connectTimeout')
        @config.options.port ?= 1433

        if @config.pooling
            # By default, we use a maximum pool size of 100, equal to the .NET default
            # See http://msdn.microsoft.com/en-us/library/8xx3tyca.aspx
            @pool = poolModule.Pool({
                name: 'tedious'
                create: (cb) => @_createConnection(cb)
                destroy: (conn) => conn.close()
                max: 100
            })

    _createConnection: (callback) ->
        conn = new Connection(@config)
        conn.on('connect', (err) =>
            if err?
                callback(err)
            else
                # MUST: find a real solution instead of this godawful workaround
                set = 'SET ANSI_PADDING ON SET ANSI_WARNINGS ON ' +
                        'SET ANSI_NULLS ON SET ARITHABORT ON SET QUOTED_IDENTIFIER ON ' +
                        'SET ANSI_NULL_DFLT_ON ON SET CONCAT_NULL_YIELDS_NULL ON'

                request = new Request(set, (err, rowCount) ->
                    return callback(err) if err?
                    callback(null, conn)
                )

                conn.execSqlBatch(request)
        )

    connect: (cb) -> if @config.pooling then @pool.acquire(cb) else @_createConnection(cb)
    release: (conn) -> if @config.pooling then @pool.release(conn) else conn.close()

    execute: (options) ->
        fnErr = options.onError ? @onExecuteError

        @connect (err, conn) =>
            if err?
                fnErr(err)
                return

            doRow = options.onRow?
            doAllRows = options.onAllRows?
            rows = [] if doAllRows

            request = new Request(options.stmt, (err, rowCount) =>
                @release(conn)

                if err?
                    fnErr(err)
                    return

                if doAllRows
                    options.onAllRows(rows, options)

                if options.onDone?
                    options.onDone(rowCount)
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
                
            conn.execSqlBatch(request)


    onConnectionMessage: (msg) ->

    onConnectionError: (err) -> throw new Error(err)
    onExecuteError: (err) -> throw new Error(err)

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
