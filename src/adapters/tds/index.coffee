tds = require('tds')
_ = require('underscore')
poolModule = require('generic-pool')

class TdsAdapter
    constructor: (config) ->
        @config = _.clone(config)
        @config.database ?= 'master'

        @pool = poolModule.Pool({
            name: 'tds'
            create: (cb) => @_createConnection(@config, cb)
            destroy: (conn) -> conn.end()
        })

    _createConnection: (options, callback) ->
        conn = new tds.Connection(@config)
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

            stmt = conn.createStatement(options.stmt)

            doRow = options.onRow?
            doAllRows = options.onAllRows?
            rows = [] if doAllRows

            if (doRow || doAllRows)
                stmt.on('row', (row) ->
                    columns = row.metadata.columns
                    rowShape = options.rowShape ? 'object'

                    # MUST: convert SQL bit to JS boolean
                    if (rowShape == 'array')
                        out = (row.getValue(col.index) for col in columns)
                    else
                        out = {}
                        for col in columns
                            v = row.getValue(col.index)
                            out[col.name ? col.index] = v

                            if (rowShape == 'mixed')
                                out[col.index] = v

                    if doRow
                        options.onRow(out, options)

                    if doAllRows
                        rows.push(out)

                    return
                )

            stmt.on('done', (affected) =>
                # MUST: ensure done is called even if there's an error, otherwise
                # we'll leak the connection

                if doAllRows
                    options.onAllRows(rows, options)

                if options.onDone?
                    options.onDone(affected)

                @pool.release(conn)
            )
                
            stmt.on('error', fnErr)
            stmt.execute()
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

module.exports = TdsAdapter
