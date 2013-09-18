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
            rows = []

            if(doRow || doAllRows)
                stmt.on('result', (row) ->
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
                stmt: "SHOW DATABASES LIKE '#{name}'"
                onAllRows: (row) -> callback(row[0]?)
            }
        )

    createDatabase: (name, callback) ->
        @execute(
            {
                master: true
                stmt: "CREATE DATABASE IF NOT EXISTS #{name}"
                onDone: (dn) -> callback(dn)
            }
        )

    dropDatabase: (name, callback) ->
        self = @
        @_killDatabaseProcesses(name, (done) ->
            self.execute(
                {
                    master: true
                    stmt: "DROP DATABASE IF EXISTS #{name}"
                    onDone: (dn) -> callback(dn)   
                }
            )
        )

    _killDatabaseProcesses: (name, callback) ->
        self = @
        @execute(
            {
                master: true
                rowShape: 'array'
                stmt: "SELECT ID FROM INFORMATION_SCHEMA.PROCESSLIST WHERE DB LIKE '#{name}'"
                onRow: (row) -> self._killProcess(row[0], callback) if row
                onDone: (done) -> callback(done)
            }
        )

    _killProcess: (id, callback) ->
        engine.execute(
            {
                master: true
                stmt: "KILL #{id}"
                onDone: (done) -> callback(done)
            }
        )



module.exports = MysqlAdapter