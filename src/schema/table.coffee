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
            e = "getKeysWithShape: No arguments given. You must provide values to specify " +
                "the key shape you seek. " +
                "Examples: getKeysWithShape(20), getKeysWithShape('foo', 'bar'), etc."
            throw new Error(e)

        return (k for k in @keys when k.matchesShape(values))

    canInsert: (row, ignoreExtraneous = true, ignoreReadOnly = true) ->
        !@getInsertErrors(row, ignoreExtraneous, ignoreReadOnly, false)

    canUpdate: (row, ignoreExtraneous = true, ignoreReadOnly = true) ->
        !@getUpdateErrors(row, ignoreExtraneous, ignoreReadOnly, false)

    canMerge: (row, ignoreExtraneous = true, ignoreReadOnly = true) ->
        !@getMergeErrors(row, ignoreExtraneous, ignoreReadOnly, false)

    hasExtraneous: (row) ->
        for k of row
            return true unless k of @columnsByProperty
        return false

    # The get*Errors() functions can be called millions of times during an ETL. They must not do
    # heap allocations UNLESS: 1. there is an error to report, and 2. the caller asked for an error
    # message. If everything is peach and there are no errors, the functions return null. If there's
    # an error but the caller doesn't need the message, it simply returns true. Otherwise it returns
    # an array with all the errors in it. The has*Errors() functions are API sugar for a caller that
    # needs to know only whether there's an error.
    getInsertErrors: (row, ignoreExtraneous = true, ignoreReadOnly = true, needsMsg = true) ->
        sins = null
        for c in @columns
            v = row[c.property]
            if v?
                # commission
                e = c.getInsertError(v, ignoreReadOnly, needsMsg)
                if e?
                    return true unless needsMsg
                    (sins ?= []).push(e)
            else
                # ommission
                if c.isRequired && !c.isReadOnly
                    return true unless needsMsg
                    (sins ?= []).push("Missing required #{c}.")

        if !ignoreExtraneous && @hasExtraneous(row)
            return true unless needsMsg
            @_addErrorsForExtraneousColumns(row, (sins ?= []))

        return sins

    getUpdateErrors: (row, ignoreExtraneous = true, ignoreReadOnly = true, needsMsg = true) ->
        sins = null
        if @keys.length == 0
            return true unless needsMsg
            (sins ?= []).push("#{@} has no keys, so you can't update a specific row.")
        else
            unless @coversSomeKey(row)
                return true unless needsMsg
                (sins ?= []).push("Row does not cover any keys in #{@}.")

        for c in @columns
            v = row[c.property]
            if v?
                e = c.getUpdateError(v, ignoreReadOnly, needsMsg)
                if e?
                    return true unless needsMsg
                    (sins ?= []).push(e)

        if !ignoreExtraneous && @hasExtraneous(row)
            return true unless needsMsg
            @_addErrorsForExtraneousColumns(row, (sins ?= []))

        return sins


    getMergeErrors: (row, ignoreExtraneous = true, ignoreReadOnly = true, needsMsg = true) ->
        unless needsMsg
            return @canInsert(row, ignoreExtraneous, ignoreReadOnly) ||
                @canUpdate(row, ignoreExtraneous, ignoreReadOnly)

        errors = [].concat(
            for op in ['Insert', 'Update']
                @["get#{op}Errors"].apply(@, arguments)
        )

        return _.uniq(errors)

    _addErrorsForExtraneousColumns: (row, sins) ->
        for k of row
            unless k of @columnsByProperty
                sins.push("Extraneous property #{k} does not correspond to any columns in #{@}.")


    # This function finds the best key in a table to be used for matching an object to the target
    # row of an UPDATE or MERGE statement. It is tricky to do these without an explicit WHERE
    # clause, and above all we must make sure no data is destroyed. Here are the rules:
    #
    # 1. We'll only allow the UPDATE/MERGE if the object covers a primary key or UNIQUE key in the
    # table.
    #
    # 2. If the table has only one key, then we have it easy. The object must cover that key, and
    # that's what we'll use in the matching clause for the UPDATE/MERGE
    #
    # 3. If a table has multiple keys, we must decide between these approaches:
    #
    # a. Assume that keys are not updated, look for a record where all keys match, and update only
    # non-key fields. This is the most conservative, but it breaks common ETL scenarios where
    # a table has an immutable identity/integer PK, and then another UNIQUE column whose values
    # change occasionally. If we use all key values for the matching, changes in the UNIQUE column
    # cannot be merged in.
    #
    # b. Look for a record where at least one of the key matches, assume it's the right record, and
    # update the non-key fields and any possible changed keys. This is too wild and can destroy
    # data (one object could match multiple rows and wreak havoc).
    #
    # c. Elect one key by a deterministic algorithm, and use it. If one of the keys is read-only and
    # its value was provided, there's no ambiguity: an object with that key value must be referring
    # to that record, and we're happy. Even if there isn't a read-only key, this is still safe.
    #
    # This function goes with c)
    getBestKeyForMerging: (values) ->
        # The key must be covered by the values. Then we like, in order of importance:
        # read-only, numeric, clustered, not composite
        candidate = null
        for k in @keys
            if k.coveredBy(values)
                candidate = @_betterKey(candidate, k)
        return candidate

    _betterKey: (k1, k2) ->
        return k1 ? k2 unless k1? && k2?

        if k1.numeric() != k2.numeric()
            return if k1.numeric() then k1 else k2

        if k1.isClustered != k2.isClustered
            return if k1.isClustered then k1 else k2

        if k1.isComposite != k2.isComposite
            # notice that composite is worse, so the logic is reversed
            return if k2.isComposite then k1 else k2

    classifyRowsForMerging: (rows) ->
        unless _.isArray(rows)
            throw new Error('classifyRowsForMerging: you must provide an array of rows')

        inserts = []
        updatesByKey = {}
        mergesByKey = {}

        for k in @keys
            mergesByKey[k.name] = []
            updatesByKey[k.name] = []

        for r in rows
            canInsert = @canInsert(r)
            canUpdate = @canUpdate(r)

            unless canInsert || canUpdate
                errors = @getMergeErrors(r).join(' ')
                throw new Error("classifyRowsForMerging: Cannot merge row #{r}: #{errors}")

            key = @getBestKeyForMerging(r)

            target = null
            if canInsert && key?
                target = mergesByKey[key.name]
            else if key?
                target = updatesByKey[key.name]
            else if canInsert
                target = inserts
            else
                throw new Error( "classifyRowsForMerging: bug triggered by row #{r}")

            target.push(r)

        return { inserts, updatesByKey, mergesByKey }

    demandInsertable: (row, ignoreExtraneous = true) ->
        return if @canInsert(row, ignoreExtraneous)
        errors = @getInsertErrors(row, ignoreExtraneous).join('  ')
        throw new Error(errors)

    coversSomeKey: (row) ->
        for k in @keys
            return true if k.coveredBy(row)
        return false

    hasIdentity: () -> @some('isIdentity')
    hasReadOnly: () -> @some('isReadOnly')
    hasProperty: (p) -> p of @columnsByProperty

    some: (p) -> _.some(@columns, (c) -> c[p])
    column: (schema) -> new Column(@, schema)
    readOnlyProperties: () -> (c.property for c in @columns when c.isReadOnly)

module.exports = dbObjects.Table = Table
