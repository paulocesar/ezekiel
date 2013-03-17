h = require('../test-helper')
A = require('assert')

{ DbSchema, Table, Column } = h.requireSrc('schema')

schema = h.getCookedSchema()
tables = schema.tablesByMany
fighters = tables.fighters

assertUniq = (uq, a) ->
    a.should.not.be.empty
    a.should.eql([uq])

describe 'Table', () ->
    it 'finds single keys by their shape', () ->
        promotions = tables.promotions

        uq = schema.constraintsByName.UQ_Promotions_Name
        assertUniq(uq, promotions.getKeysWithShape('UFC'))
        promotions.getKeysWithShape(10).should.eql([promotions.pk])

        fighters = fighters
        fighters.getKeysWithShape(1).should.eql([fighters.pk])

        fights = tables.fights
        fights.getKeysWithShape(1).should.eql([fights.pk])


    it 'finds composite keys by the types of its values', () ->
        rounds = tables.rounds

        pk = tables.rounds.pk
        assertUniq(pk, rounds.getKeysWithShape(10, 10))
        assertUniq(pk, rounds.getKeysWithShape([10, 10]))


    it 'knows whether an object covers one of its keys', () ->
        fighters.coversSomeKey({ id: 1 }).should.be.true
        fighters.coversSomeKey({ lastName: 'Silva' }).should.be.false
        fighters.coversSomeKey({ gw: {}, _s: {} }).should.be.false

        tables.promotions.coversSomeKey({ name: 'UFC' }).should.be.true

        tables.rounds.coversSomeKey({ fightId: 1 }).should.be.false
        tables.rounds.coversSomeKey({ fightId: 1, number: 1 }).should.be.true

    it 'knows whether it has an identity column', () ->
        fighters.hasIdentity().should.be.true

    it 'knows whether a row can be updated, and the key to use', ->
        newbie = h.testData.newFighter()
        fighters.canUpdate(newbie).should.be.true

        key = fighters.getBestKeyForMerging(newbie)
        key.name.should.eql('UQ_Fighters_LastName_FirstName')

        newbie.lastName = null
        fighters.canUpdate(newbie).should.be.false
        key = fighters.getBestKeyForMerging(newbie)
        A.equal(key, null)
        errors = fighters.getUpdateErrors(newbie).join(' ')
        errors.should.match(/does not cover any keys/)

        newbie = h.testData.newFighter()
        newbie.id = 1
        fighters.canUpdate(newbie).should.be.true
        key = fighters.getBestKeyForMerging(newbie)
        key.name.should.eql('PK_Fighters')

    it 'knows whether a row can be inserted', ->
        newbie = h.testData.newFighter()
        fighters.canInsert(newbie).should.be.true

        newbie.lastName = null
        fighters.canInsert(newbie).should.be.false
        errors = fighters.getInsertErrors(newbie).join(' ')
        errors.should.match(/Missing required <Column LastName>/)

        newbie = h.testData.newFighter()
        newbie.id = 10
        fighters.canInsert(newbie).should.be.false
        errors = fighters.getInsertErrors(newbie).join(' ')
        errors.should.match(/Cannot write value/)
        errors.should.match(/identity column/)

    byKey = () -> { PK_Fighters: [], UQ_Fighters_LastName_FirstName: [] }
    mergeObj = () -> { inserts: [], updatesByKey: byKey(), mergesByKey: byKey() }

    it 'classifies rows for merging', ->
        rows = h.testData.newData()
        uq = "UQ_Fighters_LastName_FirstName"

        merge = fighters.classifyRowsForMerging(rows)
        o = mergeObj()
        o.mergesByKey[uq] = rows

        merge.should.eql(o)

        rows[0].id = 1
        rows[1].id = 2
        rows[1].lastName = null

        merge = fighters.classifyRowsForMerging(rows)
        o = mergeObj()
        o.updatesByKey.PK_Fighters = [rows[0], rows[1]]
        o.mergesByKey[uq] = [rows[2], rows[3]]

        merge.should.eql(o)

    it 'throws if bulk merge has bad row', ->
        rows = h.testData.newData()
        rows[0].lastName = null
        f = () -> fighters.classifyRowsForMerging(rows)
        f.should.throw(/Cannot merge row/)
