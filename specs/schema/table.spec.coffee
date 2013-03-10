h = require('../test-helper')
require('../live-db')

{ DbSchema, Table, Column } = h.requireSrc('schema')

schema = null
tables = null
before () ->
    schema = h.liveDb.schema
    tables = schema.tablesByMany

assertUniq = (uq, a) ->
    a.should.not.be.empty
    a.should.eql([uq])

describe 'Table', () ->
    it 'Finds single keys by their shape', () ->
        promotions = tables.promotions

        uq = schema.constraintsByName.UQ_Promotions_Name
        assertUniq(uq, promotions.getKeysWithShape('UFC'))
        promotions.getKeysWithShape(10).should.eql([promotions.pk])

        fighters = tables.fighters
        fighters.getKeysWithShape(1).should.eql([fighters.pk])

        fights = tables.fights
        fights.getKeysWithShape(1).should.eql([fights.pk])

    it 'Finds composite keys by the types of its values', () ->
        rounds = tables.rounds

        pk = tables.rounds.pk
        assertUniq(pk, rounds.getKeysWithShape(10, 10))
        assertUniq(pk, rounds.getKeysWithShape([10, 10]))

