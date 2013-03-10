h = require('../test-helper')
require('../live-db')

{ DbSchema, Table, Column } = h.requireSrc('schema')

schema = null
tables = null
fighters = null
before () ->
    schema = h.liveDb.schema
    tables = schema.tablesByMany
    fighters = tables.fighters

describe 'Key', () ->
    it 'Wraps arguments into a key-value hash', () ->
        fighters.pk.wrapValues(42).should.eql({id: 42})

    it 'Throws if the values given to wrapValues() have the wrong shape', ->
        f = () -> fighters.pk.wrapValues(20, 10)
        f.should.throw(/do not match the shape/)

    it 'Wraps values for a composite key', () ->
        rounds = tables.rounds
        rounds.pk.wrapValues(10, 3).should.eql({fightId: 10, number: 3})

        f = () -> rounds.pk.wrapValues(55)
        f.should.throw(/do not match the shape/)

    it 'knows if a column is the full primary key', ->
        fighters.columnsByProperty.id.isFullPrimaryKey().should.be.true

    it 'knows that a column in a composite PK is not the full PK', ->
        tables.rounds.columnsByProperty.fightId.isFullPrimaryKey().should.be.false
