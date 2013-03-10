h = require('../test-helper')
require('../live-db')

TableGateway = h.requireSrc('access/table-gateway')

db = schema = tables = null

before () ->
    db = h.liveDb
    schema = db.schema
    tables = schema.tablesByMany

fighterGateway = () -> new TableGateway(db, tables.fighters)
assertFighterOne = (done) -> (err, row) ->
    return done(err) if err
    row.id.should.eql(1)
    done()

describe 'TableGateway', () ->
    it 'Can be instantiated', () -> fighterGateway()

    it 'Finds one row', (done) ->
        g = fighterGateway()
        g.findOne(1, assertFighterOne(done))

    it 'Can postpone query execution', (done) ->
        g = fighterGateway()
        g.findOne(1).run(assertFighterOne(done))

    it 'Is accessible via database property', (done) ->
        db.fighters.findOne(1, assertFighterOne(done))
