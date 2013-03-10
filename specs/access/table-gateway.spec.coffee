h = require('../test-helper')
_ = require('underscore')
require('../live-db')

TableGateway = h.requireSrc('access/table-gateway')

testData = h.testData
cntFighters = h.testData.fighters.length

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

    it 'Is accessible via database property', (done) ->
        db.fighters.findOne(1, assertFighterOne(done))

    it 'Can count rows', (done) ->
        db.fighters.count (err, cnt) ->
            return done(err) if err
            cnt.should.eql(cntFighters)
            done()

    it 'Is Oh So Sweet!!!', (done) ->
        db.fighters.count().where( { lastName: 'Silva' }).run (err, cnt) ->
            return done(err) if err
            expected = _.filter(testData.fighters, (f) -> f.lastName == 'Silva').length
            cnt.should.eql(expected)
            done()

    it 'Finds one row', (done) ->
        g = fighterGateway()
        g.findOne(1, assertFighterOne(done))

    it 'Can postpone query execution', (done) ->
        g = fighterGateway()
        g.findOne(1).run(assertFighterOne(done))

    it 'Inserts one row', (done) ->
        # I keep telling my brother to drop out of residency and start his MMA carreer
        # before it's too late
        f = testData.makeFighter('Guilherme', 'Duarte', '1987-03-14', 'Brazil', 180, 188, 175)
        db.fighters.insertOne f, (err, id) ->
            return done(err) if err
            db.fighters.count (err, cnt) ->
                cnt.should.eql(cntFighters + 1)
                h.cleanTestData(done)

    it 'Updates one row', (done) ->
        db.fighters.updateOne { lastName: 'Da Silva' }, { id: 1 }, (err) ->
            return done(err) if err
            db.fighters.findOne 1, (err, row) ->
                return done(err) if err
                row.lastName.should.eql('Da Silva')
                h.cleanTestData(done)

    it 'Refuses to updateOne() without key coverage', () ->
        db.fighters.updateOne { lastName: 'Huxley' }, { firstName: 'Mauricio' }, (err) ->
            err.should.match(/please use updateMany/)

    it 'Deletes one row by object predicate', () ->
        # really, it's time for retirement
        db.fighters.deleteOne { id: 2 }, (err) ->
            return done(err) if err
            db.fighters.count (err, cnt) ->
                return done(err) if err
                cnt.should.eql(cntFighters - 1)
                h.cleanTestData(done)
