h = require('../test-helper')
_ = require('underscore')
require('../live-db')

TableGateway = h.requireSrc('access/table-gateway')

testData = h.testData
cntFighters = h.testData.cntFighters

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

# SHOULD: move this into live-db.coffee, share. It's currently repeated.
assertCount = (cntExpected, done, fn) ->
    fn (err) ->
        return done(err) if err
        db.fighters.count (err, cnt) ->
            return done(err) if err
            cnt.should.eql(cntExpected)
            h.cleanTestData(done)

assertIdOne = (rowAssert, done, fn) ->
    fn (err) ->
        return done(err) if err
        db.fighters.findOne 1, (err, row) ->
            return done(err) if err
            rowAssert(row)
            h.cleanTestData(done)

describe 'TableGateway', () ->
    it 'Can be instantiated', () -> fighterGateway()


    it 'Is accessible via database property', () ->
        db.fighters.should.be.instanceof(TableGateway)


    it 'Can count rows', (done) ->
        db.fighters.count (err, cnt) ->
            return done(err) if err
            cnt.should.eql(cntFighters)
            done()


    it 'Can postpone query execution', (done) ->
        g = fighterGateway()
        g.findOne(1).run(assertFighterOne(done))


    it 'Allows query manipulation', (done) ->
        expected = _.filter(testData.fighters, (f) -> f.lastName == 'Silva').length
        db.fighters.count().where( { lastName: 'Silva' }).run (err, cnt) ->
            return done(err) if err
            cnt.should.eql(expected)
            done()


    it 'Finds one row', (done) ->
        g = fighterGateway()
        g.findOne(1, assertFighterOne(done))


    it 'Inserts one row', (done) ->
        f = testData.newFighter()
        assertCount cntFighters + 1, done, (cb) ->  db.fighters.insertOne(f, cb)


    it 'Updates one row by object predicate', (done) ->
        assert = (row) -> row.lastName.should.eql('Da Silva')
        op = (cb) -> db.fighters.updateOne({ lastName: 'Da Silva' }, { id: 1 }, cb)
        assertIdOne assert, done, op


    it 'Updates one row by direct key value', (done) ->
        assert = (row) -> row.lastName.should.eql('Aldo')
        op = (cb) -> db.fighters.updateOne({ firstName: 'Jose', lastName: 'Aldo' }, 1, cb)
        assertIdOne assert, done, op


    it 'Refuses to updateOne() without key coverage', () ->
        db.fighters.updateOne { lastName: 'Huxley' }, { firstName: 'Mauricio' }, (err) ->
            err.should.match(/please use updateMany/)


    it 'Deletes one row by object predicate', (done) ->
        # really, it's time for retirement
        assertCount cntFighters - 1, done, (cb) -> db.fighters.deleteOne({ id: 2 }, cb)


    it 'Deletes one row by direct key value', (done) ->
        assertCount cntFighters - 1, done, (cb) -> db.fighters.deleteOne(3, cb)
