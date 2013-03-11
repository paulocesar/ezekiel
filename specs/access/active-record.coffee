h = require('../test-helper')
require('../live-db')

ActiveRecord = h.requireSrc('access/active-record')

testData = h.testData
db = schema = null

before () ->
    db = h.liveDb
    schema = db.schema

# SHOULD: move this into live-db.coffee, share. It's currently repeated.
cntFighters = testData.cntFighters
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

describe 'ActiveRecord', () ->
    it 'Can be instantiated via db.newObject()', ->
        for t in schema.tables
            o = db.newObject(t.one)
            o.should.be.an.instanceof(ActiveRecord)
            o.toString().should.include(t.one)

    it 'Can be instantiated via db[table.one]()', ->
        for t in schema.tables
            o = db[t.one]()
            o.should.be.an.instanceof(ActiveRecord)
            o.toString().should.include(t.one)

    it 'Can insert a row', (done) ->
        o = db.newObject('fighter')
        o.setMany(testData.newFighter())
        assertCount cntFighters + 1, done, (cb) -> o.insert(cb)

    it 'Can update a row', (done) ->
        o = db.newObject('fighter')
        db.fighters.findOne 1, (err, row) ->
            return done(err) if err
            o.load(row)
            o.firstName = 'The Greatest' # No wind or waterfall could stall me
            assert = (row) -> row.firstName.should.eql('The Greatest')
            assertIdOne assert, done, (cb) -> o.update(cb)

    it 'Can delete a row', (done) ->
        o = db.newObject('fighter')
        db.fighters.findOne 4, (err, row) ->
            return done(err) if err
            o.load(row)
            assertCount cntFighters - 1, done, (cb) -> o.delete(cb)
