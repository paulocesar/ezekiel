h = require('../test-helper')
require('../live-db')

testData = h.testData
db = schema = tables = null

before () ->
    db = h.liveDb
    schema = db.schema
    tables = schema.tablesByMany

# SHOULD: move this into live-db.coffee, share. It's currently repeated.
cntFighters = testData.cntFighters
assertCount = (cntExpected, done, fn) ->
    fn (err) ->
        return done(err) if err
        db.fighters.count (err, cnt) ->
            return done(err) if err
            cnt.should.eql(cntExpected)
            h.cleanTestData(done)

describe 'ActiveRecord', () ->
    it 'Can insert a row', (done) ->
        o = db.newObject('fighter')
        o.setMany(testData.newFighter())
        assertCount cntFighters + 1, done, (cb) -> o.insert(cb)
