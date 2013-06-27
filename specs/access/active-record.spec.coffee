h = require('../test-helper')
require('../live-db')
_ = require('underscore')

ActiveRecord = h.requireSrc('access/active-record')

testData = h.testData
db = schema = null

before () ->
    db = h.liveDb
    schema = db.schema

# SHOULD: move this into live-db.coffee, share. It's currently repeated.
cntFighters = testData.cntFighters
assertCount = (cntExpected, done, fn = (cb) -> cb()) ->
    fn (err) ->
        return done(err) if err
        db.fighters.count (err, cnt) ->
            return done(err) if err
            cnt.should.eql(cntExpected)
            h.cleanTestData(done)

assertIdOne = (rowAssert, done) ->
    fn (err) ->
        return done(err) if err
        db.fighters.findOne 1, (err, row) ->
            return done(err) if err
            rowAssert(row)
            h.cleanTestData(done)

describe 'ActiveRecord', () ->
    it 'can be instantiated via TableGateway.newObject()', ->
        for t in schema.tables
            o = db.getTableGateway(t.many).newObject()
            o.should.be.an.instanceof(ActiveRecord)
            o.toString().should.include(t.one)

    it 'can be instantiated via db[table.one]()', ->
        for t in schema.tables
            o = db[t.one]()
            o.should.be.an.instanceof(ActiveRecord)
            o.toString().should.include(t.one)

    it 'can be loaded from DB via db[table.one](id)', (done) ->
        db.fighter 1, (err, row) ->
            return done(err) if err
            row.id.should.eql(1)
            done()

    it 'can insert a row', (done) ->
        o = db.fighter(testData.newFighter())
        o._stateName().should.eql('new')

        o.persist (err) ->
            return done(err) if err
            o._stateName().should.eql('persisted')
            o.id.should.eql(cntFighters + 1)
            assertCount cntFighters + 1, done

    it 'can update a row', (done) ->
        db.fighter 1, (err, o) ->
            return done(err) if err
            o._stateName().should.eql('persisted')
            o.firstName = 'The Greatest' # No wind or waterfall could stall me
            o._isDirty().should.be.true

            o.persist (err) ->
                return done(err) if err
                o._isDirty().should.be.false
                o._stateName().should.eql('persisted')
                _.isEmpty(o._changed).should.be.true
                o.firstName.should.eql('The Greatest')
                done()

    it 'can delete a row', (done) ->
        db.fighter 4, (err, o) ->
            return done(err) if err
            o.id.should.eql(4)
            o._stateName().should.eql('persisted')

            o.destroy (err) ->
                return done(err) if err
                o._stateName().should.eql('destroyed')
                assertCount cntFighters - 1, done

    it 'test send date in wrong format', (done) ->
        db.event 1, (err, o) ->
            return done(err) if err
            o.date = new Date().toString()
            o.persist (err) ->
                return done(err) if err
                done()
