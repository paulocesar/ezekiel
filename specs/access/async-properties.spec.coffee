h = require('../test-helper')
require('../live-db')
ezekiel = h.requireSrc()
should = require('should')
path = require('path')

db = event = null
specRoot = path.resolve(__dirname, '..')

before (done) ->
    config = { connection: h.defaultDbConfig, require: specRoot, processSchema: h.cookSchema }

    ezekiel.connect config, (err, _db) ->
        return done(err) if err
        db = _db
        h.cleanTestData(done)

describe 'Async Properties', () ->
    it 'get first event', (done) ->
        db.event 1, (err, e) ->
            return done(err) if err
            event = e
            done()

    it 'get promotion, async', (done) ->
        event.promotion (err, promotion) ->
            return done(err) if err
            promotion.should.eql(h.testData.promotions[0].name)
            done()

    it 'set promotion, async', (done) ->
        event.promotion 'New promotion!', (err) ->
            return done(err) if err
            event.promotion (err, promotion) ->
                return done(err) if err
                promotion.should.eql("New promotion!")
                done()

    it 'loadAsyncProperties', (done) ->
        event.loadAsyncProperties 'promotion', 'nextPromotion', (err, values) ->
            return done(err) if err
            values.promotion.should.eql("New promotion!")
            values.nextPromotion.should.eql(h.testData.promotions[1].name)
            done()

    it 'setPersisting', (done) ->
        data = {
            name: "Change event name"
            promotion: "Uh oh! Promotion is over!"
        }
        event.setPersisting data, (err) ->
            return done(err) if err
            event.name.should.eql("Change event name")
            event.promotion (err, promotion) ->
                promotion.should.eql("Uh oh! Promotion is over!")
                done()
