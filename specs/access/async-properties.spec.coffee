h = require('../test-helper')
require('../live-db')
ezekiel = h.requireSrc()
should = require('should')
path = require('path')

db = event = newEvent =  null
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

    it 'init a new event', () ->
        newEvent = db.event { name: 'Test event', date: new Date(), promotionId: 2 }
    
    it 'get promotion, async', (done) ->
        newEvent.promotion (err, promotion) ->
            return done(err) if err
            promotion.should.eql(h.testData.promotions[1].name)
            done()

    it 'setPersisting', (done) ->
        data = {
            name: "Yeah, I change the name again"
            promotion: "Win a MAC PRO!"
        }
        newEvent.setPersisting data, (err) ->
            return done(err) if err
            newEvent.id.should.be.eql(h.testData.events.length + 1)
            newEvent.name.should.eql("Yeah, I change the name again")
            newEvent.promotion (err, promotion) ->
                return done(err) if err
                promotion.should.eql("Win a MAC PRO!")
                done()
    
    it 'get next promotion', (done) ->
        event.nextPromotion (err, nextPromotion) ->
            return done(err) if err
            nextPromotion.should.eql("Win a MAC PRO!")
            done()

    it 'try get next promotion, gets an error', (done) ->
        newEvent.nextPromotion (err, nextPromotion) ->
           err.should.match(/No data returned for query/)
           done()
