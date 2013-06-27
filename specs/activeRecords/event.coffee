async = require('async')

f = (proto, schema) ->
    getPromotion = (callback) ->
        @_gw.db.promotions.findOne @promotionId, (err, promotion) ->
            return callback(err) if err
            callback(null, promotion.name)
    
    setPromotion = (value, callback) ->
        @_gw.db.promotions.updateOne { id: @promotionId, name: value }, callback

    proto.addAsyncProperty('promotion', getPromotion, setPromotion)

    proto.addAsyncProperty 'nextPromotion', (callback) ->
        @_gw.db.promotions.findOne @promotionId + 1, (err, promotion) ->
            return callback(err) if err
            callback(null, promotion.name)

module.exports = f
