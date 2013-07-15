async = require('async')

f = (proto, schema) ->

    proto.defineAsyncProperty 'promotion', {
        get: (callback) ->
            @_gw.db.promotions.findOne @promotionId, (err, promotion) ->
                return callback(err) if err
                callback(null, promotion.name)
        
        set: (value, callback) ->
            @_gw.db.promotions.updateOne { id: @promotionId, name: value }, callback
    }

    proto.defineAsyncProperty 'nextPromotion', {
        get: (callback) ->
            @_gw.db.promotions.findOne @promotionId + 1, (err, promotion) ->
                return callback(err) if err
                callback(null, promotion.name)
    }

    proto.defineAsyncProperty 'noSetterOrGetter', { get: null, set: null }

module.exports = f
