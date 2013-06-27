f = (proto, schema) ->
    proto.sayHi = () -> "Hi, my name is #{@firstName} #{@lastName}"

    proto.addAsyncProperty 'fullName', (callback) ->
        callback(null, "#{@firstName} #{@lastName}")
    
    , (value, callback) ->
        @firstName = value.firstName; @lastName = value.lastName
        # (...) do some async work, then callback

        callback()

    
    proto.addAsyncProperty 'nextFight', (callback) ->
        callback(null, "Tomorrow!")

module.exports = f
