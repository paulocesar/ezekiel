f = (proto, schema) ->
    proto.sayHi = () -> "Hi, my name is #{@firstName} #{@lastName}"

    proto.addAsyncProperty 'fullName', (callback) ->
        callback("#{@firstName} #{@lastName}")
    
    , (value, callback) ->
        @firstName = value.firstName; @lastName = value.lastName
        # (...) do some async work, then callback
        callback()
    

module.exports = f
