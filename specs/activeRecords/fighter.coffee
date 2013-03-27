f = (proto, schema) ->
    proto.sayHi = () -> "Hi, my name is #{@firstName} #{@lastName}"

module.exports = f
