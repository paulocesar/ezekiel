_ = require("underscore")
{ BinaryOp, NaryOp } = sql = require('./index')

newBinaryOp = (name) ->
    (left, right) ->
        if right? then new BinaryOp(left, name, right) else new BinaryOp(null, name, left)

newNaryOp = (name) ->
    (args...) -> new NaryOp(name, args)

addOps = (fn, names...) ->
    for n in names
        sql[n] = fn(n)

#binary logical
addOps(newBinaryOp, 'between', 'contains', 'endsWith', 'equals', 'in', 'notIn', 'like', 'startsWith')

#n-ary logical
addOps(newNaryOp, 'isGood', 'isNotNull', 'isntNull', 'isNull')
