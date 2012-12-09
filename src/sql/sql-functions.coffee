_ = require("more-underscore/src")
{ FunctionCall } = sql = require('./index')

addFunctions = (names...) ->
    for n in names
        sql[n] = ((name) ->
            (args...) -> new FunctionCall(name, args))(n)

addSubqueryFunctions = (names...) ->
    for n in names
        sql[n] = ((name) ->
            (query) ->
                if _.isString(query)
                    query = sql.verbatim(query)

                    return new FunctionCall(name, query)
        )(n)

#nullary
addFunctions('now', 'utcNow')

#unary subquery. These aren't really functions in SQL, but they look exactly like one,
#which makes our life easier. So there.
addSubqueryFunctions('any', 'exists', 'some')

#unary aggregation
addFunctions('abs', 'avg', 'count', 'max', 'min', 'sum')

#unary string
addFunctions('len', 'ltrim', 'rtrim', 'trim')

#n-ary
addFunctions('coalesce')
