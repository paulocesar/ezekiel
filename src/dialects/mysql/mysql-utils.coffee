_ = require('underscore')
async = require('async')
DbUtils = require('../db-utils')

class MysqlUtils extends DbUtils
    constructor: (@db) ->
        @stmts = {
            dbNow: 'SELECT NOW()'
            dbUtcNow: 'SELECT UTC_TIMESTAMP()'
            dbUtcOffset: "SELECT DATEDIFF(UTC_TIMESTAMP(), NOW())"
        }
