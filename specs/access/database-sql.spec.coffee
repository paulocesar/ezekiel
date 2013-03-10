h = require('../test-helper')
require('../live-db')
sql = h.requireSrc('sql')

db = null
before () ->
    db = h.liveDb

describe('Database using sql.* tokens', () ->
    it 'performs a SELECT query against Fighters', (done) ->
        s = sql.select('id', 'firstName').from('fighters')
        db.allRows(s, (err, rows) ->
            return done(err) if err
            rows.should.be.instanceOf(Array)
            done()
        )
    
)
