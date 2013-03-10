h = require('../test-helper')
require('../live-db')

should = require('should')
{ DbSchema, Table, Column } = h.requireSrc('schema')

meta = h.getMetaData()
db = null
schema = null
before () ->
    db = h.liveDb
    schema = db.schema



withDbAndSchema = (f) -> f(h.blankDb(), h.newSchema())
loadedSchema = null

describe 'DbSchema', () ->
    it 'loads schema correctly', () ->
        schema.tables.length.should.eql(meta.tables.length)
    
    it 'detects clashes in column positions', () ->
        s = h.newRawSchema()
        t = s.tables[0]
        (() -> t.column({ name: 'Boom', position: 1 })).should.throw(/Expected position/)
    
    it 'throws if name is missing', ->
        t = h.newRawSchema().tables[0]
        (() -> t.column({})).should.throw(/must provide a name/)

        s = h.newRawSchema()
        (() -> s.table({})).should.throw(/must provide a name/)

    it 'throws if a table name clashes', ->
        s = h.newRawSchema()
        (() -> s.table({name: 'Fighters'})).should.throw(/already a table named/)

    it 'throws if handle clashes', ->
        schema = h.newRawSchema()
        schema.tables[0].many = schema.tables[1].many = 'FAIL'
        clash = () -> schema.finish()
        clash.should.throw(/have the same handle/)
