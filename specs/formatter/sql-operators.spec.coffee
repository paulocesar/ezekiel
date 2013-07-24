h = require('../test-helper')
sql = h.requireSrc('sql')

describe('SQL Operators', () ->
    it 'supports binary operators via sql.* calls', ->
        q = sql.from('customers')
            .where(sql.startsWith('firstName', 'Bil'))
            .or(sql.endsWith('lastName', 'aggins'))
            .or(sql.contains('firstName', 'andal'))
            .or(sql.equals('lastName', 'Potter'))
            .or(sql.in('lastName', ['Snape', 'Dumbledore']))
            .or(sql.like('firstName', '%agor%'))
            .and(sql.notIn('firstName', ['foo', 'bar']))


        e = "SELECT * FROM [customers] WHERE (([firstName] LIKE 'Bil%' OR " +
            "[lastName] LIKE '%aggins' OR [firstName] LIKE '%andal%' OR " +
            "[lastName] = 'Potter' OR [lastName] IN ('Snape', 'Dumbledore') OR " +
            "[firstName] LIKE '%agor%') AND " +
            "[firstName] NOT IN ('foo', 'bar'))"

        h.assertSql(q, e)

    it 'supports partial binary operators as object values', ->
        q = sql.from('customers')
            .where(firstName: sql.startsWith('Hermio'))
            .or(lastName: sql.endsWith('anger'))
            .or(firstName: sql.contains('inerv'))
            .or(firstName: sql.equals('Quirinus'))

        e = "SELECT * FROM [customers] WHERE ([firstName] LIKE 'Hermio%' " +
            "OR [lastName] LIKE '%anger' OR [firstName] LIKE '%inerv%' OR " +
            "[firstName] = 'Quirinus')"

        h.assertSql(q, e)

    it 'allows reuse of partial binary operators', ->
        test = sql.startsWith('Voldem')
        q = sql.from('customers').where(firstName: test).or(lastName: test)

        e = "SELECT * FROM [customers] WHERE ([firstName] LIKE 'Voldem%' OR " +
            "[lastName] LIKE 'Voldem%')"
        h.assertSql(q, e)

    it 'supports binary operators by name', ->
        q = sql.from('products')
            .where(price: { 'between': [10, 20], '!=': 15 })
            .and(productName: { contains: 'dragon', endsWith: 'wand' })

        e = "SELECT * FROM [products] WHERE ([price] BETWEEN 10 AND 20 AND " +
            "[price] <> 15 AND ([productName] LIKE '%dragon%' AND [productName] LIKE '%wand'))"

        h.assertSql(q, e)

    it 'supports n-ary operators via sql.* calls', ->
        q = sql.from('customers')
            .where(sql.isntNull('firstName', 'lastName'))
            .or(sql.isNull('firstName'))
            .or(sql.isGood('firstName'))

        e = "SELECT * FROM [customers] WHERE ([firstName] IS NOT NULL AND [lastName] " +
            "IS NOT NULL OR [firstName] IS NULL OR [firstName] IS NOT NULL AND " +
            "LEN(RTRIM(LTRIM([firstName]))) > 0)"

        h.assertSql(q, e)

    e = "SELECT * FROM [customers] WHERE ([firstName] IS NULL AND " +
        "[lastName] IS NOT NULL AND LEN(RTRIM(LTRIM([lastName]))) > 0)"

    it 'supports partial n-ary operators as object values', ->
        q = sql.from('customers').where(firstName: sql.isNull(), lastName: sql.isGood())
        h.assertSql(q, e)

    it 'supports n-ary uninvoked builders as object values', ->
        q = sql.from('customers').where(firstName: sql.isNull, lastName: sql.isGood)
        h.assertSql(q, e)

    it 'supports columns as patterns in pattern match operators', ->
        q = sql.from('customers').where(firstName: sql.contains(sql.name('lastName')))
        e = "SELECT * FROM [customers] WHERE [firstName] LIKE '%' + [lastName] + '%'"
        h.assertSql(q, e)

    it 'aliases != to <>', ->
        q = sql.from('customers').where(firstName: { '!=': 'Neville' })
        e = "SELECT * FROM [customers] WHERE [firstName] <> 'Neville'"
        h.assertSql(q, e)
        
)
