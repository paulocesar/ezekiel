h = require('../../test-helper')
sql = h.requireSrc('sql')

assert = (sql, e, debug = false) -> h.assertAlias(sql, e, debug)

describe 'SqlSelect with aliased schema', () ->
    it 'does basic SELECT', ->
        s = sql.select('id', 'firstName').from('fighters')

        expected = "SELECT [Id] as [id], [FirstName] as [firstName] FROM " +
            "[Fighters] as [fighters]"

        assert(s, expected)

    it 'always treats a raw string as name', ->
        s = sql.select('id', 'firstName', 'random').from('fighters')

        expected = "SELECT [Id] as [id], [FirstName] as [firstName], [random] FROM " +
            "[Fighters] as [fighters]"

        assert(s, expected)


    it 'does SQL prefixes', ->
        s = sql.select('fighters.id', 'firstName').from('fighters')

        expected = "SELECT [fighters].[Id] as [id], [FirstName] as " +
             "[firstName] FROM [Fighters] as [fighters]"

        assert(s, expected)
    

    it 'does SELECT with WHERE clause', ->
        s = sql.select('id', 'firstName').from('fighters')
            .where({firstName: 'Anderson'})

        expected = "SELECT [Id] as [id], [FirstName] as [firstName] FROM " +
            "[Fighters] as [fighters] WHERE [FirstName] = 'Anderson'"

        assert(s, expected)


    it 'does ORDER BY', ->
        s = sql.select('id').from('fighters').orderBy('firstName')

        expected = "SELECT [Id] as [id] FROM [Fighters] as [fighters] " +
            "ORDER BY [FirstName] ASC"

        assert(s, expected)


    it 'aliases SQL * operator', ->
        s = sql.select('*').from('events')

        expected = "SELECT [Id] as [id], [Name] as [name], " +
            "[Date] as [date], [PromotionId] as [promotionId] FROM [Events] as [events]"

        assert(s, expected)


    it 'can build the JOIN predicate for a child', ->
        q = sql.select('name', 'winnerId').from('events').join('fights')

        e = "SELECT [Name] as [name], [WinnerId] as [winnerId] FROM [Events] as [events] " +
            "INNER JOIN [Fights] as [fights] ON ([fights].[EventId] = [events].[Id])"

        assert(q, e)


    it 'can build the JOIN predicate for a parent', ->
        q = sql.select('name', 'winnerId').from('fights').join('events')

        e = "SELECT [Name] as [name], [WinnerId] as [winnerId] FROM [Fights] as [fights] " +
            "INNER JOIN [Events] as [events] ON ([fights].[EventId] = [events].[Id])"

        assert(q, e)
