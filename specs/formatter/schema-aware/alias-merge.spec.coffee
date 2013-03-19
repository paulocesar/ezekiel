h = require('../../test-helper')
{ SqlToken } = sql = h.requireSrc('sql')

assert = (sql, e, debug = false) -> h.assertAlias(sql, e, debug)

schema = h.getCookedSchema()
tables = schema.tablesByMany
fighters = tables.fighters

expectedMerge = """
CREATE TABLE [#BulkMerge] (
  [FirstName] varchar(100) NOT NULL,
  [LastName] varchar(100) NOT NULL,
  [DOB] datetime NOT NULL,
  [Country] varchar(100) NOT NULL,
  [HeightInCm] int NOT NULL,
  [ReachInCm] int NOT NULL,
  [WeightInLb] int NOT NULL,
  PRIMARY KEY CLUSTERED ([LastName], [FirstName])
);

INSERT [#BulkMerge] (firstName,lastName,dOB,country,heightInCm,reachInCm,weightInLb) VALUES
('Anderson','Silva','1975-04-14','Brazil',188,197,185),
('Wanderlei','Silva','1976-07-02','Brazil',180,188,204),
('Jon','Jones','1987-07-19','USA',193,215,205),
('Cain','Velasquez','1982-07-28','USA',185,196,240);

MERGE [Fighters] as target
USING [#BulkMerge] as source
ON (target.[LastName] = source.[LastName] AND target.[FirstName] = source.[FirstName])
WHEN MATCHED THEN
  UPDATE SET target.[DOB] = source.[DOB], target.[Country] = source.[Country], target.[HeightInCm] = source.[HeightInCm], target.[ReachInCm] = source.[ReachInCm], target.[WeightInLb] = source.[WeightInLb]
WHEN NOT MATCHED THEN
  INSERT ([FirstName], [LastName], [DOB], [Country], [HeightInCm], [ReachInCm], [WeightInLb])
  VALUES (source.[FirstName], source.[LastName], source.[DOB], source.[Country], source.[HeightInCm], source.[ReachInCm], source.[WeightInLb]);
DROP TABLE [#BulkMerge];"""

describe 'SqlMerge with aliased schema', () ->
    it 'does a basic merge', ->
        rows = h.testData.newData()
        s = sql.merge('fighters').using(rows)
        assert(s, expectedMerge)
