CREATE TABLE dbo.Promotions (
	Id int NOT NULL IDENTITY(1,1) CONSTRAINT PK_Promotions PRIMARY KEY CLUSTERED,
	Name varchar(100) NOT NULL,
)

CREATE TABLE dbo.Fighters (
	Id int NOT NULL IDENTITY(1,1) CONSTRAINT PK_Fighters PRIMARY KEY CLUSTERED,
	FirstName varchar(100) NOT NULL,
	LastName varchar(100) NOT NULL,
	Nickname varchar(100) NOT NULL,
	DOB datetime NOT NULL,
	
	CONSTRAINT UQ_Fighters_LastName_FirstName_NickName UNIQUE NONCLUSTERED (LastName, FirstName, Nickname)
)

CREATE TABLE dbo.Events (
	Id int NOT NULL IDENTITY(1,1) CONSTRAINT PK_Events PRIMARY KEY CLUSTERED,
	Name varchar(100) NOT NULL,
	Date datetime NOT NULL,
	PromotionId int NOT NULL,

	CONSTRAINT FK_Events_Promotions FOREIGN KEY (PromotionId) REFERENCES Promotions,
)

CREATE TABLE dbo.Fights (
	Id int NOT NULL IDENTITY(1,1) CONSTRAINT PK_Fights PRIMARY KEY CLUSTERED,
	EventId int NOT NULL,
	
	TookPlace bit NOT NULL,
	EarlyStoppage bit NULL,
	
	WinnerId int NULL,
	LoserId int NULL,
	
	DefendingFighterId int NOT NULL,
	ContendingFighterId int NOT NULL,

	CONSTRAINT FK_Fights_Events FOREIGN KEY (EventId) REFERENCES Events,
	CONSTRAINT FK_Fights_Fighters_DefendingFighter FOREIGN KEY (DefendingFighterId) REFERENCES Fighters,
	CONSTRAINT FK_Fights_Fighters_ContendingFighter FOREIGN KEY (ContendingFighterId) REFERENCES Fighters,
	CONSTRAINT FK_Fights_Fighters_Winner FOREIGN KEY (WinnerId) REFERENCES Fighters,
	CONSTRAINT FK_Fights_Fighters_Loser FOREIGN KEY (LoserId) REFERENCES Fighters,
)

CREATE TABLE dbo.Rounds (
	Id int NOT NULL IDENTITY(1,1) CONSTRAINT PK_Rounds PRIMARY KEY CLUSTERED,
	FightId int NOT NULL,
	Number int NOT NULL,
	
	FinalRound bit NOT NULL,
	ScheduledDuration int NOT NULL,
	ActualDuration int NOT NULL,
	EarlyStoppage bit NULL,
	
	CONSTRAINT FK_Rounds_Fights FOREIGN KEY (FightId) REFERENCES Fights,
	CONSTRAINT UQ_Rounds_FightId_Number UNIQUE NONCLUSTERED (FightId, Number)
)
