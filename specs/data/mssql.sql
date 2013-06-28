CREATE TABLE dbo.Promotions (
	Id int NOT NULL IDENTITY(1,1) CONSTRAINT PK_Promotions PRIMARY KEY CLUSTERED,
	Name varchar(100) NOT NULL,
	CONSTRAINT UQ_Promotions_Name UNIQUE NONCLUSTERED (Name)
)

CREATE TABLE dbo.Fighters (
	Id int NOT NULL IDENTITY(1,1) CONSTRAINT PK_Fighters PRIMARY KEY CLUSTERED,
	FirstName varchar(100) NOT NULL,
	LastName varchar(100) NOT NULL,
	DOB datetime NOT NULL,
	Country varchar(100) NOT NULL,
	HeightInCm int NOT NULL,
	ReachInCm int NOT NULL,
	WeightInLb int NOT NULL,
	
	-- not realistic, just for unit testing
	CONSTRAINT UQ_Fighters_LastName_FirstName UNIQUE NONCLUSTERED (LastName, FirstName)
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
	
	WeightInLb int NOT NULL,
	TookPlace bit NOT NULL,
	EarlyStoppage bit NOT NULL,
	TitleFight bit NOT NULL,
	CatchWeight bit NOT NULL,
	Knockout bit NOT NULL,
	Submission bit NOT NULL,
	Draw bit NOT NULL,
	
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
	FightId int NOT NULL,
	Number int NOT NULL,
	
	FinalRound bit NOT NULL,
	ScheduledDuration int NOT NULL,
	ActualDuration int NOT NULL,
	EarlyStoppage bit NULL,
	
	CONSTRAINT PK_ROUNDS PRIMARY KEY CLUSTERED (FightId, Number),
	CONSTRAINT FK_Rounds_Fights FOREIGN KEY (FightId) REFERENCES Fights,
)

CREATE TABLE dbo.Images (
    Id int NOT NULL IDENTITY(1,1) CONSTRAINT PK_Images PRIMARY KEY CLUSTERED,
    ImageName varchar(100 NOT NULL,
    ImageFile image NOT NULL
)
