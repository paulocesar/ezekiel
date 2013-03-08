CREATE TABLE dbo.Customers (
	Id int NOT NULL IDENTITY(1,1) CONSTRAINT PK_Customers PRIMARY KEY CLUSTERED,
	FirstName varchar(100) NOT NULL,
	LastName varchar(100) NOT NULL
)

CREATE TABLE dbo.Products (
	Id int NOT NULL IDENTITY(1,1) CONSTRAINT PK_Products PRIMARY KEY CLUSTERED,
	ProductName varchar(200) NOT NULL,
	
	CONSTRAINT UQ_Products_ProductName UNIQUE NONCLUSTERED (ProductName)
)

CREATE TABLE dbo.Orders (
	Id int NOT NULL IDENTITY(1,1) CONSTRAINT PK_Orders PRIMARY KEY CLUSTERED,
	CustomerId int NOT NULL,
	CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerId) REFERENCES Customers,
	OrderDate datetime NOT NULL
)

CREATE TABLE dbo.OrderLines (
	Id int NOT NULL IDENTITY(1,1) CONSTRAINT PK_OrderLines PRIMARY KEY CLUSTERED,
	
	OrderId int NOT NULL,
	CONSTRAINT FK_OrdersLines_Orders FOREIGN KEY (OrderId) REFERENCES Orders,
	LineNumber int NOT NULL,
	CONSTRAINT UQ_OrderId_LineId UNIQUE NONCLUSTERED (OrderId, LineNumber),
	
	ProductId int NOT NULL,
	CONSTRAINT FK_OrderLines_Products FOREIGN KEY (ProductId) REFERENCES Products
)
