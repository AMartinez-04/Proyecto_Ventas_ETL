PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS OrderDetails;
DROP TABLE IF EXISTS Orders;
DROP TABLE IF EXISTS Products;
DROP TABLE IF EXISTS Customers;
DROP TABLE IF EXISTS DataSources;

CREATE TABLE DataSources(
  SourceID INTEGER PRIMARY KEY AUTOINCREMENT,
  SourceName TEXT NOT NULL,
  SourceType TEXT NOT NULL,
  SourcePath TEXT,
  LoadedAt TEXT NOT NULL
);

CREATE TABLE Customers(
  CustomerID INTEGER PRIMARY KEY,
  FirstName TEXT NOT NULL,
  LastName TEXT NOT NULL,
  Email TEXT,
  Phone TEXT,
  City TEXT,
  Country TEXT,
  SourceID INTEGER,
  FOREIGN KEY(SourceID) REFERENCES DataSources(SourceID)
);

CREATE TABLE Products(
  ProductID INTEGER PRIMARY KEY,
  ProductName TEXT NOT NULL,
  Category TEXT,
  Price REAL NOT NULL CHECK(Price >= 0),
  Stock INTEGER CHECK(Stock >= 0),
  SourceID INTEGER,
  FOREIGN KEY(SourceID) REFERENCES DataSources(SourceID)
);

CREATE TABLE Orders(
  OrderID INTEGER PRIMARY KEY,
  CustomerID INTEGER NOT NULL,
  OrderDate TEXT NOT NULL,
  Status TEXT,
  SourceID INTEGER,
  FOREIGN KEY(CustomerID) REFERENCES Customers(CustomerID),
  FOREIGN KEY(SourceID) REFERENCES DataSources(SourceID)
);

CREATE TABLE OrderDetails(
  OrderDetailID INTEGER PRIMARY KEY AUTOINCREMENT,
  OrderID INTEGER NOT NULL,
  ProductID INTEGER NOT NULL,
  Quantity INTEGER NOT NULL CHECK(Quantity > 0),
  UnitPrice REAL NOT NULL CHECK(UnitPrice >= 0),
  LineTotal REAL NOT NULL,
  SourceID INTEGER,
  FOREIGN KEY(OrderID) REFERENCES Orders(OrderID),
  FOREIGN KEY(ProductID) REFERENCES Products(ProductID),
  FOREIGN KEY(SourceID) REFERENCES DataSources(SourceID)
);

CREATE INDEX IX_Orders_OrderDate ON Orders(OrderDate);
CREATE INDEX IX_OrderDetails_OrderID ON OrderDetails(OrderID);
CREATE INDEX IX_OrderDetails_ProductID ON OrderDetails(ProductID);
