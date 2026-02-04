-- Azure SQL / SQL Server (T-SQL)
-- Sistema de Análisis de Ventas con Proceso ETL
-- Autor: Anfeerny Martínez (2016-3740)

IF DB_ID('SalesAnalytics') IS NULL
BEGIN
    CREATE DATABASE SalesAnalytics;
END
GO

USE SalesAnalytics;
GO

-- Tabla opcional para linaje / fuente de datos
IF OBJECT_ID('dbo.DataSources', 'U') IS NOT NULL DROP TABLE dbo.DataSources;
CREATE TABLE dbo.DataSources (
    SourceID INT IDENTITY(1,1) PRIMARY KEY,
    SourceName NVARCHAR(100) NOT NULL,
    SourceType NVARCHAR(50) NOT NULL,   -- CSV / API / DB
    SourcePath NVARCHAR(260) NULL,      -- ruta o identificador
    LoadedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

-- Dimensión Clientes
IF OBJECT_ID('dbo.Customers', 'U') IS NOT NULL DROP TABLE dbo.Customers;
CREATE TABLE dbo.Customers (
    CustomerID INT NOT NULL PRIMARY KEY,
    FirstName NVARCHAR(80) NOT NULL,
    LastName NVARCHAR(80) NOT NULL,
    Email NVARCHAR(120) NULL,
    Phone NVARCHAR(50) NULL,
    City NVARCHAR(80) NULL,
    Country NVARCHAR(80) NULL,
    SourceID INT NULL,
    CONSTRAINT FK_Customers_DataSources FOREIGN KEY (SourceID) REFERENCES dbo.DataSources(SourceID)
);

-- Dimensión Productos
IF OBJECT_ID('dbo.Products', 'U') IS NOT NULL DROP TABLE dbo.Products;
CREATE TABLE dbo.Products (
    ProductID INT NOT NULL PRIMARY KEY,
    ProductName NVARCHAR(150) NOT NULL,
    Category NVARCHAR(80) NULL,
    Price DECIMAL(10,2) NOT NULL CHECK (Price >= 0),
    Stock INT NULL CHECK (Stock >= 0),
    SourceID INT NULL,
    CONSTRAINT FK_Products_DataSources FOREIGN KEY (SourceID) REFERENCES dbo.DataSources(SourceID)
);

-- Encabezado de facturas / pedidos
IF OBJECT_ID('dbo.Orders', 'U') IS NOT NULL DROP TABLE dbo.Orders;
CREATE TABLE dbo.Orders (
    OrderID INT NOT NULL PRIMARY KEY,
    CustomerID INT NOT NULL,
    OrderDate DATE NOT NULL,
    Status NVARCHAR(30) NULL,
    SourceID INT NULL,
    CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerID) REFERENCES dbo.Customers(CustomerID),
    CONSTRAINT FK_Orders_DataSources FOREIGN KEY (SourceID) REFERENCES dbo.DataSources(SourceID)
);

-- Detalle de facturas / ventas (líneas)
IF OBJECT_ID('dbo.OrderDetails', 'U') IS NOT NULL DROP TABLE dbo.OrderDetails;
CREATE TABLE dbo.OrderDetails (
    OrderDetailID INT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT NOT NULL,
    ProductID INT NOT NULL,
    Quantity INT NOT NULL CHECK (Quantity > 0),
    UnitPrice DECIMAL(10,2) NOT NULL CHECK (UnitPrice >= 0),
    LineTotal AS (CAST(Quantity AS DECIMAL(18,2)) * UnitPrice) PERSISTED,
    SourceID INT NULL,
    CONSTRAINT FK_OrderDetails_Orders FOREIGN KEY (OrderID) REFERENCES dbo.Orders(OrderID),
    CONSTRAINT FK_OrderDetails_Products FOREIGN KEY (ProductID) REFERENCES dbo.Products(ProductID),
    CONSTRAINT FK_OrderDetails_DataSources FOREIGN KEY (SourceID) REFERENCES dbo.DataSources(SourceID)
);

-- Índices recomendados para análisis
CREATE INDEX IX_Orders_OrderDate ON dbo.Orders(OrderDate);
CREATE INDEX IX_OrderDetails_OrderID ON dbo.OrderDetails(OrderID);
CREATE INDEX IX_OrderDetails_ProductID ON dbo.OrderDetails(ProductID);
GO