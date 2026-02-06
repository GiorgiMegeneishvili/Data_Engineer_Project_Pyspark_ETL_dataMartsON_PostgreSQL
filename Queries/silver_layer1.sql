

---- Creation Of Sivler Tables in stored Procedure
CREATE OR REPLACE PROCEDURE silver_l.silver_data_marts_Table_creation()
LANGUAGE plpgsql
AS $$
BEGIN

		drop table  if EXISTS silver_l.FactSales cascade;
		drop table  if EXISTS silver_l.DimDate cascade;
		drop table  if EXISTS silver_l.DimTerritory cascade;
		drop table  if EXISTS silver_l.DimProduct cascade;
		drop table  if EXISTS silver_l.DimCustomer cascade;
		
		/* SQLINES DEMO *** ==========
		   DIM DATEbronze_layer_Table_creation;
		========================= */
		-- SQLINES FOR EVALUATION USE ONLY (14 DAYS)
		CREATE TABLE silver_l.DimDate
		(
		    DateKey       INT         NOT NULL,
		    FullDate      DATE        NOT NULL,
		    YearNumber    SMALLINT    NOT NULL,
		    QuarterNumber SMALLINT     NOT NULL,
		    MonthNumber   SMALLINT     NOT NULL,
		    MonthName     VARCHAR(10) NOT NULL,
		    DayNumber     SMALLINT     NOT NULL,
		    DayName       VARCHAR(10) NOT NULL,
		    IsWeekend     BOOLEAN         NOT NULL,
		
		    CONSTRAINT PK_DimDate PRIMARY KEY (DateKey),
		    CONSTRAINT UQ_DimDate UNIQUE (FullDate)
		);
		
		/* SQLINES DEMO *** ==========
		   DIM TERRITORY
		========================= */
		CREATE TABLE silver_l.DimTerritory
		(
		    TerritoryKey INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		    TerritoryID  INT NOT NULL,
		    Region       VARCHAR(50),
		    Country      VARCHAR(50),
		    "Group"      VARCHAR(50)
		);
		
		/* SQLINES DEMO *** ==========
		   DIM PRODUCT
		========================= */
		CREATE TABLE silver_l.DimProduct
		(
		    ProductKey     INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		    ProductID      INT NOT NULL,
		    ProductName    VARCHAR(100),
		    ProductNumber  VARCHAR(50),
		    Category       VARCHAR(50),
		    SubCategory    VARCHAR(50),
		    Color          VARCHAR(20),
		    Size           VARCHAR(10)
		);
		
		/* SQLINES DEMO *** ==========
		   DIM CUSTOMER
		========================= */
		CREATE TABLE silver_l.DimCustomer
		(
		    CustomerKey INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		    CustomerID  INT NOT NULL,
		    FullName    VARCHAR(150),
		    Email       VARCHAR(100),
		    City        VARCHAR(50),
		    Country     VARCHAR(50)
		);
		
		/* SQLINES DEMO *** ==========
		   FACT SALES
		========================= */
		CREATE TABLE silver_l.FactSales
		(
		    SalesKey        INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		    DateKey         INT NOT NULL,
		    ProductKey      INT NOT NULL,
		    CustomerKey     INT NOT NULL,
		    TerritoryKey    INT NOT NULL,
		    OrderQty        INT,
		    UnitPrice       MONEY,
		    SalesAmount     MONEY,
		
		    CONSTRAINT FK_Fact_Date
		        FOREIGN KEY (DateKey) REFERENCES silver_l.DimDate(DateKey),
		
		    CONSTRAINT FK_Fact_Product
		        FOREIGN KEY (ProductKey) REFERENCES silver_l.DimProduct(ProductKey),
		
		    CONSTRAINT FK_Fact_Customer
		        FOREIGN KEY (CustomerKey) REFERENCES silver_l.DimCustomer(CustomerKey),
		
		    CONSTRAINT FK_Fact_Territory
		        FOREIGN KEY (TerritoryKey) REFERENCES silver_l.DimTerritory(TerritoryKey)
		);


END;
$$;


CALL silver_l.silver_data_marts_Table_creation();
