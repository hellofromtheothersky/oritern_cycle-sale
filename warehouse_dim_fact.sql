CREATE TABLE [df].[fact_SaleProduct] (
  [ProductKey] int,
  [SpeicalOfferKey] int,
  [CustomerKey] int,
  [SalesPersonKey] int,
  [DateKey] int,
  [OrderDateKey] int,
  [ShipDateKey] int,
  [DueDateKey] int,
  [TerritoryKey] int,
  [ShipMethodKey] int,
  [OrderQty] smallint,
  [UnitPrice] money,
  [UnitPriceDiscount] money,
  [LineTotal] numeric(38, 6)
)
GO

CREATE TABLE [df].[fact_SaleHeader] (
  [CustomerKey] int,
  [SalesPersonKey] int,
  [DateKey] int,
  [OrderDateKey] int,
  [ShipDateKey] int,
  [DueDateKey] int,
  [TerritoryKey] int,
  [ShipMethodKey] int,
  [SubTotal] money,
  [TaxAmt] money,
  [Freight] money,
  [TotalDue] money
)
GO

CREATE TABLE [df].[fact_purchasing] (
  [ProductKey] int,
  [VendorKey] int,
  [PurchaseOrderedDateKey] int,
  [PurchaseReceivedDateKey] int,
  [PurchaseRejectedDateKey] int,
  [PurchaseStockedDateKey] int,
  [PurchaseOrderedQty] int,
  [PurchaseReceivedQty] int,
  [PurchaseRejectedQty] int,
  [PurchaseStockedQty] int
)
GO

CREATE TABLE [df].[fact_stock] (
  [ProductKey] int,
  [DateKey] Int,
  [QuantityIn] int,
  [QuantityOut] int,
)
GO

CREATE TABLE [df].[dim_Date] (
  [DateKey] int,
  [TheDate] date,
  [TheDay] smallint,
  [TheDaySuffix] char(2),
  [TheDayName] varchar(20),
  [TheDayOfWeek] smallint,
  [TheDayOfWeekInMonth] smallint,
  [TheDayOfYear] smallint,
  [IsWeekend] smallint,
  [TheWeek] smallint,
  [TheISOweek] smallint,
  [TheFirstOfWeek] date,
  [TheLastOfWeek] date,
  [TheWeekOfMonth] smallint,
  [TheMonth] smallint,
  [TheMonthName] varchar(20),
  [TheFirstOfMonth] date,
  [TheLastOfMonth] date,
  [TheFirstOfNextMonth] date,
  [TheLastOfNextMonth] date,
  [TheQuarter] smallint,
  [TheFirstOfQuarter] date,
  [TheLastOfQuarter] date,
  [TheYear] smallint,
  [TheISOYear] smallint,
  [TheFirstOfYear] date,
  [TheLastOfYear] date,
  [IsLeapYear] bit
)
GO

CREATE TABLE [df].[dim_Location] (
  [LocationKey] int IDENTITY(1, 1),
  [LocationID] smallint,
  [Name] nvarchar(50),
  [CostRate] smallmoney,
  [Availability] decimal(8,2),
  [start_date] datetime,
  [end_date] datetime,
  [is_current] bit
)
GO

CREATE TABLE [df].[dim_Vendor] (
  [VendorKey] int IDENTITY(1, 1),
  [VendorID] int,
  [AccountNumber] nvarchar(15),
  [Name] nvarchar(50),
  [CreditRating] tinyint,
  [PreferredVendorStatus] bit,
  [ActiveFlag] bit,
  [PurchasingWebServiceURL] nvarchar(1024),
  [start_date] datetime,
  [end_date] datetime,
  [is_current] bit
)
GO

CREATE TABLE [df].[dim_Product] (
  [ProductKey] int IDENTITY(1, 1),
  [ProductID] int,
  [Subcategory] varchar(100),
  [Category] varchar(100),
  [Name] nvarchar(50),
  [ProductNumber] nvarchar(25),
  [MakeFlag] bit,
  [FinishedGoodsFlag] bit,
  [Color] nvarchar(15),
  [SafetyStockLevel] smallint,
  [ReorderPoint] smallint,
  [Size] nvarchar(5),
  [SizeUnitMeasureCode] nchar(3),
  [WeightUnitMeasureCode] nchar(3),
  [Weight] decimal(8, 2),
  [DaysToManufacture] int,
  [ProductLine] nchar(2),
  [Class] nchar(2),
  [Style] nchar(2),
  [ProductSubcategory] varchar(100),
  [ProductCategory] varchar(100),
  [ProductModelName] varchar(100),
  [SellStartDate] datetime,
  [SellEndDate] datetime,
  [DiscontinuedDate] datetime,
  [start_date] datetime,
  [end_date] datetime,
  [is_current] bit
)
GO

CREATE TABLE [df].[dim_SalesTerritory] (
  [TerritoryKey] int IDENTITY(1, 1),
  [TerritoryID] int,
  [Name] nvarchar(50),
  [CountryRegionCode] nvarchar(3),
  [Group] nvarchar(50),
  [start_date] datetime,
  [end_date] datetime,
  [is_current] bit
)
GO

CREATE TABLE [df].[dim_SpeciaOffer] (
  [SpecialOfferKey] int IDENTITY(1, 1),
  [SpecialOfferID] int,
  [Description] nvarchar(255),
  [DiscountPct] smallmoney,
  [Type] nvarchar(50),
  [Category] nvarchar(50),
  [StartDate] datetime,
  [EndDate] datetime,
  [MinQty] int,
  [MaxQty] int,
  [start_date] datetime,
  [end_date] datetime,
  [is_current] bit
)
GO

CREATE TABLE [df].[dim_ShipMethod] (
  [ShipMethodKey] int IDENTITY(1, 1),
  [ShipMethodID] int,
  [Name] nvarchar(50),
  [ShipBase] money,
  [ShipRate] money,
  [start_date] datetime,
  [end_date] datetime,
  [is_current] bit
)
GO

CREATE TABLE [df].[dim_Customer] (
  [CustomerKey] int IDENTITY(1, 1),
  [BusinessEntityID] INT,
  [PersonType] VARCHAR(5),
  [ModifiedDate] DATETIME,
  [FirstName] VARCHAR(50),
  [MiddleName] VARCHAR(50),
  [LastName] VARCHAR(50),
  [AddressLine1] VARCHAR(100),
  [City] VARCHAR(50),
  [PostalCode] VARCHAR(10),
  [AddressName] VARCHAR(50),
  [StateProvinceCode] VARCHAR(10),
  [CountryRegionCode] VARCHAR(10),
  [StateProvinceName] VARCHAR(50),
  [start_date] datetime,
  [end_date] datetime,
  [is_current] bit
)
GO

CREATE TABLE [df].[dim_SalesPerson] (
  [SalesPersonKey] int IDENTITY(1, 1),
  [BusinessEntityID] INT,
  [PersonType] VARCHAR(5),
  [ModifiedDate] DATETIME,
  [FirstName] VARCHAR(50),
  [MiddleName] VARCHAR(50),
  [LastName] VARCHAR(50),
  [AddressLine1] VARCHAR(100),
  [City] VARCHAR(50),
  [PostalCode] VARCHAR(20),
  [AddressName] VARCHAR(50),
  [StateProvinceCode] VARCHAR(10),
  [CountryRegionCode] VARCHAR(10),
  [StateProvinceName] VARCHAR(50),
  [start_date] datetime,
  [end_date] datetime,
  [is_current] bit
)
GO

--VIEW
--using to select data from one or many stg tables with the needed column with right column name corresponding to the dim table
create or alter function stg.f_fact_SaleProduct(@last_load_run datetime = '1753-01-01 00:00:00')
RETURNS TABLE
as
RETURN
	with sale as (
		select 
			sd.SalesOrderID,
			sd.ProductID,
			sd.SpecialOfferID,
			sh.CustomerID,
			sh.SalesPersonID,
			sh.OrderDate,
			sh.ShipDate,
			sh.DueDate,
			sh.TerritoryID,
			sh.ShipMethodID,
			sd.[OrderQty],
			sd.[UnitPrice],
			sd.[UnitPriceDiscount],
			sd.[LineTotal],
			(SELECT MAX([ModifiedDate])
			FROM (VALUES (sd.[ModifiedDate]),(sh.[ModifiedDate])) AS [ModifiedDate]) 
			as [ModifiedDate]
		from stg.Sales_SalesOrderDetail sd 
		inner join stg.Sales_SalesOrderHeader sh on sd.SalesOrderID=sh.SalesOrderID 
	)
	select
		[ProductKey],
		[SpecialOfferKey],
		[CustomerKey],
		[SalesPersonKey],
		dbo.YYYYMMDD_int_format(sale.ModifiedDate) as [DateKey],
		dbo.YYYYMMDD_int_format(sale.OrderDate) as [OrderDateKey],
		dbo.YYYYMMDD_int_format(sale.ShipDate) as [ShipDateKey],
		dbo.YYYYMMDD_int_format(sale.DueDate) as [DueDateKey],
		[TerritoryKey],
		[ShipMethodKey],
		[OrderQty],
		[UnitPrice],
		[UnitPriceDiscount],
		[LineTotal]
	from sale
	left join df.dim_Product d_pro on sale.ProductID=d_pro.ProductID and d_pro.is_current=1 and sale.[ModifiedDate]>@last_load_run
	left join df.dim_SpeciaOffer d_offer on sale.SpecialOfferID=d_offer.SpecialOfferID and d_pro.is_current=1
	left join df.dim_Customer d_cus on sale.CustomerID=d_cus.BusinessEntityID and d_cus.is_current=1
	left join df.dim_SalesPerson d_saleper on sale.SalesPersonID=d_saleper.BusinessEntityID and d_saleper.is_current=1
	left join df.dim_SalesTerritory dim_ter on sale.TerritoryID=dim_ter.TerritoryID and dim_ter.is_current=1
	left join df.dim_ShipMethod dim_ship on sale.ShipMethodID=dim_ship.ShipMethodID and dim_ship.is_current=1
GO

create or alter function stg.f_fact_SaleHeader(@last_load_run datetime = '1753-01-01 00:00:00')
RETURNS TABLE
as
RETURN
	select
		[CustomerKey], 
		[SalesPersonKey],
		dbo.YYYYMMDD_int_format(sh.ModifiedDate) as [DateKey],
		dbo.YYYYMMDD_int_format(sh.OrderDate) as [OrderDateKey],
		dbo.YYYYMMDD_int_format(sh.ShipDate) as [ShipDateKey],
		dbo.YYYYMMDD_int_format(sh.DueDate) as [DueDateKey],
		[TerritoryKey],
		[ShipMethodKey],
		[SubTotal],
		[TaxAmt],
		[Freight],
		[TotalDue]
	from stg.Sales_SalesOrderHeader sh 
	left join df.dim_Customer d_cus on sh.CustomerID=d_cus.BusinessEntityID and d_cus.is_current=1 and sh.ModifiedDate>@last_load_run
	left join df.dim_SalesPerson d_saleper on sh.SalesPersonID=d_saleper.BusinessEntityID and d_saleper.is_current=1
	left join df.dim_SalesTerritory dim_ter on sh.TerritoryID=dim_ter.TerritoryID and dim_ter.is_current=1
	left join df.dim_ShipMethod dim_ship on sh.ShipMethodID=dim_ship.ShipMethodID and dim_ship.is_current=1

GO

create or alter function stg.f_fact_stock(@last_load_run datetime = '1753-01-01 00:00:00')
RETURNS TABLE
as
RETURN
	--export
	with order_status as(
		select 
			[SalesOrderID], 
			[status], 
			LAG([status]) OVER(partition by SalesOrderID ORDER BY ModifiedDate) as [pre_status],
			[ModifiedDate]
		from stg.Sales_SalesOrderHeader
		where is_current=1 and [ModifiedDate]>@last_load_run
	),
	order_status_updated_into_shipped as( --which order id have update status to shipped 
		select [SalesOrderID], [ModifiedDate]
		from order_status
		where [status]=5 and [status]<>COALESCE([pre_status], -1)
	),
	export as(
	select
		ProductID, 
		SUM(OrderQty) as 'QuantityOut',
		MAX(sta.ModifiedDate) as 'DateExport'
	from stg.Sales_SalesOrderDetail ordetail 
		inner join order_status_updated_into_shipped sta 
		on ordetail.SalesOrderID=sta.SalesOrderID
	group by ProductID
	),

	--import
	purchasing_status as(
		select 
			[PurchaseOrderID], 
			[status], 
			LAG([status]) OVER(partition by PurchaseOrderID ORDER BY ModifiedDate) as [pre_status],
			[ModifiedDate] 
		from stg.Purchasing_PurchaseOrderHeader
		where is_current=1 and [ModifiedDate]>@last_load_run
	),
	purchasing_status_updated_into_completed as(
		select [PurchaseOrderID], [ModifiedDate]
		from purchasing_status
		where [status]=4 and [status]<>COALESCE([pre_status], -1)
	),
	import as(
	select 
		ProductID, 
		SUM(OrderQty) as 'QuantityIn',
		MAX(sta.ModifiedDate) as 'DateImport'
	from stg.Purchasing_PurchaseOrderDetail ordetail 
		inner join purchasing_status_updated_into_completed sta 
		on ordetail.PurchaseOrderID=sta.PurchaseOrderID
	group by ProductID
	)
	select
		ProductKey,
		dbo.YYYYMMDD_int_format((SELECT MAX([ModifiedDate])
			FROM (VALUES (DateExport),(DateImport)) AS value([ModifiedDate])) )
		as [DateKey],
		QuantityOut, 
		QuantityIn
	from export 
	full outer join import on export.ProductID=import.ProductID
	left join df.dim_Product d_pro on COALESCE(import.ProductID, export.ProductID)=d_pro.ProductID and d_pro.is_current=1
GO


create or alter view stg.v_dim_Product
	as
	select 
		[ProductID], 
		p.[name] as [Name], 
		[ProductNumber], 
		[MakeFlag], 
		[FinishedGoodsFlag], 
		[Color], 
		[SafetyStockLevel], 
		[ReorderPoint], 
		[Size], 
		[SizeUnitMeasureCode], 
		[WeightUnitMeasureCode], 
		[Weight], 
		[DaysToManufacture], 
		[ProductLine], 
		[Class], 
		[Style], 
		psc.[name] as [ProductSubcategory], 
		pc.[name] as [ProductCategory], 
		pm.[name] as [ProductModelName], 
		[SellStartDate], 
		[SellEndDate], 
		[DiscontinuedDate],
		(SELECT MAX([ModifiedDate])
		FROM (VALUES (p.[ModifiedDate]),(psc.[ModifiedDate]),(pc.[ModifiedDate])) AS [ModifiedDate]) 
		as [ModifiedDate]
	from stg.Production_Product p 
	left join stg.Production_ProductSubcategory psc on p.ProductSubcategoryID=psc.ProductSubcategoryID and psc.is_current=1
	left join stg.Production_ProductCategory pc on psc.ProductCategoryID=pc.ProductCategoryID and pc.is_current=1
	left join stg.Production_ProductModel pm on p.ProductModelID=pm.ProductModelID and pc.is_current=1
	where p.is_current=1
GO


create or alter view stg.v_dim_SalesTerritory
As
	select
	  [TerritoryID],
	  [Name],
	  [CountryRegionCode],
	  [Group],
	  [ModifiedDate]
	from [stg].[Sales_SalesTerritory]
	where is_current=1
GO


create or alter view stg.v_dim_SpeciaOffer
as
	Select
	  [SpecialOfferID],
	  [Description],
	  [DiscountPct],
	  [Type],
	  [Category],
	  [StartDate],
	  [EndDate],
	  [MinQty],
	  [MaxQty],
	  [ModifiedDate]
	from [stg].[Sales_SpecialOffer]
	where is_current=1
GO


create or alter view stg.v_dim_ShipMethod 
as
	select
	  [ShipMethodID],
	  [Name],
	  [ShipBase],
	  [ShipRate]
	  [ModifiedDate]
	from [stg].[Purchasing_ShipMethod]
	where is_current=1
GO


create or alter view stg.v_dim_Customer
as
	select
	  [BusinessEntityID],
	  [PersonType],
	  [FirstName],
	  [MiddleName],
	  [LastName],
	  [AddressLine1],
	  [City],
	  [PostalCode],
	  [AddressName],
	  [StateProvinceCode],
	  [CountryRegionCode],
	  [StateProvinceName],
	  [ModifiedDate]
  	from [stg].[Person-IndividualCustomer]
	where is_current=1
GO


create or alter view stg.v_dim_SalesPerson
as
	select
	  [BusinessEntityID],
	  [PersonType],
	  [FirstName],
	  [MiddleName],
	  [LastName],
	  [AddressLine1],
	  [City],
	  [PostalCode],
	  [AddressName],
	  [StateProvinceCode],
	  [CountryRegionCode],
	  [StateProvinceName],
	  [ModifiedDate]
  	from [stg].[Person-SalesPerson]
	where is_current=1
GO


create or alter view stg.v_dim_Location
as
	select
	  [LocationID],
	  [Name],
	  [CostRate],
	  [Availability],
	  [ModifiedDate]
  	from [stg].[Production_Location]
	where is_current=1
GO

