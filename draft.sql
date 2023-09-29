SELECT 'DROP TABLE ' + '[' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']'
FROM INFORMATION_SCHEMA.TABLES
where TABLE_SCHEMA='DF'
ORDER BY TABLE_SCHEMA, TABLE_NAME

SELECT 'SELECT * FROM' + '[' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']'
FROM INFORMATION_SCHEMA.TABLES
where TABLE_SCHEMA='DF'
ORDER BY TABLE_SCHEMA, TABLE_NAME


DROP TABLE [df].[dim_Customer]
DROP TABLE [df].[dim_Date]
DROP TABLE [df].[dim_Product]
DROP TABLE [df].[dim_SalesPerson]
DROP TABLE [df].[dim_SalesTerritory]
DROP TABLE [df].[dim_ShipMethod]
DROP TABLE [df].[dim_SpeciaOffer]
DROP TABLE [df].[fact_SaleHeader]
DROP TABLE [df].[fact_SaleProduct]
DROP TABLE [df].[dim_Location]
DROP TABLE [df].[dim_Vendor]
DROP TABLE [df].[fact_purchasing]
DROP TABLE [df].[fact_stock]

SELECT * FROM[df].[dim_Customer]
SELECT * FROM[df].[dim_Date]
SELECT * FROM[df].[dim_Location]
SELECT * FROM[df].[dim_Product]
SELECT * FROM[df].[dim_SalesPerson]
SELECT * FROM[df].[dim_SalesTerritory]
SELECT * FROM[df].[dim_ShipMethod]
SELECT * FROM[df].[dim_SpeciaOffer]
SELECT * FROM[df].[dim_Vendor]
SELECT * FROM[df].[fact_purchasing]
SELECT * FROM[df].[fact_SaleHeader]
SELECT * FROM[df].[fact_SaleProduct]
SELECT * FROM[df].[fact_stock]


TRUNCATE TABLE [df].[dim_Customer]
TRUNCATE TABLE [df].[dim_Date]
TRUNCATE TABLE [df].[dim_Product]
TRUNCATE TABLE [df].[dim_SalesPerson]
TRUNCATE TABLE [df].[dim_SalesTerritory]
TRUNCATE TABLE [df].[dim_ShipMethod]
TRUNCATE TABLE [df].[dim_SpeciaOffer]

select * from stg.Sales_SalesOrderHeader a inner join [df].[dim_Date] b on a.DueDate=b.TheDate
select * from [stg].[Sales_SalesOrderHeader] a, [df].[dim_SalesPerson] b where a.SalesPersonID=b.BusinessEntityID

select * from stg.config_table

truncate table df.fact_SaleProduct
truncate table df.fact_SaleHeader

select * from df.fact_SaleProduct
select * from df.fact_SaleHeader

update stg.config_table set enable=1 where task_id=139 or task_id=140

EXEC load_to_fact 139
select * from stg.v_fact_SaleProduct



select
		[ProductKey],
		[LocationKey],
		proven.BusinessEntityID as [VendorKey],
		purheader.PurchaseOrderID as [PurchaseOrderID],
		purdetail.ModifiedDate as [PurchaseOrderedDateKey],
		purdetail.ModifiedDate as [PurchaseReceivedDateKey],
		purdetail.ModifiedDate as [PurchaseRejectedDateKey],
		purdetail.ModifiedDate as [PurchaseStockedDateKey],
		purdetail.OrderQty as [PurchaseOrderedQty],
		purdetail.ReceivedQty as [PurchaseReceivedQty],
		purdetail.RejectedQty as [PurchaseRejectedQty],
		purdetail.StockedQty as [PurchaseStockedQty]
	from stg.Production_ProductInventory proinv
	inner join stg.Purchasing_ProductVendor proven on proinv.ProductID = proven.ProductID
	inner join stg.Purchasing_PurchaseOrderHeader purheader on proven.BusinessEntityID = purheader.VendorID
	inner join stg.Purchasing_PurchaseOrderDetail purdetail on purheader.PurchaseOrderID = purdetail.PurchaseOrderID
	left join df.dim_Product d_pro on proinv.ProductID=d_pro.ProductID
	left join df.dim_Location d_loc on proinv.LocationID=d_loc.LocationID










SELECT 'TRUNCATE TABLE ' + '[' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']'
FROM INFORMATION_SCHEMA.TABLES
ORDER BY TABLE_SCHEMA, TABLE_NAME

EXEC load_to_dim_scd2 135



EXEC load_to_dim_scd2 140



CREATE TABLE #TempImport (
    ProductName VARCHAR(100),
    TransactionID int,
    ProductID varchar(100),
    ReferenceOrderID varchar(100),
    ReferenceOrderLineID varchar(100),
    TransactionDate varchar(100),
    TransactionType varchar(100),
    Quantity varchar(100),
    ActualCost varchar(100),
    ModifiedDate varchar(100)
);

-- Bulk insert the data from the file into the staging table
BULK INSERT #TempImport
FROM 'C:\temp\cycle-sale\csv\TransactionHistory.csv'  -- Replace with the actual path to your file
WITH (
    FIELDTERMINATOR = '\t',  -- Specify the tab character as the field delimiter
    ROWTERMINATOR = '\n'     -- Specify the line feed character as the row delimiter
);

truncate table #TempImport
select * from #TempImport order by TransactionID
select * from stg.config_table where target_table='dim_Product'