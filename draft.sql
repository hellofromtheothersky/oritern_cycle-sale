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


SELECT * FROM[df].[dim_Customer]
SELECT * FROM[df].[dim_Date]
SELECT * FROM[df].[dim_Product]
SELECT * FROM[df].[dim_SalesPerson]
SELECT * FROM[df].[dim_SalesTerritory]
SELECT * FROM[df].[dim_ShipMethod]
SELECT * FROM[df].[dim_SpeciaOffer]
SELECT * FROM[df].[fact_Sale]

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