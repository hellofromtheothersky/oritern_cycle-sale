use [oritern_ETL]
drop table if exists config_table
CREATE TABLE config_table (
    task_id INT IDENTITY(1,1) PRIMARY KEY,
    task_name VARCHAR(100),
	source_location VARCHAR(100),
	source_database VARCHAR(100),
	source_schema VARCHAR(100),
	source_table VARCHAR(100),
	target_location VARCHAR(100),
	target_database VARCHAR(100),
	target_schema VARCHAR(100),
	target_table VARCHAR(100),
    enable BIT,
    start_time DATETIME,
    end_time DATETIME,
	status VARCHAR(50),
    fail_reason VARCHAR(255)
);
--CREATE TABLE dependency_table (
--    dependency_id INT IDENTITY(1,1) PRIMARY KEY,
--    task_id INT FOREIGN KEY REFERENCES config_table(task_id),
--    parent_task_id INT,
--    CONSTRAINT FK_dependency_table_config_table FOREIGN KEY (task_id) REFERENCES config_table(task_id),
--    UNIQUE (task_id, parent_task_id)
--);


--insert into dependency_table(task_id, parent_task_id) values(4, 1)
--insert into dependency_table(task_id, parent_task_id) values(4, 2)

--insert into config_table(task_name, enable) values ('landing_csv', 1)
--insert into config_table(task_name, enable) values ('landing_json', 1)
--insert into config_table(task_name, enable) values ('landing_excel', 1)
--insert into config_table(task_name, enable) values ('landing_bicycle_retailer_db', 1)
--insert into config_table(task_name, enable) values ('landing_hrdb_db', 1)

create or alter proc create_config_for_landing_db as
begin
	select TOP 0 * into #temp_table from config_table;

	--generated code
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Sales', 'SalesOrderHeaderSalesReason')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Sales', 'SalesPerson')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'Illustration')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'Location')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Sales', 'SalesPersonQuotaHistory')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Sales', 'SalesReason')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Sales', 'SalesTaxRate')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Sales', 'PersonCreditCard')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Sales', 'SalesTerritory')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'Product')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Sales', 'SalesTerritoryHistory')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'ScrapReason')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'ProductCategory')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Purchasing', 'ShipMethod')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'ProductCostHistory')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'ProductDescription')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Sales', 'ShoppingCartItem')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'ProductDocument')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'dbo', 'DatabaseLog')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'ProductInventory')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Sales', 'SpecialOffer')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'dbo', 'ErrorLog')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'ProductListPriceHistory')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Sales', 'SpecialOfferProduct')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'ProductModel')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'ProductModelIllustration')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'dbo', 'AWBuildVersion')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'ProductModelProductDescriptionCulture')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'BillOfMaterials')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Sales', 'Store')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'ProductPhoto')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'ProductProductPhoto')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'ProductReview')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'dbo', 'sysdiagrams')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'ProductSubcategory')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Purchasing', 'ProductVendor')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'UnitMeasure')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Purchasing', 'Vendor')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Sales', 'CountryRegionCurrency')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'WorkOrder')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Purchasing', 'PurchaseOrderDetail')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Sales', 'CreditCard')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'Culture')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'WorkOrderRouting')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Sales', 'Currency')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Purchasing', 'PurchaseOrderHeader')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Sales', 'CurrencyRate')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Sales', 'Customer')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Production', 'Document')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Sales', 'SalesOrderDetail')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('BicycleRetailer', 'Sales', 'SalesOrderHeader')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('HRDB', 'HumanResources', 'EmployeeDepartmentHistory')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('HRDB', 'HumanResources', 'Employee')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('HRDB', 'HumanResources', 'Department')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('HRDB', 'HumanResources', 'Shift')
	INSERT INTO #temp_table(source_database, source_schema, source_table) VALUES ('HRDB', 'HumanResources', 'EmployeePayHistory')

	update #temp_table
	set target_location='/opt/airflow/landing_cycle-sale/', target_table=source_table, target_schema='csv'

	update #temp_table
	set task_name='landing_bicycle_retailer_db' where source_database='BicycleRetailer'

	update #temp_table
	set task_name='landing_hrdb_db' where source_database='HRDB'

	insert into config_table (task_name, 
							source_location, 
							source_database, 
							source_schema,
							source_table,
							target_location,
							target_database,
							target_schema,
							target_table)
	select task_name, 
			source_location, 
			source_database, 
			source_schema,
			source_table,
			target_location,
			target_database,
			target_schema,
			target_table 
	from #temp_table
end
go

create or alter proc create_config_for_landing_file as
begin
	select TOP 0 * into #temp_table from config_table;
	
	INSERT INTO #temp_table(task_name, source_location) VALUES 
	('landing_csv', '/opt/airflow/source/Csv/TransactionHistory.csv'), 
	('landing_excel', '/opt/airflow/source/Excel/CountryOfBusinessEntity.xlsx'),
	('landing_json', '/opt/airflow/source/Json/Person-GeneralContact.json'),
	('landing_json', '/opt/airflow/source/Json/Person-IndividualCustomer.json'),
	('landing_json', '/opt/airflow/source/Json/Person-Non-salesEmployee.json'),
	('landing_json', '/opt/airflow/source/Json/Person-SalesPerson.json'),
	('landing_json', '/opt/airflow/source/Json/Person-StoreContact.json'),
	('landing_json', '/opt/airflow/source/Json/Person-VendorContact.json')


	update #temp_table
	set target_location='/opt/airflow/landing_cycle-sale/',
		target_table=RIGHT(source_location, CHARINDEX('/', REVERSE(source_location)) - 1)

	update #temp_table
	set target_schema=RIGHT(target_table, CHARINDEX('.', REVERSE(target_table)) - 1),
		target_table=LEFT(target_table, CHARINDEX('.', target_table) - 1)

	insert into config_table (task_name, 
							source_location, 
							source_database, 
							source_schema,
							source_table,
							target_location,
							target_database,
							target_schema,
							target_table)
	select task_name, 
			source_location, 
			source_database, 
			source_schema,
			source_table,
			target_location,
			target_database,
			target_schema,
			target_table 
	from #temp_table
end
go


CREATE OR ALTER PROCEDURE load_enable_task_config
(
    @task_name VARCHAR(100)
)
AS
BEGIN
	select source_location, source_database, source_schema, source_table, target_location, target_database, target_schema, target_table
	from config_table
	where task_name=@task_name and enable=1
END
go




--run
EXEC create_config_for_landing_db
EXEC create_config_for_landing_file
EXEC load_enable_task_config 'landing_bicycle_retailer_db'
EXEC load_enable_task_config 'landing_hrdb_db'

select * from config_table

--test
update config_table set enable=1 where task_id=51 or task_id=52 or task_id>=57
truncate table config_table




