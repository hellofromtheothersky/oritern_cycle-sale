CREATE OR ALTER PROC create_config_for_landing_db as
begin
	select TOP 0 * into #temp_table from stg.config_table;

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
	INSERT INTO #temp_table(task_name, source_database, source_schema, source_table) VALUES ('landing_test_db_db', 'testdb', 'dbo', 'Sales_CreditCard')
	INSERT INTO #temp_table(task_name, source_database, source_schema, source_table) VALUES ('landing_test_db_db', 'testdb', 'dbo', 'Production_Location')


	update #temp_table
	set target_location='C:\temp\cycle-sale\'+source_database, 
		target_table=source_schema+'_'+source_table, 
		target_schema='csv',
		time_col_name='ModifiedDate'

	update #temp_table
	set task_name='landing_bicycle_retailer_db' where source_database='BicycleRetailer'

	update #temp_table
	set task_name='landing_hrdb_db' where source_database='HRDB'

	insert into stg.config_table (task_name, 
							source_location, 
							source_database, 
							source_schema,
							source_table,
							target_location,
							target_database,
							target_schema,
							target_table,
							key_col_name,
							time_col_name,
							is_incre)
	select task_name, 
			source_location, 
			source_database, 
			source_schema,
			source_table,
			target_location,
			target_database,
			target_schema,
			target_table,
			key_col_name,
			time_col_name,
			is_incre
	from #temp_table
end
GO

CREATE OR ALTER PROC create_config_for_landing_file as
begin
	select TOP 0 * into #temp_table from stg.config_table;
	
	INSERT INTO #temp_table(task_name, source_location, source_table, source_schema) VALUES 
	('landing_csv', '\\NW-ORIENTINTERN\SharedData\CSV\', 'TransactionHistory', 'csv'), 
	('landing_excel', '\\NW-ORIENTINTERN\SharedData\Excel', 'CountryOfBusinessEntity', 'xlsx'),
	('landing_json', '\\NW-ORIENTINTERN\SharedData\Json', 'Person-GeneralContact', 'json'),
	('landing_json', '\\NW-ORIENTINTERN\SharedData\Json', 'Person-IndividualCustomer', 'json'),
	('landing_json', '\\NW-ORIENTINTERN\SharedData\Json', 'Person-Non-salesEmployee', 'json'),
	('landing_json', '\\NW-ORIENTINTERN\SharedData\Json', 'Person-SalesPerson', 'json'),
	('landing_json', '\\NW-ORIENTINTERN\SharedData\Json', 'Person-StoreContact', 'json'),
	('landing_json', '\\NW-ORIENTINTERN\SharedData\Json', 'Person-VendorContact', 'json')


	--update #temp_table
	--set target_table=RIGHT(source_location, CHARINDEX('\', REVERSE(source_location)) - 1)

	--update #temp_table
	--set target_schema=RIGHT(target_table, CHARINDEX('.', REVERSE(target_table)) - 1),
	--	target_table=LEFT(target_table, CHARINDEX('.', target_table) - 1)

	update #temp_table
	set target_location='C:\temp\cycle-sale\'+source_schema,
	target_table=source_table,
	target_schema=source_schema,
	time_col_name='ModifiedDate'

	update #temp_table
	set target_schema='csv' where target_schema='xlsx'

	insert into stg.config_table (task_name, 
							source_location, 
							source_database, 
							source_schema,
							source_table,
							target_location,
							target_database,
							target_schema,
							target_table,
							key_col_name,
							time_col_name,
							is_incre)
	select task_name, 
			source_location, 
			source_database, 
			source_schema,
			source_table,
			target_location,
			target_database,
			target_schema,
			target_table,
			key_col_name,
			time_col_name,
			is_incre
	from #temp_table
end
GO

CREATE OR ALTER PROC create_config_for_staging_all as
begin
	select TOP 0 * into #temp_table from stg.config_table;
	
	INSERT INTO #temp_table(is_incre, target_table, source_location, source_schema, source_table, time_col_name) 
	select is_incre, target_table, target_location, target_schema, target_table, time_col_name from stg.config_table

	update #temp_table
	set task_name='staging',
		target_database='warehouse_db',
		target_schema='stg'
	
	update #temp_table
		set key_col_name=dbo.get_first_col(target_schema, target_table)

	insert into stg.config_table (task_name, 
							source_location, 
							source_database, 
							source_schema,
							source_table,
							target_location,
							target_database,
							target_schema,
							target_table,
							key_col_name,
							time_col_name,
							is_incre)
	select task_name, 
			source_location, 
			source_database, 
			source_schema,
			source_table,
			target_location,
			target_database,
			target_schema,
			target_table,
			key_col_name,
			time_col_name,
			is_incre
	from #temp_table
end
GO

CREATE OR ALTER PROC create_config_for_load_dim as
begin
	select TOP 0 * into #temp_table from stg.config_table;
	
	INSERT INTO #temp_table(target_table) VALUES ('dim_Product')
	INSERT INTO #temp_table(target_table) VALUES ('dim_SalesTerritory')
	INSERT INTO #temp_table(target_table) VALUES ('dim_SpeciaOffer')
	INSERT INTO #temp_table(target_table) VALUES ('dim_ShipMethod')
	INSERT INTO #temp_table(target_table) VALUES ('dim_Customer')
	INSERT INTO #temp_table(target_table) VALUES ('dim_SalesPerson')
	INSERT INTO #temp_table(target_table) VALUES ('dim_Location')
	INSERT INTO #temp_table(target_table) VALUES ('dim_Vendor')

	update #temp_table
	set task_name='load_dim',
		source_database='warehouse_db',
		source_schema='stg',
		source_table='v_'+target_table,
		target_database='warehouse_db',
		target_schema='DF',
		time_col_name='ModifiedDate'

	update #temp_table
	set key_col_name=dbo.get_first_col(source_schema, source_table)

	insert into stg.config_table (task_name, 
							source_location, 
							source_database, 
							source_schema,
							source_table,
							target_location,
							target_database,
							target_schema,
							target_table,
							key_col_name,
							time_col_name,
							is_incre)
	select task_name, 
			source_location, 
			source_database, 
			source_schema,
			source_table,
			target_location,
			target_database,
			target_schema,
			target_table,
			key_col_name,
			time_col_name,
			is_incre
	from #temp_table
end
GO


CREATE OR ALTER PROC create_config_for_load_fact as
begin
	select TOP 0 * into #temp_table from stg.config_table;
	
	INSERT INTO #temp_table(target_table) VALUES ('fact_SaleProduct')
	INSERT INTO #temp_table(target_table) VALUES ('fact_SaleHeader')
	INSERT INTO #temp_table(target_table) VALUES ('fact_Stock')


	update #temp_table
	set task_name='load_fact',
		source_database='warehouse_db',
		source_schema='stg',
		source_table='p_'+target_table,
		target_database='warehouse_db',
		target_schema='DF',
		time_col_name='ModifiedDate'


	insert into stg.config_table (task_name, 
							source_location, 
							source_database, 
							source_schema,
							source_table,
							target_location,
							target_database,
							target_schema,
							target_table,
							key_col_name,
							time_col_name,
							is_incre)
	select task_name, 
			source_location, 
			source_database, 
			source_schema,
			source_table,
			target_location,
			target_database,
			target_schema,
			target_table,
			key_col_name,
			time_col_name,
			is_incre
	from #temp_table
end
GO