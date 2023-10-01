--==INIT
use warehouse_db
--create schema stg --staging
--create schema df --dimfake


--==CONFIG TABLE
truncate table stg.config_table
--set landing
EXEC create_config_for_landing_db
EXEC create_config_for_landing_file
--set increment for landing, staging will auto set incre based on config on landing
update stg.config_table set is_incre=1 
where (task_name='landing_test_db_db' and source_table='Sales_CreditCard')
or source_table='TransactionHistory'
select * from stg.config_table where is_incre=1


--set staging
EXEC create_config_for_staging_all


--set load dim
EXEC create_config_for_load_dim
--set increment for load dim
update stg.config_table set is_incre=1 
where task_name='load_dim'
select * from stg.config_table where is_incre=1
--DIM DATE
truncate table df.dim_Date
exec load_to_dim_Date '20000101'


--set load fact
EXEC create_config_for_load_fact


--set enable task
update stg.config_table set enable=1 where source_table NOT IN ('Production_ProductDocument',
												'Production_ProductPhoto',
												'dbo_sysdiagrams',
												'Production_Document',
												'HumanResources_Employee')
select * from stg.config_table where enable=1