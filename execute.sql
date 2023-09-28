--==INIT
use warehouse_db
create schema stg --staging
create schema df --dimfake


--==CONFIG TABLE
truncate table stg.config_table
--set landing
EXEC create_config_for_landing_db
EXEC create_config_for_landing_file
--set increment
update stg.config_table set is_incre=1 where task_name='landing_test_db_db' and source_table='Sales_CreditCard'
select * from stg.config_table where is_incre=1
--set staging
EXEC create_config_for_staging_all
--set load dim
EXEC create_config_for_load_dim
--set load fact
EXEC create_config_for_load_fact
--set enable task
update stg.config_table set enable=1 where source_table NOT IN ('Production_ProductDocument',
												'Production_ProductPhoto',
												'dbo_sysdiagrams',
												'Production_Document',
												'HumanResources_Employee',
												'TransactionHistory')
select * from stg.config_table where enable=1


--==DIM DATE
truncate table df.dim_Date
exec load_to_dim_Date '20000101'


--==TEST
EXEC load_to_dim_scd2 134

select * from stg.config_table where task_id=140
update stg.config_table set target_schema='DF' where task_id=140

EXEC load_to_stage_table 84
select * from stg.config_table where task_id=84
select source_table from stg.config_table where status='failed'


select *
from stg.config_table
where source_table IN ('Production_ProductDocument',
												'Production_ProductPhoto',
												'dbo_sysdiagrams',
												'Production_Document',
												'HumanResources_Employee',
												'TransactionHistory')
											exec load_to_stage_table 125