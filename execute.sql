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
update stg.config_table set enable=1 where task_name='landing_test_db_db' or source_location LIKE 'C:\temp\cycle-sale\testdb\%'
update stg.config_table set enable=1 where task_id=123 or task_id=124
update stg.config_table set enable=1 where task_name='load_dim'
select * from stg.config_table where enable=1


--==DIM DATE
truncate table df.dim_Date
exec load_to_dim_Date '20000101'


--==TEST
EXEC load_to_dim_scd2 134