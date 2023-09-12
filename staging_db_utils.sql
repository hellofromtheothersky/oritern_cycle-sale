use staging_db

create or alter function get_col_in_str
(
	@table_name varchar(100)
)
RETURNS varchar(500)
as
begin
	declare @col_list varchar(500)
	select @col_list=STRING_AGG(COLUMN_NAME, ', ')
	from INFORMATION_SCHEMA.COLUMNS
	where TABLE_NAME=@table_name
	return @col_list
end


CREATE OR ALTER PROC load_enable_task_config
(
    @task_name VARCHAR(100)
)
AS
BEGIN
	select task_id, source_location, source_database, source_schema, source_table, target_location, target_database, target_schema, target_table, is_incre
	from config_table
	where task_name=@task_name and enable=1
END
GO


CREATE OR ALTER PROC set_task_config
(
    @task_id VARCHAR(100),
	@start_time datetimeoffset,
	@end_time datetimeoffset,
	@duration decimal(18, 3),
	@status varchar(50),
	@last_load_run datetimeoffset
)
AS
BEGIN
	update config_table 
	set start_time=@start_time, end_time=@end_time, duration=@duration, status=@status, last_load_run=@last_load_run
	where task_id=@task_id
END
GO


CREATE OR ALTER PROC get_qr_for_select_data_of_task
(
    @task_id INT
)
AS
BEGIN
    DECLARE @table_name VARCHAR(100), @source_schema VARCHAR(100), @source_table VARCHAR(100),
            @is_incre BIT, @incre_date_name VARCHAR(100), @last_load_run datetimeoffset,
			@sql NVARCHAR(255)

    SELECT @source_schema = source_schema, @source_table = source_table, 
           @is_incre = is_incre, @incre_date_name = incre_date_name, @last_load_run = last_load_run 
    FROM config_table
    WHERE task_id = @task_id

    SET @table_name = CONCAT(@source_schema, '.', @source_table)

    IF @is_incre = 0 OR @is_incre IS NULL or @last_load_run is NULL
        SET @sql = 'SELECT * FROM ' + @table_name
    ELSE
    BEGIN
		DECLARE @datetimeValue datetime
		SET @datetimeValue = SWITCHOFFSET(CONVERT(datetimeoffset, @last_load_run), '+00:00')
        SET @sql = CONCAT('SELECT * FROM ', @table_name, 
                          ' WHERE ', QUOTENAME(@incre_date_name), ' > ', QUOTENAME(@datetimeValue, ''''))
    END

	select @sql
END
GO


CREATE OR ALTER PROC load_to_stage_table
(
	@file_path nvarchar(100),
	@stage_table_name varchar(100),
	@is_incre bit
)
AS
BEGIN
	DECLARE @sql NVARCHAR(MAX)
	IF @is_incre = 0
	BEGIN
	--replace * by column list
		set @sql=CONCAT('
		select TOP 0 * into temp_table from ', @stage_table_name, '

		alter table temp_table drop column checksum
		alter table temp_table drop column is_deleted
		alter table temp_table drop column is_current

		bulk INSERT temp_table
		FROM ''', @file_path, '''
		WITH 
		(FIRSTROW = 2, 
		FIELDTERMINATOR = '','',
		ROWTERMINATOR = ''0x0A''
		)')
		EXEC(@sql)

		DECLARE @col_list varchar(500)
		set @col_list=dbo.get_col_in_str('temp_table')
		alter table temp_table add checksum binary(16)

		set @sql=CONCAT('
		update temp_table 
		set checksum=HASHBYTES(''MD5'', concat_ws(''~'', ', @col_list,')) 

		update ', @stage_table_name, '
		set is_deleted=1 where checksum not in (select checksum from temp_table)

		insert into ', @stage_table_name, '
		select *, NULL, NULL from temp_table where checksum not in (select checksum from ' ,@stage_table_name, ')

		drop table temp_table')
		EXEC(@sql)
	END
	ELSE
	BEGIN
		set @sql=CONCAT('
		select TOP 0 * into temp_table from ', @stage_table_name, '

		alter table temp_table drop column checksum
		alter table temp_table drop column is_deleted
		alter table temp_table drop column is_current

		bulk INSERT temp_table
		FROM ''', @file_path, '''
		WITH 
		(FIRSTROW = 2, 
		FIELDTERMINATOR = '','',
		ROWTERMINATOR = ''0x0A''
		)

		insert into ', @stage_table_name, '
		select *, NULL, NULL, NULL from temp_table

		drop table temp_table')
		EXEC(@sql)
	END
END
GO

load_to_stage_table 'C:\temp\cycle-sale\testdb\dbo_Production_Location.csv', dbo_Production_Location, 0
select * from dbo_Production_Location
select * from dbo_Sales_CreditCard
truncate table dbo_Production_Location
truncate table dbo_Sales_CreditCard














