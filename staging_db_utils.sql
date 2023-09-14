use staging_db

create or alter function get_col_in_str
(
	@table_name varchar(100)
)
RETURNS varchar(500)
as
begin
	declare @col_list varchar(500)
	select @col_list= STRING_AGG(QUOTENAME(COLUMN_NAME), ', ')
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
	select task_id, source_location, source_database, source_schema, source_table, target_location, target_database, target_schema, target_table
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
            @is_incre BIT, @time_col_name VARCHAR(100), @last_load_run datetimeoffset,
			@sql NVARCHAR(255)

    SELECT @source_schema = source_schema, @source_table = source_table, 
           @is_incre = is_incre, @time_col_name = time_col_name, @last_load_run = last_load_run 
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
                          ' WHERE ', QUOTENAME(@time_col_name), ' > ', QUOTENAME(@datetimeValue, ''''))
    END

	select @sql
END
GO


CREATE OR ALTER PROC load_to_stage_table
(
	@task_id INT
)
AS
BEGIN
	DECLARE @file_path nvarchar(100), @stage_table_name varchar(100), @is_incre bit, @key_col_name varchar(100), @sql NVARCHAR(MAX)

	SELECT @file_path=source_location, @stage_table_name=target_table, @is_incre=is_incre, @key_col_name=key_col_name
	FROM config_table
	where task_id=@task_id

	BEGIN TRAN
	IF @is_incre = 0 or @is_incre is null
	BEGIN
		--replace * by column list
		set @sql=CONCAT('
		select TOP 0 * into temp_landing from ', @stage_table_name, '

		alter table temp_landing drop column checksum
		alter table temp_landing drop column is_deleted
		alter table temp_landing drop column is_current

		bulk INSERT temp_landing
		FROM ''', @file_path, '''
		WITH 
		(FIRSTROW = 2, 
		FIELDTERMINATOR = '','',
		ROWTERMINATOR = ''0x0A''
		)
		')
		EXEC(@sql)

		DECLARE @col_list varchar(500)
		set @col_list=dbo.get_col_in_str('temp_landing')
		alter table temp_landing add checksum binary(16)

		set @sql=CONCAT('
		update temp_landing 
		set checksum=HASHBYTES(''MD5'', concat_ws(''~'', ', @col_list,')) 

		select l.checksum as landing_checksum, 
			   l.', @key_col_name,' as landing_key,
			   s.checksum as stage_checksum,
			   s.', @key_col_name,' as stage_key
		into temp_compare_hash
		from temp_landing l FULL OUTER JOIN ', @stage_table_name, ' s on l.checksum=s.checksum

		insert into ', @stage_table_name, '
		select *, 0, 1 from temp_landing where checksum in 
		(select landing_checksum from temp_compare_hash 
		where landing_checksum is not null and stage_checksum is null)

		update ', @stage_table_name, '
		set is_deleted=1, is_current=0 
		where checksum in (select stage_checksum from temp_compare_hash 
							where stage_checksum is not null and landing_checksum is null) 
		and ', @key_col_name,' not in (select landing_key from temp_compare_hash 
									where landing_checksum is not null and stage_checksum is null)

		update ', @stage_table_name, '
		set is_deleted=0, is_current=0 
		where checksum in (select stage_checksum from temp_compare_hash 
							where stage_checksum is not null and landing_checksum is null) 
		and ', @key_col_name,' in (select landing_key from temp_compare_hash 
									where landing_checksum is not null and stage_checksum is null)


		drop table temp_landing
		drop table temp_compare_hash
		')
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

		drop table temp_table
		')
		EXEC(@sql)
	END
	COMMIT TRAN
END
GO




