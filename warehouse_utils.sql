use warehouse_db

create or alter function get_col_in_str
(
	@schema_name varchar(100),
	@table_name varchar(100)
)
RETURNS varchar(500)
as
begin
	declare @col_list varchar(500)
	select @col_list= STRING_AGG(QUOTENAME(COLUMN_NAME), ', ')
	from INFORMATION_SCHEMA.COLUMNS
	where TABLE_NAME=@table_name and TABLE_SCHEMA=@schema_name
	return @col_list
end
Go

create or alter function get_col_in_str_and_xml_convert
(	
	@schema_name varchar(100),
	@table_name varchar(100)
)
RETURNS varchar(500)
as
begin
	declare @col_list varchar(500)
	select @col_list=STRING_AGG(case DATA_TYPE when 'xml' 
					then CONCAT('convert(varchar(MAX), ', QUOTENAME(COLUMN_NAME),')')
					else QUOTENAME(COLUMN_NAME) end, ', ')
	from INFORMATION_SCHEMA.COLUMNS
	where TABLE_NAME=@table_name and TABLE_SCHEMA=@schema_name
	return @col_list
end
Go

CREATE OR ALTER PROC load_enable_task_config
(
    @task_name VARCHAR(100)
)
AS
BEGIN
	select task_id, source_location, source_database, source_schema, source_table, target_location, target_database, target_schema, target_table
	from stg.config_table
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
	update stg.config_table 
	set start_time=@start_time, end_time=@end_time, duration=@duration, status=@status, last_load_run=@last_load_run
	where task_id=@task_id
END
GO

--make the query to select data incrementally or full loading from a source table in config table
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
    FROM stg.config_table
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

-- helper proc to load a file to database, and then load it to the staging table
CREATE OR ALTER PROC load_table_to_external_table
(
	@filepath nvarchar(100),
	@external_table_name varchar(100)
)
as
begin
	declare @sql NVARCHAR(MAX)
	set @sql=CONCAT('
		bulk INSERT ', @external_table_name,'
		FROM ''', @filepath, '''
		WITH 
		(FORMAT = ''CSV'',
		FIRSTROW = 2, 
		FIELDTERMINATOR = '','',
		ROWTERMINATOR = ''0x0A''
		)')
	EXEC(@sql)
end
Go

CREATE OR ALTER PROC load_json_to_external_table
(
	@filepath nvarchar(100),
	@json_file_opt nvarchar(100),
	@external_table_name varchar(100)
)
as
begin
	declare @sql NVARCHAR(MAX)

	DECLARE @format_json varchar(MAX)
	if @json_file_opt='Person-GeneralContact' 
		set @format_json='BusinessEntityID INT ''$.BusinessEntityID'',
							PersonType VARCHAR(2) ''$.PersonType'',
							ModifiedDate DATETIME ''$.ModifiedDate'',
							FirstName VARCHAR(50) ''$.PersonInfo[0].FirstName'',
							MiddleName VARCHAR(50) ''$.PersonInfo[0].MiddleName'',
							LastName VARCHAR(50) ''$.PersonInfo[0].LastName'',
							PersonAddressDetail VARCHAR(50) ''$.PersonInfo[0].PersonContact[0].PersonAddress[0].PersonAddressDetail'''

	if @json_file_opt='Person-IndividualCustomer'
		set @format_json='  BusinessEntityID INT ''$.BusinessEntityID'',
							PersonType VARCHAR(2) ''$.PersonType'',
							ModifiedDate DATETIME ''$.ModifiedDate'',
							FirstName VARCHAR(50) ''$.PersonInfo[0].FirstName'',
							MiddleName VARCHAR(50) ''$.PersonInfo[0].MiddleName'',
							LastName VARCHAR(50) ''$.PersonInfo[0].LastName'',
							AddressLine1 VARCHAR(100) ''$.PersonInfo[0].PersonContact[0].AddressLine1'',
							City VARCHAR(50) ''$.PersonInfo[0].PersonContact[0].City'',
							PostalCode VARCHAR(10) ''$.PersonInfo[0].PersonContact[0].PostalCode'',
							AddressName VARCHAR(50) ''$.PersonInfo[0].PersonContact[0].PersonAddress[0].AddressName'',
							StateProvinceCode VARCHAR(10) ''$.PersonInfo[0].PersonContact[0].PersonAddress[0].PersonAddressDetail[0].StateProvinceCode'',
							CountryRegionCode VARCHAR(10) ''$.PersonInfo[0].PersonContact[0].PersonAddress[0].PersonAddressDetail[0].CountryRegionCode'',
							StateProvinceName VARCHAR(50) ''$.PersonInfo[0].PersonContact[0].PersonAddress[0].PersonAddressDetail[0].StateProvinceName'''
	if @json_file_opt='Person-Non-salesEmployee'
		set @format_json='
		    BusinessEntityID INT ''$[0].BusinessEntityID'',
			PersonType VARCHAR(2) ''$[0].PersonType'',
			ModifiedDate DATETIME ''$[0].ModifiedDate'',
			FirstName VARCHAR(50) ''$[0].PersonInfo[0].FirstName'',
			MiddleName VARCHAR(50) ''$[0].PersonInfo[0].MiddleName'',
			LastName VARCHAR(50) ''$[0].PersonInfo[0].LastName'',
			AddressLine1 VARCHAR(100) ''$[0].PersonInfo[0].PersonContact[0].AddressLine1'',
			City VARCHAR(50) ''$[0].PersonInfo[0].PersonContact[0].City'',
			PostalCode VARCHAR(10) ''$[0].PersonInfo[0].PersonContact[0].PostalCode'',
			AddressName VARCHAR(50) ''$[0].PersonInfo[0].PersonContact[0].PersonAddress[0].AddressName'',
			StateProvinceCode VARCHAR(10) ''$[0].PersonInfo[0].PersonContact[0].PersonAddress[0].PersonAddressDetail[0].StateProvinceCode'',
			CountryRegionCode VARCHAR(10) ''$[0].PersonInfo[0].PersonContact[0].PersonAddress[0].PersonAddressDetail[0].CountryRegionCode'',
			StateProvinceName VARCHAR(50) ''$[0].PersonInfo[0].PersonContact[0].PersonAddress[0].PersonAddressDetail[0].StateProvinceName'''
	if @json_file_opt='Person-SalesPerson'
		set @format_json='  BusinessEntityID INT ''$.BusinessEntityID'',
							PersonType VARCHAR(2) ''$.PersonType'',
							ModifiedDate DATETIME ''$.ModifiedDate'',
							FirstName VARCHAR(50) ''$.PersonInfo[0].FirstName'',
							MiddleName VARCHAR(50) ''$.PersonInfo[0].MiddleName'',
							LastName VARCHAR(50) ''$.PersonInfo[0].LastName'',
							AddressLine1 VARCHAR(100) ''$.PersonInfo[0].PersonContact[0].AddressLine1'',
							City VARCHAR(50) ''$.PersonInfo[0].PersonContact[0].City'',
							PostalCode VARCHAR(10) ''$.PersonInfo[0].PersonContact[0].PostalCode'',
							AddressName VARCHAR(50) ''$.PersonInfo[0].PersonContact[0].PersonAddress[0].AddressName'',
							StateProvinceCode VARCHAR(10) ''$.PersonInfo[0].PersonContact[0].PersonAddress[0].PersonAddressDetail[0].StateProvinceCode'',
							CountryRegionCode VARCHAR(10) ''$.PersonInfo[0].PersonContact[0].PersonAddress[0].PersonAddressDetail[0].CountryRegionCode'',
							StateProvinceName VARCHAR(50) ''$.PersonInfo[0].PersonContact[0].PersonAddress[0].PersonAddressDetail[0].StateProvinceName'''
	if @json_file_opt='Person-StoreContact'
		set @format_json='BusinessEntityID INT ''$.BusinessEntityID'',
							PersonType VARCHAR(2) ''$.PersonType'',
							ModifiedDate DATETIME ''$.ModifiedDate'',
							FirstName VARCHAR(50) ''$.PersonInfo[0].FirstName'',
							MiddleName VARCHAR(50) ''$.PersonInfo[0].MiddleName'',
							LastName VARCHAR(50) ''$.PersonInfo[0].LastName'''
	if @json_file_opt='Person-VendorContact'
		set @format_json='BusinessEntityID INT ''$.BusinessEntityID'',
							PersonType VARCHAR(2) ''$.PersonType'',
							ModifiedDate DATETIME ''$.ModifiedDate'',
							FirstName VARCHAR(50) ''$.PersonInfo[0].FirstName'',
							MiddleName VARCHAR(50) ''$.PersonInfo[0].MiddleName'',
							LastName VARCHAR(50) ''$.PersonInfo[0].LastName'''
	Set @sql=CONCAT('
	Declare @JSON varchar(max)
	SELECT @JSON=BulkColumn
	FROM OPENROWSET (BULK ', QUOTENAME(@filepath, ''''), ', SINGLE_NCLOB) import
	insert into ', @external_table_name,'
	SELECT * FROM OPENJSON (@JSON)
	WITH (
		', @format_json,'
	);')
	EXEC (@sql)
END
Go

CREATE OR ALTER PROC load_to_stage_table
(
	@task_id INT
)
AS
BEGIN
	SET XACT_ABORT ON
	-- init variables
	DECLARE @file_path nvarchar(100), 
			@file_type varchar(10),
			@stage_table_name varchar(100), 
			@stage_schema_name varchar(100), 
			@stage_table varchar(100),
			@is_incre bit, 
			@key_col_name varchar(100), 
			@external_table varchar(100),
			@external_table_name varchar(100),
			@external_schema_name varchar(100),
			@sql NVARCHAR(MAX)

	SELECT  @file_path=CONCAT(source_location, '\', source_table, '.', source_schema), 
			@file_type=source_schema,

			@stage_table=target_schema+'.'+QUOTENAME(target_table), 
			@stage_table_name=target_table,
			@stage_schema_name=target_schema,

			@is_incre=is_incre, 
			@key_col_name=key_col_name,

			@external_table=target_schema+'.[temp_'+target_table+']',
			@external_table_name='temp_'+target_table,
			@external_schema_name=target_schema
	FROM stg.config_table
	where task_id=@task_id

	-- init external table
	BEGIN TRAN

	SET @sql=CONCAT('
	drop table if exists ', @external_table,'
	select TOP 0 * into ', @external_table,' from ', @stage_table, '
	alter table ', @external_table,' drop column checksum
	alter table ', @external_table,' drop column is_deleted
	alter table ', @external_table,' drop column is_current')
	EXEC(@sql)	
	
	if @file_type = 'json' 
		EXEC load_json_to_external_table @file_path, @stage_table_name, @external_table
	else 
		EXEC load_table_to_external_table @file_path, @external_table
	
	print 'loaded to external tb'
	-- load to stage
	IF @is_incre = 0 or @is_incre is null
	BEGIN
		DECLARE @col_list varchar(500) = dbo.get_col_in_str_and_xml_convert(@external_schema_name, @external_table_name)

		set @sql=CONCAT('
		alter table ', @external_table,' add checksum binary(16)')
		EXEC(@sql)

		set @sql=CONCAT('
		update ', @external_table,' set checksum=HASHBYTES(''MD5'', concat_ws(''~'', ', @col_list,')) 
		print ''hash value created''

		select l.checksum as landing_checksum, 
			   l.', @key_col_name,' as landing_key,
			   s.checksum as stage_checksum,
			   s.', @key_col_name,' as stage_key
		into temp_compare_hash
		from ', @external_table,' l FULL OUTER JOIN ', @stage_table, ' s on l.checksum=s.checksum

		insert into ', @stage_table, '
		select *, 0, 1 from ', @external_table,' where checksum in 
		(select landing_checksum from temp_compare_hash 
		where landing_checksum is not null and stage_checksum is null)

		update ', @stage_table, '
		set is_deleted=1, is_current=0 
		where checksum in (select stage_checksum from temp_compare_hash 
							where stage_checksum is not null and landing_checksum is null) 
		and ', @key_col_name,' not in (select landing_key from temp_compare_hash 
									where landing_checksum is not null and stage_checksum is null)

		update ', @stage_table, '
		set is_deleted=0, is_current=0 
		where checksum in (select stage_checksum from temp_compare_hash 
							where stage_checksum is not null and landing_checksum is null) 
		and ', @key_col_name,' in (select landing_key from temp_compare_hash 
									where landing_checksum is not null and stage_checksum is null)


		drop table ', @external_table,'
		drop table temp_compare_hash
		')
		print @sql
		EXEC(@sql)
	END
	ELSE
	BEGIN
		set @sql=CONCAT('
		insert into ', @stage_table, '
		select *, NULL, NULL, NULL from ', @external_table,'

		drop table ', @external_table,'
		')
		EXEC(@sql)
	END
	COMMIT TRAN
END
GO

EXEC load_to_stage_table 67