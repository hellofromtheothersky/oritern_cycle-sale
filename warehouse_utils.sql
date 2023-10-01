use warehouse_db

go
CREATE OR ALTER FUNCTION get_col_seq
(
	@schema_name varchar(100),
	@table_name varchar(100),
	@prefix varchar(100) = NULL
)
RETURNS varchar(MAX)
as
begin
	declare @col_list varchar(MAX)

	select @col_list= STRING_AGG(COALESCE(@prefix+'.', '')+QUOTENAME(COLUMN_NAME), ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
	from INFORMATION_SCHEMA.COLUMNS
	where TABLE_NAME=@table_name and TABLE_SCHEMA=@schema_name

	return @col_list
end
Go


CREATE OR ALTER FUNCTION get_first_col
(	
	@schema_name varchar(100),
	@table_name varchar(100)
)
RETURNS varchar(100)
as
begin
	declare @first_col varchar(100)
	select @first_col= COLUMN_NAME
	from INFORMATION_SCHEMA.COLUMNS
	where TABLE_NAME=@table_name and TABLE_SCHEMA=@schema_name and ORDINAL_POSITION=1

	return @first_col
end
Go


CREATE OR ALTER FUNCTION get_col_seq_and_xml_convert
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


CREATE OR ALTER FUNCTION YYYYMMDD_int_format
(	
	@datetime datetime
)
RETURNS int
as
begin
	return CONVERT(INT, CONVERT(VARCHAR(8), @datetime, 112))
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
	print @sql

	BEGIN TRY
		EXEC(@sql)
	END TRY
	BEGIN CATCH
		set @sql=CONCAT('
		bulk INSERT ', @external_table_name,'
		FROM ''', @filepath, '''
		WITH 
		(FORMAT = ''CSV'',
		FIRSTROW = 2, 
		FIELDTERMINATOR = ''\t'',
		ROWTERMINATOR = ''0x0A''
		)')
		print @sql
		EXEC(@sql)
	END CATCH
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

	SET @sql=CONCAT('
	drop table if exists ', @external_table,'
	select TOP 0 * into ', @external_table,' from ', @stage_table, '
	alter table ', @external_table,' drop column checksum
	alter table ', @external_table,' drop column is_deleted
	alter table ', @external_table,' drop column is_current')
	print @sql
	EXEC(@sql)	
	
	if @file_type = 'json' 
		EXEC load_json_to_external_table @file_path, @stage_table_name, @external_table
	else 
		EXEC load_table_to_external_table @file_path, @external_table
	
	print 'loaded to external tb'
	-- load to stage
	IF @is_incre = 0 or @is_incre is null
	BEGIN
		DECLARE @col_list varchar(500) = dbo.get_col_seq_and_xml_convert(@external_schema_name, @external_table_name)

		set @sql=CONCAT('
		alter table ', @external_table,' add checksum binary(16)')
		EXEC(@sql)
		--temp_compare_hash conflict
		set @sql=CONCAT('
		update ', @external_table,' set checksum=HASHBYTES(''MD5'', concat_ws(''~'', ', @col_list,')) 
		print ''hash value created''
		
		DROP TABLE IF EXISTS temp_compare_hash
		select l.checksum as landing_checksum, 
			   l.', @key_col_name,' as nal_key,
			   s.checksum as stage_checksum
		into temp_compare_hash
		from ', @external_table,' l INNER JOIN ', @stage_table, ' s on l.', @key_col_name,' = s.', @key_col_name,'
		where s.is_current=1
		print ''temp_compare_hash created''

		--source dont have key that stage have -> source delete
			update ', @stage_table, '
			set is_deleted=1, is_current=0 
			where ', @key_col_name,' not in (select nal_key from temp_compare_hash)

		--source have key that stage have -> source update
			update ', @stage_table, '
			set is_deleted=0, is_current=0
			where ', @key_col_name,' in (select nal_key from temp_compare_hash where landing_checksum<>stage_checksum)
			and is_current=1

			insert into ', @stage_table, '
			select *, 0, 1 from ', @external_table,' where ', @key_col_name,' in
			(select nal_key from temp_compare_hash where landing_checksum<>stage_checksum)

		--source have key that stage dont have -> source insert
			insert into ', @stage_table, '
			select *, 0, 1 from ', @external_table,' where ', @key_col_name,' not in
			(select nal_key from temp_compare_hash)

		drop table ', @external_table,'
		drop table temp_compare_hash
		')
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
	print @sql
END
GO


CREATE OR ALTER PROC load_to_dim_scd2
(
	@task_id int
)
as
begin
	DECLARE @src_schma varchar(100),
			@src_table varchar(100),
			@dim_schma varchar(100),
			@dim_table varchar(100),
			@key VARCHAR(100),
			@src_time varchar(100)
			

	SELECT  @src_schma = source_schema,
			@src_table = source_table,
			@dim_schma = target_schema,
			@dim_table = target_table,
			@key = QUOTENAME(key_col_name),
			@src_time = QUOTENAME(time_col_name)
	from stg.config_table
	where task_id=@task_id

	DECLARE @src VARCHAR(100) = CONCAT(@src_schma, '.', @src_table)
	DECLARE @dim VARCHAR(100) = CONCAT(@dim_schma, '.', @dim_table)  
	
	DECLARE @src_col_without_time_col VARCHAR(max) = REPLACE(dbo.get_col_seq(@src_schma, @src_table, NULL), ', '+@src_time, '')
	DECLARE @src_col_prefix  VARCHAR(max) = dbo.get_col_seq(@src_schma, @src_table, 'source')

	DECLARE @sql nvarchar(max) =
	CONCAT('
	insert into ', @dim,'
	(	
		--Table and columns in which to insert the data
		', @src_col_without_time_col,',
		[start_date],
		[end_date],
		[is_current]
	)
	select    
		', @src_col_without_time_col,',
		[start_date],
		[end_date],
		[is_current]
	from
	(
		MERGE into ', @dim,' AS target
		USING 
		(
		SELECT *
		from ', @src,'
		) AS source 
		ON target.', @key,' = source.', @key,' and target.[is_current]=1

		--delete old record from updating
		WHEN MATCHED 
			and target.[start_date] < source.[ModifiedDate]
		THEN 
			UPDATE SET [end_date]=source.[ModifiedDate], is_current=0
		--delete record
		WHEN NOT MATCHED BY SOURCE and target.[is_current]=1
		THEN
			UPDATE SET is_current=0
		--insert record
		WHEN NOT MATCHED BY TARGET
		THEN 
			INSERT 
			(
			', @src_col_without_time_col,',
			[start_date],
			[end_date],
			[is_current]
			)
			VALUES 
			(
			', @src_col_prefix,',
			''12/31/9999'',
			1
			)
		OUTPUT $action as ''action'', 
		', @src_col_prefix,',
		''12/31/9999'',
		1
	) -- the end of the merge statement
	--The changes output below are the records that have changed and will need
	--to be inserted into the slowly changing dimension.
	as changes
	(
	[action],
		', @src_col_without_time_col,',
	[start_date],
	[end_date],
	[is_current]
	)
	where action=''UPDATE'' and ', @key,' is not null;
	')
	print @sql
	EXEC(@sql)

	set @sql=CONCAT('
	insert into ', @dim,' (', @key,', [start_date], [end_date], is_current)
	select ', @key,', ''1753-01-01 00:00:00'', min([start_date]), 0 from ', @dim,'
	group by ', @key,'
	having min([start_date])<>''1753-01-01 00:00:00''
	')

	print @sql
	EXEC(@sql)
end
go


CREATE OR ALTER PROC load_to_fact
(
	@task_id int
)
as
begin
	 DECLARE @src_schma varchar(100),
			@src_table varchar(100),
			@fct_schma varchar(100),
			@fct_table varchar(100),
			@src_time varchar(100),
			@last_load_run int
			

	SELECT  @src_schma = source_schema,
			@src_table = source_table,
			@fct_schma = target_schema,
			@fct_table = target_table,
			@src_time = QUOTENAME(time_col_name),
			@last_load_run = dbo.YYYYMMDD_int_format(last_load_run)
	from stg.config_table
	where task_id=@task_id

	DECLARE @src VARCHAR(100) = CONCAT(@src_schma, '.', @src_table)
	DECLARE @fct VARCHAR(100) = CONCAT(@fct_schma, '.', @fct_table) 

	DECLARE @sql NVARCHAR(200)
	set @sql=CONCAT('INSERT INTO ', @fct,' EXEC ', @src)
	if @last_load_run is not null
		set @sql=@sql+' '+QUOTENAME(@last_load_run, '''')
	print @sql
	EXEC(@sql)
end
go


create proc load_to_dim_Date 
(
	@StartDate  date
)
AS
begin
	DECLARE @CutoffDate date = DATEADD(DAY, -1, DATEADD(YEAR, 30, @StartDate));

	;WITH seq(n) AS 
	(
	  SELECT 0 UNION ALL SELECT n + 1 FROM seq
	  WHERE n < DATEDIFF(DAY, @StartDate, @CutoffDate)
	),
	d(d) AS 
	(
	  SELECT DATEADD(DAY, n, @StartDate) FROM seq
	),
	src AS
	(
	  SELECT
		TheDate         = CONVERT(date, d),
		TheDay          = DATEPART(DAY,       d),
		TheDayName      = DATENAME(WEEKDAY,   d),
		TheWeek         = DATEPART(WEEK,      d),
		TheISOWeek      = DATEPART(ISO_WEEK,  d),
		TheDayOfWeek    = DATEPART(WEEKDAY,   d),
		TheMonth        = DATEPART(MONTH,     d),
		TheMonthName    = DATENAME(MONTH,     d),
		TheQuarter      = DATEPART(Quarter,   d),
		TheYear         = DATEPART(YEAR,      d),
		TheFirstOfMonth = DATEFROMPARTS(YEAR(d), MONTH(d), 1),
		TheLastOfYear   = DATEFROMPARTS(YEAR(d), 12, 31),
		TheDayOfYear    = DATEPART(DAYOFYEAR, d)
	  FROM d
	),
	dim AS
	(
	  SELECT
		YYYYMMDD              = TheYear*10000+TheMonth*100+TheDay,
		TheDate, 
		TheDay,
		TheDaySuffix        = CONVERT(char(2), CASE WHEN TheDay / 10 = 1 THEN 'th' ELSE 
								CASE RIGHT(TheDay, 1) WHEN '1' THEN 'st' WHEN '2' THEN 'nd' 
								WHEN '3' THEN 'rd' ELSE 'th' END END),
		TheDayName,
		TheDayOfWeek,
		TheDayOfWeekInMonth = CONVERT(tinyint, ROW_NUMBER() OVER 
								(PARTITION BY TheFirstOfMonth, TheDayOfWeek ORDER BY TheDate)),
		TheDayOfYear,
		IsWeekend           = CASE WHEN TheDayOfWeek IN (CASE @@DATEFIRST WHEN 1 THEN 6 WHEN 7 THEN 1 END,7) 
								THEN 1 ELSE 0 END,
		TheWeek,
		TheISOweek,
		TheFirstOfWeek      = DATEADD(DAY, 1 - TheDayOfWeek, TheDate),
		TheLastOfWeek       = DATEADD(DAY, 6, DATEADD(DAY, 1 - TheDayOfWeek, TheDate)),
		TheWeekOfMonth      = CONVERT(tinyint, DENSE_RANK() OVER 
								(PARTITION BY TheYear, TheMonth ORDER BY TheWeek)),
		TheMonth,
		TheMonthName,
		TheFirstOfMonth,
		TheLastOfMonth      = MAX(TheDate) OVER (PARTITION BY TheYear, TheMonth),
		TheFirstOfNextMonth = DATEADD(MONTH, 1, TheFirstOfMonth),
		TheLastOfNextMonth  = DATEADD(DAY, -1, DATEADD(MONTH, 2, TheFirstOfMonth)),
		TheQuarter,
		TheFirstOfQuarter   = MIN(TheDate) OVER (PARTITION BY TheYear, TheQuarter),
		TheLastOfQuarter    = MAX(TheDate) OVER (PARTITION BY TheYear, TheQuarter),
		TheYear,
		TheISOYear          = TheYear - CASE WHEN TheMonth = 1 AND TheISOWeek > 51 THEN 1 
								WHEN TheMonth = 12 AND TheISOWeek = 1  THEN -1 ELSE 0 END,      
		TheFirstOfYear      = DATEFROMPARTS(TheYear, 1,  1),
		TheLastOfYear,
		IsLeapYear          = CONVERT(bit, CASE WHEN (TheYear % 400 = 0) 
								OR (TheYear % 4 = 0 AND TheYear % 100 <> 0) 
								THEN 1 ELSE 0 END)
	  FROM src
	)
	insert into df.dim_Date
	SELECT * FROM dim
	  ORDER BY TheDate
	  OPTION (MAXRECURSION 0);
end
go