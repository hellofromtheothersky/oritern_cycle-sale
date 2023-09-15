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
    start_time datetimeoffset,
    end_time datetimeoffset,
	duration DECIMAL(18, 3),
	status VARCHAR(50),
    fail_reason VARCHAR(255),
	is_incre bit,
	key_col_name varchar(100),
	time_col_name varchar(100),
	last_load_run datetimeoffset,
);

CREATE TABLE [Sales_SalesOrderHeaderSalesReason] (
[SalesOrderID] int NOT NULL,[SalesReasonID] int NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_SalesPerson] (
[BusinessEntityID] int NOT NULL,[TerritoryID] int NULL,[SalesQuota] money NULL,[Bonus] money NOT NULL,[CommissionPct] smallmoney NOT NULL,[SalesYTD] money NOT NULL,[SalesLastYear] money NOT NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_Illustration] (
[IllustrationID] int NOT NULL,[Diagram] xml NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_Location] (
[LocationID] smallint NOT NULL,[Name] nvarchar(50) NOT NULL,[CostRate] smallmoney NOT NULL,[Availability] decimal(8,2) NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_SalesPersonQuotaHistory] (
[BusinessEntityID] int NOT NULL,[QuotaDate] datetime NOT NULL,[SalesQuota] money NOT NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_SalesReason] (
[SalesReasonID] int NOT NULL,[Name] nvarchar(50) NOT NULL,[ReasonType] nvarchar(50) NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_SalesTaxRate] (
[SalesTaxRateID] int NOT NULL,[StateProvinceID] int NOT NULL,[TaxType] tinyint NOT NULL,[TaxRate] smallmoney NOT NULL,[Name] nvarchar(50) NOT NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_PersonCreditCard] (
[BusinessEntityID] int NOT NULL,[CreditCardID] int NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [HumanResources_vEmployee] (
[BusinessEntityID] int NOT NULL,[Title] nvarchar(8) NULL,[FirstName] nvarchar(50) NOT NULL,[MiddleName] nvarchar(50) NULL,[LastName] nvarchar(50) NOT NULL,[Suffix] nvarchar(10) NULL,[JobTitle] nvarchar(50) NOT NULL,[PhoneNumber] nvarchar(25) NULL,[PhoneNumberType] nvarchar(50) NULL,[EmailAddress] nvarchar(50) NULL,[EmailPromotion] int NOT NULL,[AddressLine1] nvarchar(60) NOT NULL,[AddressLine2] nvarchar(60) NULL,[City] nvarchar(30) NOT NULL,[StateProvinceName] nvarchar(50) NOT NULL,[PostalCode] nvarchar(15) NOT NULL,[CountryRegionName] nvarchar(50) NOT NULL,[AdditionalContactInfo] xml NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_SalesTerritory] (
[TerritoryID] int NOT NULL,[Name] nvarchar(50) NOT NULL,[CountryRegionCode] nvarchar(3) NOT NULL,[Group] nvarchar(50) NOT NULL,[SalesYTD] money NOT NULL,[SalesLastYear] money NOT NULL,[CostYTD] money NOT NULL,[CostLastYear] money NOT NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [HumanResources_vEmployeeDepartment] (
[BusinessEntityID] int NOT NULL,[Title] nvarchar(8) NULL,[FirstName] nvarchar(50) NOT NULL,[MiddleName] nvarchar(50) NULL,[LastName] nvarchar(50) NOT NULL,[Suffix] nvarchar(10) NULL,[JobTitle] nvarchar(50) NOT NULL,[Department] nvarchar(50) NOT NULL,[GroupName] nvarchar(50) NOT NULL,[StartDate] date NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [HumanResources_vEmployeeDepartmentHistory] (
[BusinessEntityID] int NOT NULL,[Title] nvarchar(8) NULL,[FirstName] nvarchar(50) NOT NULL,[MiddleName] nvarchar(50) NULL,[LastName] nvarchar(50) NOT NULL,[Suffix] nvarchar(10) NULL,[Shift] nvarchar(50) NOT NULL,[Department] nvarchar(50) NOT NULL,[GroupName] nvarchar(50) NOT NULL,[StartDate] date NOT NULL,[EndDate] date NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_vIndividualCustomer] (
[BusinessEntityID] int NOT NULL,[Title] nvarchar(8) NULL,[FirstName] nvarchar(50) NOT NULL,[MiddleName] nvarchar(50) NULL,[LastName] nvarchar(50) NOT NULL,[Suffix] nvarchar(10) NULL,[PhoneNumber] nvarchar(25) NULL,[PhoneNumberType] nvarchar(50) NULL,[EmailAddress] nvarchar(50) NULL,[EmailPromotion] int NOT NULL,[AddressType] nvarchar(50) NOT NULL,[AddressLine1] nvarchar(60) NOT NULL,[AddressLine2] nvarchar(60) NULL,[City] nvarchar(30) NOT NULL,[StateProvinceName] nvarchar(50) NOT NULL,[PostalCode] nvarchar(15) NOT NULL,[CountryRegionName] nvarchar(50) NOT NULL,[Demographics] xml NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_Product] (
[ProductID] int NOT NULL,[Name] nvarchar(50) NOT NULL,[ProductNumber] nvarchar(25) NOT NULL,[MakeFlag] bit NOT NULL,[FinishedGoodsFlag] bit NOT NULL,[Color] nvarchar(15) NULL,[SafetyStockLevel] smallint NOT NULL,[ReorderPoint] smallint NOT NULL,[StandardCost] money NOT NULL,[ListPrice] money NOT NULL,[Size] nvarchar(5) NULL,[SizeUnitMeasureCode] nchar(3) NULL,[WeightUnitMeasureCode] nchar(3) NULL,[Weight] decimal(8,2) NULL,[DaysToManufacture] int NOT NULL,[ProductLine] nchar(2) NULL,[Class] nchar(2) NULL,[Style] nchar(2) NULL,[ProductSubcategoryID] int NULL,[ProductModelID] int NULL,[SellStartDate] datetime NOT NULL,[SellEndDate] datetime NULL,[DiscontinuedDate] datetime NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_vPersonDemographics] (
[BusinessEntityID] int NOT NULL,[TotalPurchaseYTD] money NULL,[DateFirstPurchase] datetime NULL,[BirthDate] datetime NULL,[MaritalStatus] nvarchar(1) NULL,[YearlyIncome] nvarchar(30) NULL,[Gender] nvarchar(1) NULL,[TotalChildren] int NULL,[NumberChildrenAtHome] int NULL,[Education] nvarchar(30) NULL,[Occupation] nvarchar(30) NULL,[HomeOwnerFlag] bit NULL,[NumberCarsOwned] int NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [HumanResources_vJobCandidate] (
[JobCandidateID] int NOT NULL,[BusinessEntityID] int NULL,[Name.Prefix] nvarchar(30) NULL,[Name.First] nvarchar(30) NULL,[Name.Middle] nvarchar(30) NULL,[Name.Last] nvarchar(30) NULL,[Name.Suffix] nvarchar(30) NULL,[Skills] nvarchar(max) NULL,[Addr.Type] nvarchar(30) NULL,[Addr.Loc.CountryRegion] nvarchar(100) NULL,[Addr.Loc.State] nvarchar(100) NULL,[Addr.Loc.City] nvarchar(100) NULL,[Addr.PostalCode] nvarchar(20) NULL,[EMail] nvarchar(max) NULL,[WebSite] nvarchar(max) NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [HumanResources_vJobCandidateEmployment] (
[JobCandidateID] int NOT NULL,[Emp.StartDate] datetime NULL,[Emp.EndDate] datetime NULL,[Emp.OrgName] nvarchar(100) NULL,[Emp.JobTitle] nvarchar(100) NULL,[Emp.Responsibility] nvarchar(max) NULL,[Emp.FunctionCategory] nvarchar(max) NULL,[Emp.IndustryCategory] nvarchar(max) NULL,[Emp.Loc.CountryRegion] nvarchar(max) NULL,[Emp.Loc.State] nvarchar(max) NULL,[Emp.Loc.City] nvarchar(max) NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [HumanResources_vJobCandidateEducation] (
[JobCandidateID] int NOT NULL,[Edu.Level] nvarchar(max) NULL,[Edu.StartDate] datetime NULL,[Edu.EndDate] datetime NULL,[Edu.Degree] nvarchar(50) NULL,[Edu.Major] nvarchar(50) NULL,[Edu.Minor] nvarchar(50) NULL,[Edu.GPA] nvarchar(5) NULL,[Edu.GPAScale] nvarchar(5) NULL,[Edu.School] nvarchar(100) NULL,[Edu.Loc.CountryRegion] nvarchar(100) NULL,[Edu.Loc.State] nvarchar(100) NULL,[Edu.Loc.City] nvarchar(100) NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_vProductAndDescription] (
[ProductID] int NOT NULL,[Name] nvarchar(50) NOT NULL,[ProductModel] nvarchar(50) NOT NULL,[CultureID] nchar(6) NOT NULL,[Description] nvarchar(400) NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_vProductModelCatalogDescription] (
[ProductModelID] int NOT NULL,[Name] nvarchar(50) NOT NULL,[Summary] nvarchar(max) NULL,[Manufacturer] nvarchar(max) NULL,[Copyright] nvarchar(30) NULL,[ProductURL] nvarchar(256) NULL,[WarrantyPeriod] nvarchar(256) NULL,[WarrantyDescription] nvarchar(256) NULL,[NoOfYears] nvarchar(256) NULL,[MaintenanceDescription] nvarchar(256) NULL,[Wheel] nvarchar(256) NULL,[Saddle] nvarchar(256) NULL,[Pedal] nvarchar(256) NULL,[BikeFrame] nvarchar(max) NULL,[Crankset] nvarchar(256) NULL,[PictureAngle] nvarchar(256) NULL,[PictureSize] nvarchar(256) NULL,[ProductPhotoID] nvarchar(256) NULL,[Material] nvarchar(256) NULL,[Color] nvarchar(256) NULL,[ProductLine] nvarchar(256) NULL,[Style] nvarchar(256) NULL,[RiderExperience] nvarchar(1024) NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_vProductModelInstructions] (
[ProductModelID] int NOT NULL,[Name] nvarchar(50) NOT NULL,[Instructions] nvarchar(max) NULL,[LocationID] int NULL,[SetupHours] decimal(9,4) NULL,[MachineHours] decimal(9,4) NULL,[LaborHours] decimal(9,4) NULL,[LotSize] int NULL,[Step] nvarchar(1024) NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_vSalesPerson] (
[BusinessEntityID] int NOT NULL,[Title] nvarchar(8) NULL,[FirstName] nvarchar(50) NOT NULL,[MiddleName] nvarchar(50) NULL,[LastName] nvarchar(50) NOT NULL,[Suffix] nvarchar(10) NULL,[JobTitle] nvarchar(50) NOT NULL,[PhoneNumber] nvarchar(25) NULL,[PhoneNumberType] nvarchar(50) NULL,[EmailAddress] nvarchar(50) NULL,[EmailPromotion] int NOT NULL,[AddressLine1] nvarchar(60) NOT NULL,[AddressLine2] nvarchar(60) NULL,[City] nvarchar(30) NOT NULL,[StateProvinceName] nvarchar(50) NOT NULL,[PostalCode] nvarchar(15) NOT NULL,[CountryRegionName] nvarchar(50) NOT NULL,[TerritoryName] nvarchar(50) NULL,[TerritoryGroup] nvarchar(50) NULL,[SalesQuota] money NULL,[SalesYTD] money NOT NULL,[SalesLastYear] money NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_SalesTerritoryHistory] (
[BusinessEntityID] int NOT NULL,[TerritoryID] int NOT NULL,[StartDate] datetime NOT NULL,[EndDate] datetime NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_vSalesPersonSalesByFiscalYears] (
[SalesPersonID] int NULL,[FullName] nvarchar(152) NULL,[JobTitle] nvarchar(50) NOT NULL,[SalesTerritory] nvarchar(50) NOT NULL,[2002] money NULL,[2003] money NULL,[2004] money NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_vStoreWithDemographics] (
[BusinessEntityID] int NOT NULL,[Name] nvarchar(50) NOT NULL,[AnnualSales] money NULL,[AnnualRevenue] money NULL,[BankName] nvarchar(50) NULL,[BusinessType] nvarchar(5) NULL,[YearOpened] int NULL,[Specialty] nvarchar(50) NULL,[SquareFeet] int NULL,[Brands] nvarchar(30) NULL,[Internet] nvarchar(30) NULL,[NumberEmployees] int NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_vStoreWithContacts] (
[BusinessEntityID] int NOT NULL,[Name] nvarchar(50) NOT NULL,[ContactType] nvarchar(50) NOT NULL,[Title] nvarchar(8) NULL,[FirstName] nvarchar(50) NOT NULL,[MiddleName] nvarchar(50) NULL,[LastName] nvarchar(50) NOT NULL,[Suffix] nvarchar(10) NULL,[PhoneNumber] nvarchar(25) NULL,[PhoneNumberType] nvarchar(50) NULL,[EmailAddress] nvarchar(50) NULL,[EmailPromotion] int NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_ScrapReason] (
[ScrapReasonID] smallint NOT NULL,[Name] nvarchar(50) NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_vStoreWithAddresses] (
[BusinessEntityID] int NOT NULL,[Name] nvarchar(50) NOT NULL,[AddressType] nvarchar(50) NOT NULL,[AddressLine1] nvarchar(60) NOT NULL,[AddressLine2] nvarchar(60) NULL,[City] nvarchar(30) NOT NULL,[StateProvinceName] nvarchar(50) NOT NULL,[PostalCode] nvarchar(15) NOT NULL,[CountryRegionName] nvarchar(50) NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Purchasing_vVendorWithContacts] (
[BusinessEntityID] int NOT NULL,[Name] nvarchar(50) NOT NULL,[ContactType] nvarchar(50) NOT NULL,[Title] nvarchar(8) NULL,[FirstName] nvarchar(50) NOT NULL,[MiddleName] nvarchar(50) NULL,[LastName] nvarchar(50) NOT NULL,[Suffix] nvarchar(10) NULL,[PhoneNumber] nvarchar(25) NULL,[PhoneNumberType] nvarchar(50) NULL,[EmailAddress] nvarchar(50) NULL,[EmailPromotion] int NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Purchasing_vVendorWithAddresses] (
[BusinessEntityID] int NOT NULL,[Name] nvarchar(50) NOT NULL,[AddressType] nvarchar(50) NOT NULL,[AddressLine1] nvarchar(60) NOT NULL,[AddressLine2] nvarchar(60) NULL,[City] nvarchar(30) NOT NULL,[StateProvinceName] nvarchar(50) NOT NULL,[PostalCode] nvarchar(15) NOT NULL,[CountryRegionName] nvarchar(50) NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_ProductCategory] (
[ProductCategoryID] int NOT NULL,[Name] nvarchar(50) NOT NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Purchasing_ShipMethod] (
[ShipMethodID] int NOT NULL,[Name] nvarchar(50) NOT NULL,[ShipBase] money NOT NULL,[ShipRate] money NOT NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_ProductCostHistory] (
[ProductID] int NOT NULL,[StartDate] datetime NOT NULL,[EndDate] datetime NULL,[StandardCost] money NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_ProductDescription] (
[ProductDescriptionID] int NOT NULL,[Description] nvarchar(400) NOT NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_ShoppingCartItem] (
[ShoppingCartItemID] int NOT NULL,[ShoppingCartID] nvarchar(50) NOT NULL,[Quantity] int NOT NULL,[ProductID] int NOT NULL,[DateCreated] datetime NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_ProductDocument] (
[ProductID] int NOT NULL,[DocumentNode] hierarchyid NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [dbo_DatabaseLog] (
[DatabaseLogID] int NOT NULL,[PostTime] datetime NOT NULL,[DatabaseUser] nvarchar(128) NOT NULL,[Event] nvarchar(128) NOT NULL,[Schema] nvarchar(128) NULL,[Object] nvarchar(128) NULL,[TSQL] nvarchar(max) NOT NULL,[XmlEvent] xml NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_ProductInventory] (
[ProductID] int NOT NULL,[LocationID] smallint NOT NULL,[Shelf] nvarchar(10) NOT NULL,[Bin] tinyint NOT NULL,[Quantity] smallint NOT NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_SpecialOffer] (
[SpecialOfferID] int NOT NULL,[Description] nvarchar(255) NOT NULL,[DiscountPct] smallmoney NOT NULL,[Type] nvarchar(50) NOT NULL,[Category] nvarchar(50) NOT NULL,[StartDate] datetime NOT NULL,[EndDate] datetime NOT NULL,[MinQty] int NOT NULL,[MaxQty] int NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [dbo_ErrorLog] (
[ErrorLogID] int NOT NULL,[ErrorTime] datetime NOT NULL,[UserName] nvarchar(128) NOT NULL,[ErrorNumber] int NOT NULL,[ErrorSeverity] int NULL,[ErrorState] int NULL,[ErrorProcedure] nvarchar(126) NULL,[ErrorLine] int NULL,[ErrorMessage] nvarchar(4000) NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_ProductListPriceHistory] (
[ProductID] int NOT NULL,[StartDate] datetime NOT NULL,[EndDate] datetime NULL,[ListPrice] money NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_SpecialOfferProduct] (
[SpecialOfferID] int NOT NULL,[ProductID] int NOT NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_ProductModel] (
[ProductModelID] int NOT NULL,[Name] nvarchar(50) NOT NULL,[CatalogDescription] xml NULL,[Instructions] xml NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_ProductModelIllustration] (
[ProductModelID] int NOT NULL,[IllustrationID] int NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [dbo_AWBuildVersion] (
[SystemInformationID] tinyint NOT NULL,[Database Version] nvarchar(25) NOT NULL,[VersionDate] datetime NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_ProductModelProductDescriptionCulture] (
[ProductModelID] int NOT NULL,[ProductDescriptionID] int NOT NULL,[CultureID] nchar(6) NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_BillOfMaterials] (
[BillOfMaterialsID] int NOT NULL,[ProductAssemblyID] int NULL,[ComponentID] int NOT NULL,[StartDate] datetime NOT NULL,[EndDate] datetime NULL,[UnitMeasureCode] nchar(3) NOT NULL,[BOMLevel] smallint NOT NULL,[PerAssemblyQty] decimal(8,2) NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_Store] (
[BusinessEntityID] int NOT NULL,[Name] nvarchar(50) NOT NULL,[SalesPersonID] int NULL,[Demographics] xml NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_ProductPhoto] (
[ProductPhotoID] int NOT NULL,[ThumbNailPhoto] varbinary(max) NULL,[ThumbnailPhotoFileName] nvarchar(50) NULL,[LargePhoto] varbinary(max) NULL,[LargePhotoFileName] nvarchar(50) NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_ProductProductPhoto] (
[ProductID] int NOT NULL,[ProductPhotoID] int NOT NULL,[Primary] bit NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_ProductReview] (
[ProductReviewID] int NOT NULL,[ProductID] int NOT NULL,[ReviewerName] nvarchar(50) NOT NULL,[ReviewDate] datetime NOT NULL,[EmailAddress] nvarchar(50) NOT NULL,[Rating] int NOT NULL,[Comments] nvarchar(3850) NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [dbo_sysdiagrams] (
[name] nvarchar(128) NOT NULL,[principal_id] int NOT NULL,[diagram_id] int NOT NULL,[version] int NULL,[definition] varbinary(max) NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_ProductSubcategory] (
[ProductSubcategoryID] int NOT NULL,[ProductCategoryID] int NOT NULL,[Name] nvarchar(50) NOT NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Purchasing_ProductVendor] (
[ProductID] int NOT NULL,[BusinessEntityID] int NOT NULL,[AverageLeadTime] int NOT NULL,[StandardPrice] money NOT NULL,[LastReceiptCost] money NULL,[LastReceiptDate] datetime NULL,[MinOrderQty] int NOT NULL,[MaxOrderQty] int NOT NULL,[OnOrderQty] int NULL,[UnitMeasureCode] nchar(3) NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_UnitMeasure] (
[UnitMeasureCode] nchar(3) NOT NULL,[Name] nvarchar(50) NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Purchasing_Vendor] (
[BusinessEntityID] int NOT NULL,[AccountNumber] nvarchar(15) NOT NULL,[Name] nvarchar(50) NOT NULL,[CreditRating] tinyint NOT NULL,[PreferredVendorStatus] bit NOT NULL,[ActiveFlag] bit NOT NULL,[PurchasingWebServiceURL] nvarchar(1024) NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_CountryRegionCurrency] (
[CountryRegionCode] nvarchar(3) NOT NULL,[CurrencyCode] nchar(3) NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_WorkOrder] (
[WorkOrderID] int NOT NULL,[ProductID] int NOT NULL,[OrderQty] int NOT NULL,[StockedQty] int NOT NULL,[ScrappedQty] smallint NOT NULL,[StartDate] datetime NOT NULL,[EndDate] datetime NULL,[DueDate] datetime NOT NULL,[ScrapReasonID] smallint NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Purchasing_PurchaseOrderDetail] (
[PurchaseOrderID] int NOT NULL,[PurchaseOrderDetailID] int NOT NULL,[DueDate] datetime NOT NULL,[OrderQty] smallint NOT NULL,[ProductID] int NOT NULL,[UnitPrice] money NOT NULL,[LineTotal] money NOT NULL,[ReceivedQty] decimal(8,2) NOT NULL,[RejectedQty] decimal(8,2) NOT NULL,[StockedQty] decimal(9,2) NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_CreditCard] (
[CreditCardID] int NOT NULL,[CardType] nvarchar(50) NOT NULL,[CardNumber] nvarchar(25) NOT NULL,[ExpMonth] tinyint NOT NULL,[ExpYear] smallint NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_Culture] (
[CultureID] nchar(6) NOT NULL,[Name] nvarchar(50) NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_WorkOrderRouting] (
[WorkOrderID] int NOT NULL,[ProductID] int NOT NULL,[OperationSequence] smallint NOT NULL,[LocationID] smallint NOT NULL,[ScheduledStartDate] datetime NOT NULL,[ScheduledEndDate] datetime NOT NULL,[ActualStartDate] datetime NULL,[ActualEndDate] datetime NULL,[ActualResourceHrs] decimal(9,4) NULL,[PlannedCost] money NOT NULL,[ActualCost] money NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_Currency] (
[CurrencyCode] nchar(3) NOT NULL,[Name] nvarchar(50) NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Purchasing_PurchaseOrderHeader] (
[PurchaseOrderID] int NOT NULL,[RevisionNumber] tinyint NOT NULL,[Status] tinyint NOT NULL,[EmployeeID] int NOT NULL,[VendorID] int NOT NULL,[ShipMethodID] int NOT NULL,[OrderDate] datetime NOT NULL,[ShipDate] datetime NULL,[SubTotal] money NOT NULL,[TaxAmt] money NOT NULL,[Freight] money NOT NULL,[TotalDue] money NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_CurrencyRate] (
[CurrencyRateID] int NOT NULL,[CurrencyRateDate] datetime NOT NULL,[FromCurrencyCode] nchar(3) NOT NULL,[ToCurrencyCode] nchar(3) NOT NULL,[AverageRate] money NOT NULL,[EndOfDayRate] money NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_Customer] (
[CustomerID] int NOT NULL,[PersonID] int NULL,[StoreID] int NULL,[TerritoryID] int NULL,[AccountNumber] varchar(10) NOT NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Production_Document] (
[DocumentNode] hierarchyid NOT NULL,[DocumentLevel] smallint NULL,[Title] nvarchar(50) NOT NULL,[Owner] int NOT NULL,[FolderFlag] bit NOT NULL,[FileName] nvarchar(400) NOT NULL,[FileExtension] nvarchar(8) NOT NULL,[Revision] nchar(5) NOT NULL,[ChangeNumber] int NOT NULL,[Status] tinyint NOT NULL,[DocumentSummary] nvarchar(max) NULL,[Document] varbinary(max) NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_SalesOrderDetail] (
[SalesOrderID] int NOT NULL,[SalesOrderDetailID] int NOT NULL,[CarrierTrackingNumber] nvarchar(25) NULL,[OrderQty] smallint NOT NULL,[ProductID] int NOT NULL,[SpecialOfferID] int NOT NULL,[UnitPrice] money NOT NULL,[UnitPriceDiscount] money NOT NULL,[LineTotal] numeric(38,6) NOT NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [Sales_SalesOrderHeader] (
[SalesOrderID] int NOT NULL,[RevisionNumber] tinyint NOT NULL,[OrderDate] datetime NOT NULL,[DueDate] datetime NOT NULL,[ShipDate] datetime NULL,[Status] tinyint NOT NULL,[OnlineOrderFlag] bit NOT NULL,[SalesOrderNumber] nvarchar(25) NOT NULL,[PurchaseOrderNumber] nvarchar(25) NULL,[AccountNumber] nvarchar(15) NULL,[CustomerID] int NOT NULL,[SalesPersonID] int NULL,[TerritoryID] int NULL,[BillToAddressID] int NOT NULL,[ShipToAddressID] int NOT NULL,[ShipMethodID] int NOT NULL,[CreditCardID] int NULL,[CreditCardApprovalCode] varchar(15) NULL,[CurrencyRateID] int NULL,[SubTotal] money NOT NULL,[TaxAmt] money NOT NULL,[Freight] money NOT NULL,[TotalDue] money NOT NULL,[Comment] nvarchar(128) NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)



CREATE TABLE [HumanResources_EmployeeDepartmentHistory] (
[BusinessEntityID] int NOT NULL,[DepartmentID] smallint NOT NULL,[ShiftID] tinyint NOT NULL,[StartDate] date NOT NULL,[EndDate] date NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [HumanResources_Employee] (
[BusinessEntityID] int NOT NULL,[NationalIDNumber] nvarchar(15) NOT NULL,[LoginID] nvarchar(256) NOT NULL,[OrganizationNode] hierarchyid NULL,[OrganizationLevel] smallint NULL,[JobTitle] nvarchar(50) NOT NULL,[BirthDate] date NOT NULL,[MaritalStatus] nchar(1) NOT NULL,[Gender] nchar(1) NOT NULL,[HireDate] date NOT NULL,[SalariedFlag] bit NOT NULL,[VacationHours] smallint NOT NULL,[SickLeaveHours] smallint NOT NULL,[CurrentFlag] bit NOT NULL,[rowguid] uniqueidentifier NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [HumanResources_Department] (
[DepartmentID] smallint NOT NULL,[Name] varchar(2000) NOT NULL,[GroupName] varchar(2000) NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [HumanResources_Shift] (
[ShiftID] tinyint NOT NULL,[Name] varchar(2000) NOT NULL,[StartTime] time NOT NULL,[EndTime] time NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)
CREATE TABLE [HumanResources_EmployeePayHistory] (
[BusinessEntityID] int NOT NULL,[RateChangeDate] datetime NOT NULL,[Rate] money NOT NULL,[PayFrequency] tinyint NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)


CREATE TABLE [dbo_Sales_CreditCard] (
[CreditCardID] smallint NOT NULL,[CardType] nvarchar(50) NOT NULL,[CardNumber] bigint NOT NULL,[ExpMonth] tinyint NOT NULL,[ExpYear] smallint NOT NULL,[ModifiedDate] datetime NOT NULL,
checksum binary(16), is_deleted bit, is_current bit
)


CREATE TABLE [dbo_Production_Location] (
[LocationID] varchar(100) NULL,[Name] varchar(100) NULL,[CostRate] varchar(100) NULL,[Availability] varchar(100) NULL,[ModifiedDate] varchar(100) NULL,
checksum binary(16), is_deleted bit, is_current bit
)


---code to get create table code:
--DECLARE @table_schema nvarchar(200)
--DECLARE @table_name nvarchar(200)
--declare @sql varchar(MAX)

--DECLARE cursor_table_name CURSOR FOR  -- khai báo con trỏ 
--SELECT
--  	TABLE_SCHEMA, TABLE_NAME
--FROM
--  	INFORMATION_SCHEMA.TABLES     -- dữ liệu trỏ tới

--OPEN cursor_table_name                -- Mở con trỏ
--FETCH NEXT FROM cursor_table_name     -- Đọc dòng đầu tiên
--      INTO @table_schema, @table_name
--WHILE @@FETCH_STATUS = 0          --vòng lặp WHILE khi đọc Cursor thành công
--BEGIN
--	Set @sql ='';
    
--	WITH q AS (
--    SELECT
--        c.TABLE_SCHEMA,
--        c.TABLE_NAME,
--        c.ORDINAL_POSITION,
--        c.COLUMN_NAME,
--        c.DATA_TYPE,
--        CASE
--            WHEN c.DATA_TYPE IN ( N'binary', N'varbinary'                    ) THEN ( CASE c.CHARACTER_OCTET_LENGTH   WHEN -1 THEN N'(max)' ELSE CONCAT( N'(', c.CHARACTER_OCTET_LENGTH  , N')' ) END )
--            WHEN c.DATA_TYPE IN ( N'char', N'varchar', N'nchar', N'nvarchar' ) THEN ( CASE c.CHARACTER_MAXIMUM_LENGTH WHEN -1 THEN N'(max)' ELSE CONCAT( N'(', c.CHARACTER_MAXIMUM_LENGTH, N')' ) END )
--            WHEN c.DATA_TYPE IN ( N'datetime2', N'datetimeoffset'            ) THEN CONCAT( N'(', c.DATETIME_PRECISION, N')' )
--            WHEN c.DATA_TYPE IN ( N'decimal', N'numeric'                     ) THEN CONCAT( N'(', c.NUMERIC_PRECISION , N',', c.NUMERIC_SCALE, N')' )
--        END AS DATA_TYPE_PARAMETER,
--        CASE c.IS_NULLABLE
--            WHEN N'NO'  THEN N' NOT NULL'
--            WHEN N'YES' THEN     N' NULL'
--        END AS IS_NULLABLE2
--    FROM
--        INFORMATION_SCHEMA.COLUMNS AS c
--	)
--	SELECT
--		@sql=@sql+CONCAT('[', q.COLUMN_NAME, '] ', q.DATA_TYPE, ISNULL( q.DATA_TYPE_PARAMETER, N'' ), q.IS_NULLABLE2, ',')
--	FROM
--		q
--	WHERE
--		q.TABLE_SCHEMA = @table_schema
--		and q.TABLE_NAME = @table_name
--	ORDER BY
--		q.TABLE_SCHEMA,
--		q.TABLE_NAME,
--		q.ORDINAL_POSITION;


--	print CONCAT('CREATE TABLE [', @table_schema, '_', @table_name,'] (')
--	print @sql
--	print 'checksum bigint, is_deleted bit, is_current bit'
--	print ')'
--    FETCH NEXT FROM cursor_table_name -- Đọc dòng tiếp
--          INTO @table_schema, @table_name
--END

--CLOSE cursor_table_name              -- Đóng Cursor
--DEALLOCATE cursor_table_name         -- Giải phóng tài nguyên










create table [Person-GeneralContact]
(
  BusinessEntityID INT,
  PersonType VARCHAR(2) ,
  ModifiedDate DATETIME ,
  FirstName VARCHAR(50),
  MiddleName VARCHAR(50),
  LastName VARCHAR(50),
  PersonAddressDetail VARCHAR(50),
  checksum binary(16), is_deleted bit, is_current bit
 )


create table [Person-IndividualCustomer]
(
  BusinessEntityID INT ,
  PersonType VARCHAR(2) ,
  ModifiedDate DATETIME,
  FirstName VARCHAR(50),
  MiddleName VARCHAR(50) ,
  LastName VARCHAR(50) ,
  AddressLine1 VARCHAR(100),
  City VARCHAR(50),
  PostalCode VARCHAR(10) ,
  AddressName VARCHAR(50),
  StateProvinceCode VARCHAR(10),
  CountryRegionCode VARCHAR(10),
  StateProvinceName VARCHAR(50),
  checksum binary(16), is_deleted bit, is_current bit
)


create table [Person-Non-salesEmployee]
(
  BusinessEntityID INT ,
  PersonType VARCHAR(2) ,
  ModifiedDate DATETIME,
  FirstName VARCHAR(50),
  MiddleName VARCHAR(50) ,
  LastName VARCHAR(50) ,
  AddressLine1 VARCHAR(100),
  City VARCHAR(50),
  PostalCode VARCHAR(10) ,
  AddressName VARCHAR(50),
  StateProvinceCode VARCHAR(10),
  CountryRegionCode VARCHAR(10),
  StateProvinceName VARCHAR(50),
  checksum binary(16), is_deleted bit, is_current bit
)


CREATE TABLE [Person-SalesPerson] (
    BusinessEntityID INT,
    PersonType VARCHAR(2),
    ModifiedDate DATETIME,
    FirstName VARCHAR(50),
    MiddleName VARCHAR(50),
    LastName VARCHAR(50),
    AddressLine1 VARCHAR(100),
    City VARCHAR(50),
    PostalCode VARCHAR(20),
    AddressName VARCHAR(50),
    StateProvinceCode VARCHAR(10),
    CountryRegionCode VARCHAR(10),
    StateProvinceName VARCHAR(50),
	checksum binary(16), is_deleted bit, is_current bit
);

create table [Person-StoreContact]
(
  BusinessEntityID INT,
  PersonType VARCHAR(2) ,
  ModifiedDate DATETIME ,
  FirstName VARCHAR(50),
  MiddleName VARCHAR(50),
  LastName VARCHAR(50),
  checksum binary(16), is_deleted bit, is_current bit
)
create table [Person-VendorContact]
(
	  BusinessEntityID INT,
	  PersonType VARCHAR(2) ,
	  ModifiedDate DATETIME ,
	  FirstName VARCHAR(50),
	  MiddleName VARCHAR(50),
	  LastName VARCHAR(50),
	  checksum binary(16), is_deleted bit, is_current bit
)








CREATE TABLE TransactionHistory (
    ProductName VARCHAR(100),
    TransactionID INT,
    ProductID INT,
    ReferenceOrderID INT,
    ReferenceOrderLineID INT,
    TransactionDate DATETIME,
    TransactionType CHAR(1),
    Quantity INT,
    ActualCost DECIMAL(18, 2),
    ModifiedDate DATETIME,
	checksum binary(16), is_deleted bit, is_current bit
);


CREATE TABLE CountryOfBusinessEntity (
    BusinessEntityID INT,
    StateProvinceCode VARCHAR(2),
    CountryRegionCode VARCHAR(2),
    StateProvinceName VARCHAR(100),
    CountryRegionName VARCHAR(100),
	checksum binary(16), is_deleted bit, is_current bit
);