/*
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE 
		@start_time DATETIME,
		@end_time DATETIME,
		@start_batch_time DATETIME,
		@end_batch_time DATETIME;

	BEGIN TRY
	SET @start_batch_time = GETDATE()
		PRINT'===============================';
		PRINT 'Loading Bronze Layer';
		PRINT'===============================';
		PRINT '------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------';
		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: [bronze].[crm_cust_info]';
		TRUNCATE TABLE [bronze].[crm_cust_info] ;
		-- LOAD data to [bronze].[crm_cust_info]
		PRINT 'Insert Data into: [bronze].[crm_cust_info]'
		BULK INSERT [bronze].[crm_cust_info] 
		FROM 'D:\Data Engineer\3-Building a Modern Data Warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> The time to load Table is: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(30)) + 'seconds'

		SET @start_time = GETDATE()
		PRINT'---------------------------------------------'
		PRINT'>> Truncating Table: [bronze].[crm_cust_info]';
		TRUNCATE TABLE [bronze].[crm_prd_info];
		-- LOAD data to [bronze].[crm_prd_info]
		PRINT 'Insert Data into: [bronze].[crm_prd_info]';
		BULK INSERT [bronze].[crm_prd_info]
		FROM 'D:\Data Engineer\3-Building a Modern Data Warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> The time to load Table is: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(30)) + 'seconds'
		PRINT'---------------------------------------------'
		SET @start_time = GETDATE()
		PRINT'>> Truncating Table: [bronze].[crm_sales_details]';
		TRUNCATE TABLE [bronze].[crm_sales_details] ;
		-- LOAD data to [bronze].[crm_sales_details]
		PRINT 'Insert Data into: [bronze].[crm_sales_details]';
		BULK INSERT [bronze].[crm_sales_details]
		FROM 'D:\Data Engineer\3-Building a Modern Data Warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> The time to load Table is: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(30)) + 'seconds'
		SET @start_time = GETDATE()
		PRINT '------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------';
		PRINT'>> Truncating Table: [bronze].[erp_cust_az12]';
		SET @end_time = GETDATE()
		PRINT '>> The time to load Table is: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(30)) + 'seconds'
		PRINT'---------------------------------------------'
		SET @start_time = GETDATE()
		TRUNCATE TABLE [bronze].[erp_cust_az12];
		-- LOAD data to [bronze].[erp_cust_az12]
		PRINT 'Insert Data into: [bronze].[erp_cust_az12]';
		BULK INSERT [bronze].[erp_cust_az12]
		FROM 'D:\Data Engineer\3-Building a Modern Data Warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> The time to load Table is: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(30)) + 'seconds'
		PRINT'---------------------------------------------'
		SET @start_time = GETDATE()
		PRINT'>> Truncating Table: [bronze].[erp_loc_a101]';
		TRUNCATE TABLE [bronze].[erp_loc_a101];
		-- LOAD data to [bronze].[erp_loc_a101]
		PRINT 'Insert Data into: [bronze].[erp_loc_a101]';
		BULK INSERT [bronze].[erp_loc_a101]
		FROM 'D:\Data Engineer\3-Building a Modern Data Warehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> The time to load Table is: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(30)) + 'seconds'
		PRINT'---------------------------------------------'
		SET @start_time = GETDATE()
		PRINT'>> Truncating Table: [bronze].[erp_px_cat_g1v2]';
		TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];
		-- LOAD data to [bronze].[erp_px_cat_g1v2]
		PRINT 'Insert Data into: [bronze].[erp_px_cat_g1v2]';
		BULK INSERT [bronze].[erp_px_cat_g1v2]
		FROM 'D:\Data Engineer\3-Building a Modern Data Warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> The time to load Table is: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR(30)) + 'seconds'
		PRINT'---------------------------------------------'
		SET @end_batch_time = GETDATE()
		PRINT '>> Total time load: ' + CAST(DATEDIFF(SECOND,@start_batch_time,@end_batch_time) AS NVARCHAR(30)) + 'seconds'
		PRINT'---------------------------------------------'
	END TRY
	BEGIN CATCH
		PRINT '=============================';
		PRINT 'ERROR IN Loadin Date' + CAST(ERROR_MESSAGE() AS NVARCHAR(50));
		PRINT '=============================';
	END CATCH
END

