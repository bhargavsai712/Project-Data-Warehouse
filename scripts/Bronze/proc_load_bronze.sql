/*
Stored procedure to load data into bronze layer

PURPOSE:
Loads data into bronze schema from external CSV files.
Uses the BULK INSERT command.
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME, @batch_end DATETIME;
	BEGIN TRY
		SET @batch_start=GETDATE();
		PRINT'=======================================';
		PRINT'LOADING DATA INTO BRONZE LAYER';
		PRINT'=======================================';
		SET @start_time=GETDATE();
		PRINT'---------------------------------------';
		PRINT'LOADING CRM DATA';
		PRINT'---------------------------------------';
		PRINT'>> Truncating table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT'>> Loading data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\BHARGAV\Desktop\DW_project\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>>LOAD DURATION: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds';
		PRINT'------------------'

		SET @start_time=GETDATE();
		PRINT'>> Truncating table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT'>> Loading data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\BHARGAV\Desktop\DW_project\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>>LOAD DURATION: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds';
		PRINT'------------------'

		SET @start_time=GETDATE();
		PRINT'>> Truncating table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT'>> Loading data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\BHARGAV\Desktop\DW_project\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>>LOAD DURATION: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds';
		PRINT'------------------'
		PRINT'---------------------------------------';
		PRINT'LOADING ERP DATA';
		PRINT'---------------------------------------';
		SET @start_time=GETDATE();
		PRINT'>> Truncating table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT'>> Loading data into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\BHARGAV\Desktop\DW_project\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>>LOAD DURATION: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds';
		PRINT'------------------'

		SET @start_time=GETDATE();
		PRINT'>> Truncating table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT'>> Loading data into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\BHARGAV\Desktop\DW_project\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>>LOAD DURATION: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds';
		PRINT'------------------'

		SET @start_time=GETDATE();
		PRINT'>> Truncating table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT'>> Loading data into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\BHARGAV\Desktop\DW_project\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>>LOAD DURATION: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds';
		PRINT'------------------'
		SET @batch_end=GETDATE();
		PRINT '>>TOTAL BATCH LOAD DURATION: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds';
		PRINT'------------------'
	END TRY
	
	
	BEGIN CATCH
		PRINT 'ERROR OCCURED'
		PRINT 'ERROR MESSAGE:'+ ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE:'+CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE:'+CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH

END
