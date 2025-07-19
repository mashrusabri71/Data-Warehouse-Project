/*
===============================================================================
DDL Script: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This is a sorted procedure that loads data into the bronze schema from external CSV files
    It performs the following actions: 
      - Truncate the bronze tables before inserting data
      -  Uses the 'BULK INSERT' command to load data from CSV files to bronze tables.

Parameter: 
    N/A
    Does not return any value
Usage: 
  Write the following command: 
    EXEC bronze_layer.load_bronze_layer
    
===============================================================================
*/
EXEC bronze_layer.load_bronze_layer
GO
CREATE OR ALTER PROCEDURE bronze_layer.load_bronze_layer AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '============================='
		PRINT 'Loading Bronze Layer'
		PRINT '============================='

		PRINT '-----------------------------'
		PRINT 'Loadding CRM Tables'
		PRINT '-----------------------------'

		SET @start_time  = GETDATE();
		PRINT '>>> Truncating: bronze_layer.crm_cust_info '
		TRUNCATE TABLE bronze_layer.crm_cust_info
		PRINT '>>> Inserting Data into: bronze_layer.crm_cust_info'
		BULK INSERT bronze_layer.crm_cust_info
		FROM 'D:\FreeCodeCamp\Data warehouse project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(

		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		
		
		);
		SET @end_time  = GETDATE();
		
		
		PRINT '<><><><><><><><<><><><><><<><>><><><><><>'
		PRINT '		Loading Time: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ 'seconds';
		PRINT '<><><><><><><><<><><><><><<><>><><><><><>'

		PRINT '>>> Truncating: bronze_layer.prd_info.csv '
		TRUNCATE TABLE bronze_layer.crm_prd_info
		PRINT '>>> Inserting Data into: bronze_layer.prd_info.csv'
		BULK INSERT bronze_layer.crm_prd_info
		FROM 'D:\FreeCodeCamp\Data warehouse project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time  = GETDATE();
		
		SET @start_time  = GETDATE();
		PRINT '>>> Truncating: bronze_layer.crm_sales_details'
		TRUNCATE TABLE bronze_layer.crm_sales_details
		PRINT '>>> Inserting Data into: bronze_layer.crm_sales_details'
		BULK INSERT bronze_layer.crm_sales_details
		FROM 'D:\FreeCodeCamp\Data warehouse project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time  = GETDATE();
		
		PRINT '<><><><><><><><<><><><><><<><>><><><><><>'
		PRINT '		Loading Time: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ 'seconds';
		PRINT '<><><><><><><><<><><><><><<><>><><><><><>'

		PRINT '-----------------------------'
		PRINT 'Loadding ERP Tables'
		PRINT '-----------------------------'
		
		SET @start_time  = GETDATE();
		PRINT '>>> Truncating: bronze_layer.erp_cust_az12'
		TRUNCATE TABLE bronze_layer.erp_cust_az12
		PRINT '>>> Inserting Data into: : bronze_layer.erp_cust_az12'
		BULK INSERT bronze_layer.erp_cust_az12
		FROM 'D:\FreeCodeCamp\Data warehouse project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time  = GETDATE();
		PRINT '<><><><><><><><<><><><><><<><>><><><><><>'
		PRINT '		Loading Time: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ 'seconds';
		PRINT '<><><><><><><><<><><><><><<><>><><><><><>'

		
		SET @start_time  = GETDATE();
		PRINT '>>>Truncating bronze_layer.erp_loc_a101'
		TRUNCATE TABLE bronze_layer.erp_loc_a101
		PRINT '>>> Inserting Data into: bronze_layer.erp_loc_a101'
		BULK INSERT bronze_layer.erp_loc_a101
		FROM 'D:\FreeCodeCamp\Data warehouse project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time  = GETDATE();
		PRINT '<><><><><><><><<><><><><><<><>><><><><><>'
		PRINT '		Loading Time: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ 'seconds';
		PRINT '<><><><><><><><<><><><><><<><>><><><><><>'

		


		SET @start_time  = GETDATE();
		PRINT '>>> Truncating bronze_layer.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze_layer.erp_px_cat_g1v2
		PRINT '>>> Inserting Data into: bronze_layer.erp_px_cat_g1v2'
		BULK INSERT bronze_layer.erp_px_cat_g1v2
		FROM 'D:\FreeCodeCamp\Data warehouse project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time  = GETDATE();

		PRINT '<><><><><><><><<><><><><><<><>><><><><><>'
		PRINT '		Loading Time: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ 'seconds';
		PRINT '<><><><><><><><<><><><><><<><>><><><><><>'

		PRINT ''
		
		SET @batch_end_time = GETDATE();

		PRINT'<><><><><><><><><><><><><><><><><><><><><><><><><><><><>'
		PRINT'|||||||||||||||||||||||||||||||||||||||||||||||||||||||||'
		PRINT'		Total Bronze Layer Load Time: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds' ;
		PRINT'|||||||||||||||||||||||||||||||||||||||||||||||||||||||||'
		PRINT'<><><><><><><><><><><><><><><><><><><><><><><><><><><><>'	
	END TRY
	BEGIN CATCH
		PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
		PRINT '			AN ERROR HAS OCCURED IN BRONZE_LAYER:';
		PRINT '			ERROR MESSAGE: ' + ERROR_MESSAGE();
		PRINT '			ERROR MESSAGE: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT '			ERROR MESSAGE: ' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'

	END CATCH


END
