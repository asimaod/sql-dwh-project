/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    call  bronze.load_bronze();
===============================================================================
*/

create or replace procedure bronze.load_bronze() 
language plpgsql
as $$
declare start_time timestamp; 
		end_time timestamp;
		bl_start_time timestamp;
		bl_end_time timestamp;
begin
	bl_start_time:=now();
	raise notice '======================================================================================';
	raise notice 'Loading Bronze Layer';
	raise notice '======================================================================================';

	raise notice '--------------------------------------------------------------------------------------';
	raise notice 'Loading CRM Tables';
	raise notice '--------------------------------------------------------------------------------------';
	
	start_time:=now();
	raise notice '>> Truncating table: bronze.crm_cust_info ';
	truncate table bronze.crm_cust_info;
	
	raise notice '>> Inserting Data Into: bronze.crm_cust_info ';
	copy bronze.crm_cust_info
	from '/Users/a/Desktop/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
	delimiter ','
	csv header;
	end_time:=now();
	raise notice '>>Load Duration: % seconds', extract(epoch from(end_time-start_time)) as duration;
	raise notice '----------------------';
	
	start_time:=now();
	raise notice '>> Truncating table: bronze.crm_prd_info ';
	truncate table bronze.crm_prd_info;
	
	raise notice '>> Inserting Data Into: bronze.crm_prd_info ';
	copy bronze.crm_prd_info
	from '/Users/a/Desktop/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
	delimiter ','
	csv header;
	end_time:=now();
	raise notice '>>Load Duration: % seconds', extract(epoch from(end_time-start_time)) as duration;
	raise notice '----------------------';
	
	start_time:=now();
	raise notice '>> Truncating table: bronze.crm_sales_details ';
	truncate table bronze.crm_sales_details;
	
	raise notice '>> Inserting Data Into: bronze.crm_sales_details ';
	copy bronze.crm_sales_details
	from '/Users/a/Desktop/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
	delimiter ','
	csv header;
	end_time:=now();
	raise notice '>>Load Duration: % seconds', extract(epoch from(end_time-start_time)) as duration;

	raise notice '--------------------------------------------------------------------------------------';
	raise notice 'Loading ERP Tables';
	raise notice '--------------------------------------------------------------------------------------';
	
	start_time:=now();
	raise notice '>> Truncating table: bronze.erp_cust_az12 ';
	truncate table bronze.erp_cust_az12;
	
	raise notice '>> Inserting Data Into: bronze.erp_cust_az12 ';
	copy bronze.erp_cust_az12
	from '/Users/a/Desktop/sql-data-warehouse-project/datasets/source_erp/cust_az12.csv'
	delimiter ','
	csv header;
	end_time:=now();
	raise notice '>>Load Duration: % seconds', extract(epoch from(end_time-start_time)) as duration;
	raise notice '----------------------';
	
	start_time:=now();
	raise notice '>> Truncating table: bronze.erp_loc_a101 ';
	truncate table bronze.erp_loc_a101;
	
	raise notice '>> Inserting Data Into: bronze.erp_loc_a101 ';
	copy bronze.erp_loc_a101
	from '/Users/a/Desktop/sql-data-warehouse-project/datasets/source_erp/loc_a101.csv'
	delimiter ','
	csv header;
	end_time:=now();
	raise notice '>>Load Duration: % seconds', extract(epoch from(end_time-start_time)) as duration;
	raise notice '----------------------';
	
	start_time:=now();
	raise notice '>> Truncating table: bronze.erp_px_cat_g1v2 ';
	truncate table bronze.erp_px_cat_g1v2;
	
	raise notice '>> Inserting Data Into: bronze.erp_px_cat_g1v2 ';
	copy bronze.erp_px_cat_g1v2
	from '/Users/a/Desktop/sql-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv'
	delimiter ','
	csv header;
	end_time:=now();
	raise notice '>>Load Duration: % seconds', extract(epoch from(end_time-start_time)) as duration;
	
	bl_end_time:=now();
	raise notice '======================================================================================';
	raise notice '>>Loading Bronze Layer is completed';
	raise notice '>>    - Total Load Duration: % seconds', extract(epoch from(bl_end_time-bl_start_time)) as bronze_layer_load_duration;
	raise notice '======================================================================================';
	
exception when others
then
	raise notice '======================================================================================';
	raise notice 'Error occured during loading bronze layer';
	raise notice 'Error code: %. Error message: %.', sqlstate, sqlerrm;
	raise notice '======================================================================================';
end; 
$$
