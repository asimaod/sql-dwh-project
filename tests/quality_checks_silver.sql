/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- Table: bronze.crm_cust_info
-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result
select
	cst_id,
	count(*)
from bronze.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is NULL;

-- Check for unwanted Spaces
-- Expectation: No Results
select cst_firstname
from bronze.crm_cust_info
where cst_firstname!=trim(cst_firstname);

-- Data Standardization & Consistency
select distinct cst_gndr
from bronze.crm_cust_info;


-- Table: silver.crm_cust_info
-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result
select
	cst_id,
	count(*)
from silver.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is NULL;

-- Check for unwanted Spaces
-- Expectation: No Results
select cst_firstname
from silver.crm_cust_info
where cst_firstname!=trim(cst_firstname);

-- Data Standardization & Consistency
select distinct cst_gndr
from silver.crm_cust_info;


-- Table: bronze.crm_prd_info
-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result
select
	prd_id,
	count(*)
from bronze.crm_prd_info
group by prd_id
having count(*)>1 or prd_id is NULL;

-- Check for unwanted Spaces
-- Expectation: No Results
select prd_nm
from bronze.crm_prd_info
where prd_nm!=trim(prd_nm);

-- Check for NULLs or Negative Numbers
-- Expectation: No Results
select prd_cost
from bronze.crm_prd_info
where prd_cost<0 or prd_cost is null;

-- Data Standardization & Consistency
select distinct prd_line
from bronze.crm_prd_info;

-- Check for invalid date orders
select *
from bronze.crm_prd_info
where prd_end_dt<prd_start_dt;


-- Table: silver.crm_prd_info
-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result
select
	prd_id,
	count(*)
from silver.crm_prd_info
group by prd_id
having count(*)>1 or prd_id is NULL;

-- Check for unwanted Spaces
-- Expectation: No Results
select prd_nm
from silver.crm_prd_info
where prd_nm!=trim(prd_nm);

-- Check for NULLs or Negative Numbers
-- Expectation: No Results
select prd_cost
from silver.crm_prd_info
where prd_cost<0 or prd_cost is null;

-- Data Standardization & Consistency
select distinct prd_line
from silver.crm_prd_info;

-- Check for invalid date orders
select *
from silver.crm_prd_info
where prd_end_dt<prd_start_dt;


-- Table: bronze.crm_sales_details
-- Check for invalid dates
select nullif(sls_due_dt, 0) as sls_due_dt
from bronze.crm_sales_details
where sls_due_dt<=0 
	or length(sls_due_dt::text)!=8
	or sls_due_dt>20500101
	or sls_due_dt<19000101;
	
-- Check for invalid date orders
select * 
from bronze.crm_sales_details
where sls_order_dt>sls_ship_dt or sls_order_dt>sls_due_dt;

-- Check data consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity*Price
-- >> Values must not be NULL, zero, or negative 
select distinct
	sls_sales as old_sls_sales,
	sls_quantity,
	sls_price as old_sls_price,
	case when sls_sales is null or sls_sales<=0 or sls_sales!=sls_quantity*abs(sls_price) then abs(sls_price)*sls_quantity
		else sls_sales
	end sls_sales,
	case when sls_price is null or sls_price<=0 then sls_sales/nullif(sls_quantity,0)
		else sls_price
	end sls_price
from bronze.crm_sales_details
where sls_sales!=sls_quantity*sls_price
	or sls_sales is null or sls_quantity is null or sls_price is null
	or sls_sales<=0 or sls_quantity<=0 or sls_price<=0
order by sls_sales, sls_quantity, sls_price;


-- Table: silver.crm_sales_details
-- Check for invalid date orders
select * 
from silver.crm_sales_details
where sls_order_dt>sls_ship_dt or sls_order_dt>sls_due_dt;

-- Check data consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity*Price
-- >> Values must not be NULL, zero, or negative 
select distinct
	sls_sales,
	sls_quantity,
	sls_price
from silver.crm_sales_details
where sls_sales!=sls_quantity*sls_price
	or sls_sales is null or sls_quantity is null or sls_price is null
	or sls_sales<=0 or sls_quantity<=0 or sls_price<=0
order by sls_sales, sls_quantity, sls_price;


-- Table: bronze.erp_cust_az12
-- Identify out-of range dates
select distinct bdate
from bronze.erp_cust_az12
where bdate<'1925-01-01' or bdate>now();

-- Data standardization & consistency
select distinct 
	gen,
	case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
		when upper(trim(gen)) in ('M', 'MALE') then 'Male'
		else 'n/a'
	end gen
from bronze.erp_cust_az12;
	
	
-- Table: silver.erp_cust_az12
-- Identify out-of range dates
select distinct bdate
from silver.erp_cust_az12
where bdate<'1925-01-01' or bdate>now();

-- Data standardization & consistency
select distinct 
	gen
from silver.erp_cust_az12;

select * from silver.erp_cust_az12;


-- Table: bronze.erp_loc_a101
-- Data standardization & consistency
select distinct 
	cntry as old_cntry,
	case when trim(cntry)='DE' then 'Germany'
		when trim(cntry) in ('US', 'USA') then 'United States'
		when trim(cntry)='' or cntry is null then 'n/a'
		else trim(cntry)
	end cntry
from bronze.erp_loc_a101
order by cntry;


-- Table: silver.erp_loc_a101
-- Data standardization & consistency
select distinct cntry 
from silver.erp_loc_a101
order by cntry;


-- Table: bronze.erp_px_cat_g1v2
-- Check fow unwanted spaces
select *
from bronze.erp_px_cat_g1v2
where cat!=trim(cat) or subcat!=trim(subcat) or maintenance!=trim(maintenance);
	
-- Data standardization & consistency
select distinct maintenance
from bronze.erp_px_cat_g1v2;	


-- Table: silver.erp_px_cat_g1v2
-- Check fow unwanted spaces
select *
from silver.erp_px_cat_g1v2
where cat!=trim(cat) or subcat!=trim(subcat) or maintenance!=trim(maintenance);
	
-- Data standardization & consistency
select distinct maintenance
from silver.erp_px_cat_g1v2;
