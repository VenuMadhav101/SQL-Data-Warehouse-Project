-- CLEANING AND LOADING crm_cust_info

SELECT * FROM bronze.crm_cust_info;

-- check for nulls or duplicates in primary key.
-- there are some duplicates and nulls, this should be resolved as it is not expected.
SELECT 
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1

-- solving and cleaning
SELECT * FROM bronze.crm_cust_info
WHERE cst_id = 29466

SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id = 29466

SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info

-- IDENTIFYING ALL DUPLICATE VALUES
SELECT * FROM (
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
)t WHERE flag_last != 1

-- SELECTING ALL NON-DUPLICATE VALUES
SELECT * FROM (
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
)t WHERE flag_last = 1;

-- checking for unwanted spaces
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-- solving to remove spaces
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date 
FROM (
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1

-- finding empty spaces
-- no empty spaces
SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key)

-- data standardization & consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info



-- for prd table
SELECT 
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL
-- no duplicates or nulls

	
        

-- checking for unwanted spaces
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)
-- no unwanted spaces

-- Check for NULLs or Empty Spaces
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- Check for Invalid Date Orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT 
prd_key,
prd_id,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

-- for sales details table
-- identifying for empty spaces(no empty spaces)
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

-- check for duplicates
-- no duplicates
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price
END AS sls_price,
sls_quantity
FROM bronze.crm_sales_details


-- correcting the dates
SELECT 
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0

SELECT 
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details

-- length must be 8
SELECT 
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8

-- dates before the year 2050 and after 1900
SELECT 
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt > 20500101 OR sls_order_dt < 19000101

-- correcting for shipping dates
SELECT 
NULLIF(sls_ship_dt,0) sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101 
OR sls_ship_dt < 19000101

-- correcting for due dates
SELECT 
NULLIF(sls_due_dt,0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101 
OR sls_due_dt < 19000101

-- checking for invalid dates(no invalid dates)
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- total_sales = quantity * price, 
-- negatives, zeroes and nulls are not allowed
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL
OR sls_quantity IS NULL
OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- if sales is negative or zero or null, derive it using quantity and price.
-- if price is zero or null, calculate it using sales and quantity.
-- if price is negative, convert it to a positive value.

SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,

CASE WHEN sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price
END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL
OR sls_quantity IS NULL
OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- inserting into silver table


-- verifying the quality
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE  sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 
ORDER BY sls_sales, sls_quantity, sls_price

SELECT *
FROM silver.crm_sales_details


-- for erp_cust_az12 table
SELECT
cid,
bdate,
gen
FROM bronze.erp_cust_az12

SELECT * FROM [silver].[crm_cust_info]

SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12

-- identifying invalid dates
SELECT 
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1925-01-01' OR bdate > GETDATE()

-- fixing
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
END AS cid,
CASE WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
END AS bdate,
gen
FROM bronze.erp_cust_az12

-- fixing invalid gender values
SELECT DISTINCT gen
FROM bronze.erp_cust_az12

-- data standardization & consistency
SELECT DISTINCT
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'N/A'
END AS gen
FROM bronze.erp_cust_az12

-- final query

SELECT * FROM silver.erp_cust_az12

-- for erp_loc_a101
SELECT 
REPLACE(cid,'-','') cid,
cntry
FROM bronze.erp_loc_a101

SELECT cst_key FROM silver.crm_cust_info

SELECT 
REPLACE(cid,'-','') cid,
cntry
FROM bronze.erp_loc_a101 WHERE REPLACE(cid,'-','') NOT IN
(SELECT cst_key FROM silver.crm_cust_info)

-- data standardization & consistency
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

SELECT DISTINCT
cntry AS old_cntry,
CASE WHEN TRIM(cntry) = 'DE' THEN'Germany'
	 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
	 ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101 
ORDER BY cntry

-- final table
SELECT 
REPLACE(cid,'-','') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN'Germany'
	 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
	 ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101



SELECT * FROM silver.erp_loc_a101


-- for erp_px_cat_g1v2
SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2

-- checking for unwanted spaces(no unwanted spaces)
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- data standardization & consistency
SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
subcat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2

_g1v2
