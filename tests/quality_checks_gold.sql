USE DataWarehouse;

-- CREAYE DIMENSION CUSTOMERS

-- CHECKING FOR DUPLICATES(NO DUPLICATES)
SELECT cst_id, COUNT(*) FROM (
SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 ca
ON	ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON	ci.cst_key = la.cid
)t GROUP BY cst_id 
HAVING COUNT(*) > 1

SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 ca
ON	ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON	ci.cst_key = la.cid

-- DATA INTEGRATION (FIXING ERRORS IN GENDER VALUES)
SELECT DISTINCT
	ci.cst_gndr,
	ca.gen
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 ca
ON	ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON	ci.cst_key = la.cid
ORDER BY 1,2

-- ASSUMING crm AS MORE ACCURATE
SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr != 'Other' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'Other')
	END AS new_gen
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 ca
ON	ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON	ci.cst_key = la.cid
ORDER BY 1,2

-- FIXED QUERY
SELECT 
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'Other' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'Other')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 ca
ON	ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON	ci.cst_key = la.cid

-- GENERATING A SURROGATE KEY
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'Other' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'Other')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 ca
ON	ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON	ci.cst_key = la.cid

-- FINAL QUERY
CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'Other' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'Other')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 ca
ON	ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON	ci.cst_key = la.cid

SELECT * FROM gold.dim_customers

-- CREATE DIMENSION PRODUCTS

-- FILTERING OUT ALL HISTORICAL DATA
SELECT
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NOT NULL

-- GENERATING A SURROGATE KEY
SELECT
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NOT NULL

-- FINAL QUERY
CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NOT NULL

SELECT * FROM gold.dim_products

-- FACT SALES TABLE
CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id

SELECT * FROM gold.fact_sales

-- Foreign Key Integrity(Dimensions)
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE c.customer_key IS NULL
