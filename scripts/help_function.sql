CREATE OR REPLACE FUNCTION reset_bronze_tables()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  EXECUTE 'TRUNCATE TABLE bronze.crm_cust_info';
  EXECUTE 'TRUNCATE TABLE bronze.crm_prd_info';
  EXECUTE 'TRUNCATE TABLE bronze.crm_sales_details';
  EXECUTE 'TRUNCATE TABLE bronze.erp_loc_a101';
  EXECUTE 'TRUNCATE TABLE bronze.erp_cust_az12';
  EXECUTE 'TRUNCATE TABLE bronze.erp_px_cat_g1v2';
END;
$$;