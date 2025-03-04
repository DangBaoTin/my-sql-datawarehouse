CREATE OR REPLACE FUNCTION silver.load_silver_erp_px_cat_g1v2()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Loading silver.erp_px_cat_g1v2
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;
END;
$$;