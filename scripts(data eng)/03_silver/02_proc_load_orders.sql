/*
=======================================================================================================
Stored Procedure: Load Silver Table (Bronze -> Silver)
=======================================================================================================
Script Purpose:
	This script loads data from a bronze table into a corresponding silver table [orders].
	It also performs data transformaions where necessary. Additionaly, it loads log tables 
	with vital log details, essential not only for traceability and debugging, but for 
	monitoring the quality of the silver records.

Parameter: @job_run_id

Usage: EXEC silver.usp_load_silver_orders @job_run_id

Note:
	* Running this script independently demands that you assign an integer value to @job_run_id.
	* It is imperative that this value already exist in the log table [metadata.etl_job_run] due
	  to the foreign key constraint set on dependent tables.
	* To test the working condition of this script, check folder titled "test_run".
=======================================================================================================
*/
CREATE OR ALTER PROCEDURE silver.usp_load_silver_orders @job_run_id INT AS
BEGIN
	-- Abort on severe error
	SET XACT_ABORT ON;

	-- Create and map values to variables where necessary
	DECLARE
	@step_run_id INT,
	@step_name NVARCHAR(50) = 'usp_load_silver_orders',
	@load_type NVARCHAR(50) = 'INCREMENTAL',
	@ingest_layer NVARCHAR(50) = 'SILVER',
	@ingest_table NVARCHAR(50) = 'orders',
	@start_time DATETIME,
	@end_time DATETIME,
	@step_duration INT,
	@step_status NVARCHAR(50) = 'RUNNING',
	@source_path NVARCHAR(50) = 'bronze.orders',
	@rows_in_source INT,
	@rows_to_load INT,
	@rows_updated INT = 0,
	@rows_inserted INT = 0,
	@rows_loaded INT = 0,
	@rows_diff INT;

	-- Capture start time
	SET @start_time = GETDATE();

	-- Retrieve total rows from source table
	SELECT @rows_in_source = COUNT(*) FROM bronze.orders;

	-- Load log details at step level
	INSERT INTO metadata.etl_step_run
	(
		job_run_id,
		step_name,
		load_type,
		ingest_layer,
		ingest_table,
		start_time,
		step_status,
		source_path,
		rows_in_source,
		rows_updated,
		rows_inserted,
		rows_loaded
	)
	VALUES
	(
		@job_run_id,
		@step_name,
		@load_type,
		@ingest_layer,
		@ingest_table,
		@start_time,
		@step_status,
		@source_path,
		@rows_in_source,
		@rows_updated,
		@rows_inserted,
		@rows_loaded
	);
	-- Retrieve newly generated step_run_id
	SET @step_run_id = SCOPE_IDENTITY();

	BEGIN TRY
		-- Delete data in staging table
		TRUNCATE TABLE silver_stg.orders;

		-- Transform source records
		WITH data_transformations AS
		(
		SELECT
			order_id,
			customer_id,
			product_id,
			order_date,
			shipping_date,
			CASE
				WHEN (sales IS NOT NULL AND NOT sales <= 0) AND (unit_price IS NULL OR unit_price = 0) THEN sales
				WHEN sales IS NULL OR sales <= 0 OR sales != quantity * unit_price THEN ABS(quantity * unit_price)
				ELSE sales
			END AS sales,
			quantity,
			CASE
				WHEN discount < 0 THEN ABS(discount)
				WHEN discount IS NULL THEN 0
				ELSE discount
			END AS discount,
			profit,
			CASE
				WHEN unit_price IS NULL OR unit_price = 0 THEN ROUND(ABS(sales/NULLIF(quantity, 0)), 3)
				WHEN unit_price < 0 THEN ABS(unit_price)
				ELSE unit_price
			END AS unit_price
		FROM
		(
		SELECT
			order_id,
			customer_id,
			product_id,
			CONVERT(DATE, order_date, 103) AS order_date,
			CONVERT(DATE, shipping_date, 103) AS shipping_date,
			CAST(REPLACE(sales, ',', '.') AS DECIMAL(10, 3)) AS sales,
			quantity,
			CAST(REPLACE(discount, ',', '.') AS DECIMAL(10, 2)) AS discount,
			CAST(REPLACE(profit, ',', '.') AS DECIMAL(10, 3)) AS profit,
			CAST(REPLACE(unit_price, ',', '.') AS DECIMAL(10, 3)) AS unit_price
		FROM bronze.orders
		)SUB
		)
		-- Generate metadata columns
		, metadata_columns AS
		(
		SELECT
			order_id,
			customer_id,
			product_id,
			order_date,
			shipping_date,
			sales,
			quantity,
			discount,
			profit,
			unit_price,
			HASHBYTES('SHA2_256', CONCAT_WS('|',
			COALESCE(CAST(order_id AS NVARCHAR(10)), ''),
			COALESCE(CAST(customer_id AS NVARCHAR(10)), ''),
			COALESCE(CAST(product_id AS NVARCHAR(10)), ''),
			COALESCE(CONVERT(NVARCHAR(10), order_date, 120), ''),
			COALESCE(CONVERT(NVARCHAR(10), shipping_date, 120), ''),
			COALESCE(CONVERT(NVARCHAR(30), sales, 2), ''),
			COALESCE(CAST(quantity AS NVARCHAR(20)), ''),
			COALESCE(CONVERT(NVARCHAR(30), discount, 2), ''),
			COALESCE(CONVERT(NVARCHAR(30), profit, 2), ''),
			COALESCE(CONVERT(NVARCHAR(30), unit_price, 2), ''))) AS dwh_row_hash
		FROM data_transformations
		)
		-- Load into staging table
		INSERT INTO silver_stg.orders
		(
			order_id,
			customer_id,
			product_id,
			order_date,
			shipping_date,
			sales,
			quantity,
			discount,
			profit,
			unit_price,
			dwh_row_hash
		)
		SELECT
			mc.order_id,
			mc.customer_id,
			mc.product_id,
			mc.order_date,
			mc.shipping_date,
			mc.sales,
			mc.quantity,
			mc.discount,
			mc.profit,
			mc.unit_price,
			mc.dwh_row_hash
		FROM metadata_columns mc
		LEFT JOIN silver.orders o
		ON mc.order_id = o.order_id
		AND mc.product_id = o.product_id
		AND mc.dwh_row_hash = o.dwh_row_hash
		WHERE o.dwh_row_hash IS NULL;

		-- Retrieve total records from staging table
		SELECT @rows_to_load = COUNT(*) FROM silver_stg.orders;

		-- Stop operation if total records is zero or NULL
		IF @rows_to_load IS NULL OR @rows_to_load = 0 
		BEGIN
			SET @end_time = GETDATE();
			SET @step_duration = DATEDIFF(second, @start_time, @end_time);
			SET @step_status = 'NO_OPERATION';
			SET @rows_to_load = 0;
			SET @rows_diff = 0;

			UPDATE metadata.etl_step_run
				SET
					end_time = @end_time,
					step_duration_seconds = @step_duration,
					step_status = @step_status,
					rows_to_load = @rows_to_load,
					rows_diff = @rows_diff
				WHERE step_run_id = @step_run_id;

			RETURN;
		END;

		-- Drop temp table if exits
		DROP TABLE IF EXISTS #dq_metrics;

		-- Create a temporary table and load into it important dq metrics
		SELECT
			COUNT(*) AS rows_checked,
			COUNT(CONCAT_WS(',', order_id, product_id)) - COUNT(DISTINCT CONCAT_WS(',', order_id, product_id)) AS rows_failed_unq,
			SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS rows_failed_order_id_null,
			SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS rows_failed_customer_id_null,
			SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS rows_failed_product_id_null,
			SUM(CASE WHEN sales IS NULL OR sales <= 0 OR sales != quantity * unit_price THEN 1 ELSE 0 END) AS rows_failed_invalid_sales,
			SUM(CASE WHEN quantity IS NULL OR quantity <= 0 OR quantity != sales/NULLIF(unit_price, 0) THEN 1 ELSE 0 END) AS rows_failed_invalid_quantity,
			SUM(CASE WHEN unit_price IS NULL OR unit_price <= 0 OR unit_price != sales/NULLIF(quantity, 0) THEN 1 ELSE 0 END) AS rows_failed_unit_price
			INTO #dq_metrics
		FROM silver_stg.orders;

		-- Load data quality checks in log table
		INSERT INTO metadata.etl_data_quality_check
		(
			job_run_id,
			step_run_id,
			dq_layer_name,
			dq_table_name,
			dq_check_name,
			rows_checked,
			rows_failed,
			dq_status
		)
		SELECT @job_run_id, @step_run_id, @ingest_layer, @ingest_table, 'UNQ_ORDER_ID_PRODUCT_ID', rows_checked, rows_failed_unq AS rows_failed,
		CASE WHEN rows_failed_unq > 0 THEN 'FAILED' ELSE 'PASSED' END AS dq_status FROM #dq_metrics
		UNION ALL
		SELECT @job_run_id, @step_run_id, @ingest_layer, @ingest_table, 'ORDER_ID_NULL', rows_checked, rows_failed_order_id_null AS rows_failed,
		CASE WHEN rows_failed_order_id_null > 0 THEN 'FAILED' ELSE 'PASSED' END AS dq_status FROM #dq_metrics
		UNION ALL
		SELECT @job_run_id, @step_run_id, @ingest_layer, @ingest_table, 'CUSTOMER_ID_NULL', rows_checked, rows_failed_customer_id_null AS rows_failed,
		CASE WHEN rows_failed_customer_id_null > 0 THEN 'FAILED' ELSE 'PASSED' END AS dq_status FROM #dq_metrics
		UNION ALL
		SELECT @job_run_id, @step_run_id, @ingest_layer, @ingest_table, 'PRODUCT_ID_NULL', rows_checked, rows_failed_product_id_null AS rows_failed,
		CASE WHEN rows_failed_product_id_null > 0 THEN 'FAILED' ELSE 'PASSED' END AS dq_status FROM #dq_metrics
		UNION ALL
		SELECT @job_run_id, @step_run_id, @ingest_layer, @ingest_table, 'INVALID_SALES', rows_checked, rows_failed_invalid_sales AS rows_failed,
		CASE WHEN rows_failed_invalid_sales > 0 THEN 'FAILED' ELSE 'PASSED' END AS dq_status FROM #dq_metrics
		UNION ALL
		SELECT @job_run_id, @step_run_id, @ingest_layer, @ingest_table, 'INVALID_QUANTITY', rows_checked, rows_failed_invalid_quantity AS rows_failed,
		CASE WHEN rows_failed_invalid_quantity > 0 THEN 'FAILED' ELSE 'PASSED' END AS dq_status FROM #dq_metrics
		UNION ALL
		SELECT @job_run_id, @step_run_id, @ingest_layer, @ingest_table, 'INVALID_UNIT_PRICE', rows_checked, rows_failed_unit_price AS rows_failed,
		CASE WHEN rows_failed_unit_price > 0 THEN 'FAILED' ELSE 'PASSED' END AS dq_status FROM #dq_metrics;

		-- Stop operation when critical data quality rule is violated
		IF EXISTS 
			(SELECT 1 FROM metadata.etl_data_quality_check WHERE (step_run_id = @step_run_id) AND (dq_check_name = 'UNQ_ORDER_ID_PRODUCT_ID' 
			OR dq_check_name  = 'ORDER_ID_NULL' OR dq_check_name = 'CUSTOMER_ID_NULL' OR dq_check_name = 'PRODUCT_ID_NULL') AND (rows_failed > 0)) 
		THROW 50002, 'Unable to load due to critical rule(s) violated.', 2;

		-- Begin transaction
		BEGIN TRAN;

		-- Load into target table
		INSERT INTO silver.orders
		(
			order_id,
			customer_id,
			product_id,
			order_date,
			shipping_date,
			sales,
			quantity,
			discount,
			profit,
			unit_price,
			dwh_row_hash
		)
		SELECT
			order_id,
			customer_id,
			product_id,
			order_date,
			shipping_date,
			sales,
			quantity,
			discount,
			profit,
			unit_price,
			dwh_row_hash
		FROM silver_stg.orders;

		-- Retrieve total records loaded into target table
		SET @rows_inserted = @@ROWCOUNT;

		-- Finalize transaction on success
		COMMIT TRAN;

		-- Map values to variables in try block
		SET @end_time = GETDATE();
		SET @step_duration = DATEDIFF(second, @start_time, @end_time);
		SET @step_status = 'SUCCESSFUL';
		SET @rows_loaded = @rows_inserted;
		SET @rows_diff = @rows_to_load - @rows_loaded;

		-- Update step-level log table on success
		UPDATE metadata.etl_step_run
			SET
				end_time = @end_time,
				step_duration_seconds = @step_duration,
				step_status = @step_status,
				rows_to_load = @rows_to_load,
				rows_inserted = @rows_inserted,
				rows_loaded = @rows_loaded,
				rows_diff = @rows_diff
			WHERE step_run_id = @step_run_id;
	END TRY

	BEGIN CATCH
		-- Map values to variables in catch block
		SET @end_time = GETDATE();
		SET @step_duration = DATEDIFF(second, @start_time, @end_time);
		SET @step_status = 'FAILED';

		-- Rollback transaction on error
		IF @@TRANCOUNT > 0 ROLLBACK TRAN;

		-- Map zero to rows inserted when NULL
		IF @rows_inserted IS NULL SET @rows_inserted = 0;

		-- Retrieve rows loaded on error
		SET @rows_loaded = @rows_inserted;

		-- Retrieve rows difference on error
		SET @rows_diff = @rows_to_load - @rows_loaded;

		-- Update step-level log table on error
		UPDATE metadata.etl_step_run
			SET
				end_time = @end_time,
				step_duration_seconds = @step_duration,
				step_status = @step_status,
				rows_to_load = @rows_to_load,
				rows_inserted = @rows_inserted,
				rows_loaded = @rows_loaded,
				rows_diff = @rows_diff
			WHERE step_run_id = @step_run_id;

		-- Load error details into log table
		INSERT INTO metadata.etl_error_log
		(
			job_run_id,
			step_run_id,
			step_status,
			err_procedure,
			err_number,
			err_message,
			err_line
		)
		VALUES
		(
			@job_run_id,
			@step_run_id,
			@step_status,
			ERROR_PROCEDURE(),
			ERROR_NUMBER(),
			ERROR_MESSAGE(),
			ERROR_LINE()
		)
	END CATCH;
END;
