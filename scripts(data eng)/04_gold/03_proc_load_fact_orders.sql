/*
=======================================================================================================
Stored Procedure: Load Gold Table (Silver -> Gold)
=======================================================================================================
Script Purpose:
	This script loads data from a silver table into a corresponding gold table [fact_orders].
	It also performs data integration where necessary. Additionaly, it loads log tables 
	with vital log details, essential for traceability and debugging.

Parameter: @job_run_id

Usage: EXEC usp_load_gold_fact_orders @job_run_id

Note:
	* Running this script independently demands that you assign an integer value to @job_run_id.
	* It is imperative that this value already exist in the log table [metadata.etl_job_run] due
	  to the foreign key constraint set on dependent tables.
	* To test the working condition of this script, check folder titled "test_run".
=======================================================================================================
*/
CREATE OR ALTER PROCEDURE gold.usp_load_gold_fact_orders @job_run_id INT AS
BEGIN
	-- Abort on severe error
	SET XACT_ABORT ON;

	-- Create and map values to variables where necessary
	DECLARE
	@step_run_id INT,
	@step_name NVARCHAR(50) = 'usp_load_gold_fact_orders',
	@load_type NVARCHAR(50) = 'INCREMENTAL',
	@ingest_layer NVARCHAR(50) = 'GOLD',
	@ingest_table NVARCHAR(50) = 'fact_orders',
	@start_time DATETIME,
	@end_time DATETIME,
	@step_duration INT,
	@step_status NVARCHAR(50) = 'RUNNING',
	@source_path NVARCHAR(50) = 'silver.orders',
	@rows_in_source INT,
	@rows_to_load INT,
	@rows_updated INT = 0,
	@rows_inserted INT = 0,
	@rows_loaded INT = 0,
	@rows_diff INT;

	-- Capture start time
	SET @start_time = GETDATE();

	-- Retrieve total records from source table
	SELECT @rows_in_source = COUNT(*) FROM silver.orders;

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
		-- Drop temp table if exists
		DROP TABLE IF EXISTS #gold_stg_fact_orders;

		-- Perform data integration
		WITH data_integration AS
		(
		SELECT
			so.order_id,
			dc.customer_key,
			dp.product_key,
			so.order_date,
			so.shipping_date,
			so.sales,
			so.quantity,
			so.discount,
			so.profit,
			so.unit_price
		FROM silver.orders so
		LEFT JOIN gold.dim_customers dc
		ON so.customer_id = dc.customer_id
		LEFT JOIN gold.dim_products dp
		ON so.product_id = dp.product_id
		)
		-- Retrive & load new records into temp table
		SELECT
			di.order_id,
			di.customer_key,
			di.product_key,
			di.order_date,
			di.shipping_date,
			di.sales,
			di.quantity,
			di.discount,
			di.profit,
			di.unit_price
			INTO #gold_stg_fact_orders
		FROM data_integration di
		LEFT JOIN gold.fact_orders fo
		ON di.order_id = fo.order_id
		AND di.product_key = fo.product_key
		WHERE fo.order_id IS NULL AND fo.product_key IS NULL;

		-- Retrieve total number of records from temp table
		SELECT @rows_to_load = COUNT(*) FROM #gold_stg_fact_orders;

		-- Stop transaction if rows to load is zero or NULL
		IF @rows_to_load = 0 OR @rows_to_load IS NULL
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

		-- Begin transaction
		BEGIN TRAN;

		-- Load new records into target table
		INSERT INTO gold.fact_orders
		(
			order_id,
			customer_key,
			product_key,
			order_date,
			shipping_date,
			sales,
			quantity,
			discount,
			profit,
			unit_price
		)
		SELECT
			order_id,
			customer_key,
			product_key,
			order_date,
			shipping_date,
			sales,
			quantity,
			discount,
			profit,
			unit_price
		FROM #gold_stg_fact_orders;

		-- Retrieve total rows inserted
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

		-- Map zero to rows inserted if NULL
		IF @rows_inserted IS NULL SET @rows_inserted = 0;

		-- Map value to rows loaded
		SET @rows_loaded = @rows_inserted;

		-- Calculate rows diff
		SET @rows_diff = @rows_to_load - @rows_loaded;

		-- Update step-level log table on failure
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
		);
	END CATCH;
END;
