/*
=======================================================================================================
Stored Procedure: Load Gold Table (Silver -> Gold)
=======================================================================================================
Script Purpose:
	This script loads data from a silver table into a corresponding gold table [dim_products].
	It also performs data integration where necessary. Additionaly, it loads log tables 
	with vital log details, essential for traceability and debugging.

Parameter: @job_run_id

Usage: EXEC gold.usp_load_gold_dim_products @job_run_id

Note:
	* Running this script independently demands that you assign an integer value to @job_run_id.
	* It is imperative that this value already exist in the log table [metadata.etl_job_run] due
		to the foreign key constraint set on dependent tables.
	* To test the working condition of this script, check folder titled "test_run".
=======================================================================================================
*/
CREATE OR ALTER PROCEDURE gold.usp_load_gold_dim_products @job_run_id INT AS
BEGIN
	-- Abort on severe error
	SET XACT_ABORT ON;

	-- Create and map values to variables where necessary
	DECLARE 
	@step_run_id INT,
	@step_name NVARCHAR(50) = 'usp_load_gold_products',
	@load_type NVARCHAR(50) = 'INCREMENTAL',
	@ingest_layer NVARCHAR(50) = 'GOLD',
	@ingest_table NVARCHAR(50) = 'dim_products',
	@start_time DATETIME,
	@end_time DATETIME,
	@step_duration INT,
	@step_status NVARCHAR(50) = 'RUNNING',
	@source_path NVARCHAR(50) = 'silver.products',
	@rows_in_source INT,
	@rows_to_load INT,
	@rows_updated INT = 0,
	@rows_inserted INT = 0,
	@rows_loaded INT = 0,
	@rows_diff INT;

	-- Capture start time
	SET @start_time = GETDATE();

	-- Retrieve total records from source table
	SELECT @rows_in_source = COUNT(*) FROM silver.products;

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
		DROP TABLE IF EXISTS #gold_stg_dim_products;

		-- Retrieve new records
		WITH new_records AS
		(
		SELECT
			sp.product_id,
			sp.product_name,
			sp.category,
			sp.sub_category
		FROM silver.products sp
		LEFT JOIN gold.dim_products dp
		ON sp.product_id = dp.product_id
		WHERE 
			dp.product_id IS NULL OR
			((dp.product_id IS NOT NULL) AND
			(COALESCE(dp.product_name, '') != COALESCE(sp.product_name, '') OR
			COALESCE(dp.category, '') != COALESCE(sp.category, '') OR
			COALESCE(dp.sub_category, '') != COALESCE(sp.sub_category, '')))
		)
		-- Load new records into temp table
		SELECT
			product_id,
			product_name,
			category,
			sub_category
			INTO #gold_stg_dim_products
		FROM new_records;

		-- Retrieve total number of records from temp table
		SELECT @rows_to_load = COUNT(*) FROM #gold_stg_dim_products;

		-- Stop transaction if rows to load is zero or NULL
		IF @rows_to_load = 0 OR @rows_to_load IS NULL
		BEGIN
			SET @end_time = GETDATE();
			SET @step_duration = DATEDIFF(second, @start_time, @end_time);
			SET @step_status = 'NO_OPERATION';
			SET @rows_to_load = 0;
			SET @rows_diff = @rows_to_load - @rows_loaded;

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

		-- Update outdated records
		UPDATE tgt
			SET
				tgt.product_name = src.product_name,
				tgt.category = src.category,
				tgt.sub_category = src.sub_category
				FROM gold.dim_products tgt
				INNER JOIN #gold_stg_dim_products src
				ON tgt.product_id = src.product_id
			WHERE 
				tgt.product_name != src.product_name OR
				tgt.category != src.category OR
				tgt.sub_category != src.sub_category;

		-- Retrieve total rows updated
		SET @rows_updated = @@ROWCOUNT;

		-- Load new records into target table	
		INSERT INTO gold.dim_products
		(
			product_id,
			product_name,
			category,
			sub_category
		)
		SELECT
			src.product_id,
			src.product_name,
			src.category,
			src.sub_category
		FROM #gold_stg_dim_products src
		LEFT JOIN gold.dim_products tgt
		ON src.product_id = tgt.product_id
		WHERE tgt.product_id IS NULL;

		-- Retrieve total rows inserted
		SET @rows_inserted = @@ROWCOUNT;

		-- Finalize transaction on success
		COMMIT TRAN;

		-- Map values to variables in try block
		SET @end_time = GETDATE();
		SET @step_duration = DATEDIFF(second, @start_time, @end_time);
		SET @step_status = 'SUCCESSFUL';
		SET @rows_loaded = @rows_updated + @rows_inserted;
		SET @rows_diff = @rows_to_load - @rows_loaded;

		-- Update step-level log table on success
		UPDATE metadata.etl_step_run
			SET
				end_time = @end_time,
				step_duration_seconds = @step_duration,
				step_status = @step_status,
				rows_to_load = @rows_to_load,
				rows_updated = @rows_updated,
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

		-- Map zero to rows updated if NULL
		IF @rows_updated IS NULL SET @rows_updated = 0;

		-- Map zero to rows inserted if NULL
		IF @rows_inserted IS NULL SET @rows_inserted = 0;

		-- Calculate rows loaded & rows diff
		SET @rows_loaded = @rows_updated + @rows_inserted;
		SET @rows_diff = @rows_to_load - @rows_loaded;

		-- Update step-level log table on failure
		UPDATE metadata.etl_step_run
			SET
				end_time = @end_time,
				step_duration_seconds = @step_duration,
				step_status = @step_status,
				rows_to_load = @rows_to_load,
				rows_updated = @rows_updated,
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
