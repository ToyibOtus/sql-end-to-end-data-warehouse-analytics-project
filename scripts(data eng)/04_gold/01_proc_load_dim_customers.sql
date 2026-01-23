/*
=======================================================================================================
Stored Procedure: Load Gold Table (Silver -> Gold)
=======================================================================================================
Script Purpose:
	This script loads data from a silver table into a corresponding gold table [dim_customers].
	It also performs data integration where necessary. Additionaly, it loads log tables 
	with vital log details, essential for traceability and debugging.

Parameter: @job_run_id

Usage: EXEC usp_load_gold_dim_customers @job_run_id

Note:
	* Running this script independently demands that you assign an integer value to @job_run_id.
	* It is imperative that this value already exist in the log table [metadata.etl_job_run] due
	  to the foreign key constraint set on dependent tables.
	* To test the working condition of this script, check folder titled "test_run_gold".
=======================================================================================================
*/
CREATE OR ALTER PROCEDURE gold.usp_load_gold_dim_customers @job_run_id INT AS
BEGIN
	-- Abort on severe error
	SET XACT_ABORT ON;

	-- Create and map values to variables where necessary
	DECLARE 
	@step_run_id INT,
	@step_name NVARCHAR(50) = 'gold.usp_load_gold_dim_customers',
	@load_type NVARCHAR(50) = 'INCREMENTAL',
	@ingest_layer NVARCHAR(50) = 'gold',
	@ingest_table NVARCHAR(50) = 'dim_customers',
	@start_time DATETIME,
	@end_time DATETIME,
	@step_duration INT,
	@step_status NVARCHAR(50) = 'RUNNING',
	@source_path NVARCHAR(50) = 'silver.customers',
	@rows_in_source INT,
	@rows_to_load INT,
	@rows_updated INT = 0,
	@rows_inserted INT = 0,
	@rows_loaded INT = 0,
	@rows_diff INT;

	-- Capture start time
	SET @start_time = GETDATE();

	-- Retrieve total records from source table
	SELECT @rows_in_source = COUNT(*) FROM silver.customers;

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
		DROP TABLE IF EXISTS #gold_stg_dim_customers;

		-- Retrieve new records
		WITH new_records AS
		(
		SELECT
			sc.customer_id,
			sc.first_name,
			sc.last_name,
			sc.postal_code,
			sc.city,
			sc.country,
			sc.score
		FROM silver.customers sc
		LEFT JOIN gold.dim_customers dc
		ON sc.customer_id = dc.customer_id
		WHERE
			(dc.customer_id IS NULL) OR
			(dc.customer_id IS NOT NULL AND
			(COALESCE(sc.first_name, '') != COALESCE(dc.first_name, '') OR
			COALESCE(sc.last_name, '') != COALESCE(dc.last_name, '') OR
			COALESCE(sc.postal_code, -1) != COALESCE(dc.postal_code, -1) OR
			COALESCE(sc.city, '') != COALESCE(dc.city, '') OR
			COALESCE(sc.country, '') != COALESCE(dc.country, '') OR
			COALESCE(sc.score, -1) != COALESCE(dc.score, -1)))
		)
		-- Load new records into temp table
		SELECT
			customer_id,
			first_name,
			last_name,
			postal_code,
			city,
			country,
			score
			INTO #gold_stg_dim_customers
		FROM new_records;

		-- Retrieve total number of records from temp table
		SELECT @rows_to_load = COUNT(*) FROM #gold_stg_dim_customers;

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
				tgt.first_name = src.first_name,
				tgt.last_name = src.last_name, 
				tgt.postal_code = src.postal_code,
				tgt.city = src.city, 
				tgt.country = src.country, 
				tgt.score = src.score
				FROM gold.dim_customers tgt
				INNER JOIN #gold_stg_dim_customers src
				ON tgt.customer_id = src.customer_id
			WHERE 
				COALESCE(tgt.first_name, '') != COALESCE(src.first_name, '') OR
				COALESCE(tgt.last_name, '') != COALESCE(src.last_name, '') OR
				COALESCE(tgt.postal_code, -1) != COALESCE(src.postal_code, -1) OR
				COALESCE(tgt.city, '') != COALESCE(src.city, '') OR
				COALESCE(tgt.country, '') != COALESCE(src.country, '') OR
				COALESCE(tgt.score, -1) != COALESCE(src.score, -1);

		-- Retrieve total rows updated
		SET @rows_updated = @@ROWCOUNT;

		-- Load new records into target table		
		INSERT INTO gold.dim_customers
		(
			customer_id,
			first_name,
			last_name,
			postal_code,
			city,
			country,
			score
		)
		SELECT
			src.customer_id,
			src.first_name,
			src.last_name,
			src.postal_code,
			src.city,
			src.country,
			src.score
		FROM #gold_stg_dim_customers src
		LEFT JOIN gold.dim_customers tgt
		ON src.customer_id = tgt.customer_id
		WHERE tgt.customer_id IS NULL;

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
		INSERT INTO metadata.etl_step_run_error_log
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
