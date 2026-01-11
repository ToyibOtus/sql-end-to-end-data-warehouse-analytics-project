/*
=======================================================================================================
Stored Procedure: Load Silver Table (Bronze -> Silver)
=======================================================================================================
Script Purpose:
	This script loads data from a bronze table into a corresponding silver table [customers].
	It also performs data transformaions where necessary. Additionaly, it loads log tables 
	with vital log details, essential not only for traceability and debugging, but for 
	monitoring the quality of the silver records.

Parameter: @job_run_id

Usage: EXEC silver.usp_load_silver_customers @job_run_id

Note:
	* Running this script independently demands that you assign a integer value to @job_run_id.
	* It is imperative that this value already exist in the log table [metadata.etl_job_run] due
	  to the foreign key constraint set on dependent tables.
	* To test the working condition of this script, check folder titled "test_run".
=======================================================================================================
*/
CREATE OR ALTER PROCEDURE silver.usp_load_silver_customers @job_run_id INT AS
BEGIN
	-- Abort on severe error
	SET XACT_ABORT ON;

	-- Declare and map values to variables where necessary
	DECLARE
	@step_run_id INT,
	@step_name NVARCHAR(50) = 'usp_load_silver_customers',
	@load_type NVARCHAR(50) = 'INCREMENTAL',
	@ingest_layer NVARCHAR(50) = 'SILVER',
	@ingest_table NVARCHAR(50) = 'customers',
	@start_time DATETIME,
	@end_time DATETIME,
	@step_duration INT,
	@step_status NVARCHAR(50) = 'RUNNING',
	@source_path NVARCHAR(50) = 'bronze.customers',
	@rows_in_source INT,
	@rows_to_load INT,
	@rows_updated INT = 0,
	@rows_inserted INT = 0,
	@rows_loaded INT = 0,
	@rows_diff INT;

	-- Capture start time
	SET @start_time = GETDATE();

	-- Retrieve rows in source tables
	SELECT @rows_in_source = COUNT(*) FROM bronze.customers;

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
	-- Retrievely recently generated step run id
	SET @step_run_id = SCOPE_IDENTITY();

	BEGIN TRY
		-- Delete staging table
		TRUNCATE TABLE silver_stg.customers;

		-- Transform retrieved records from source table
		WITH data_transformations AS
		(
		SELECT
			customer_id,
			TRIM(first_name) AS first_name,
			TRIM(last_name) AS last_name,
			postal_code,
			TRIM(city) AS city,
			CASE
				WHEN UPPER(TRIM(country)) = 'USA' THEN 'United States'
				ELSE TRIM(country)
				END AS country,
			score
		FROM bronze.customers
		)
		-- Generate metadata columns
		, metadata_columns AS
		(
		SELECT
			customer_id,
			first_name,
			last_name,
			postal_code,
			city,
			country,
			score,
			HASHBYTES('SHA2_256', CONCAT_WS('|',
			COALESCE(CAST(customer_id AS NVARCHAR(10)), ''),
			COALESCE(UPPER(first_name), ''),
			COALESCE(UPPER(last_name), ''),
			COALESCE(CAST(postal_code AS NVARCHAR(10)), ''),
			COALESCE(UPPER(city), ''),
			COALESCE(UPPER(country), ''),
			COALESCE(CAST(score AS NVARCHAR(10)), ''))) AS dwh_row_hash
		FROM data_transformations
		)
		-- Load staging table
		INSERT INTO silver_stg.customers
		(
			customer_id,
			first_name,
			last_name,
			postal_code,
			city,
			country,
			score,
			dwh_row_hash
		)
		SELECT
			mc.customer_id,
			mc.first_name,
			mc.last_name,
			mc.postal_code,
			mc.city,
			mc.country,
			mc.score,
			mc.dwh_row_hash
		FROM metadata_columns mc
		LEFT JOIN silver.customers sc
		ON mc.customer_id = sc.customer_id
		AND mc.dwh_row_hash = sc.dwh_row_hash
		WHERE sc.dwh_row_hash IS NULL;

		-- Retrieve rows to be loaded
		SELECT @rows_to_load = COUNT(*) FROM silver_stg.customers;

		-- Stop transaction if there are no new records from staging table
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

		-- Drop temp table if exists
		DROP TABLE IF EXISTS #dq_metrics;

		-- Create a temporary table and load into it important dq metrics
		SELECT
			COUNT(*) AS rows_checked,
			SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS rows_failed_pk_null,
			COUNT(customer_id) - COUNT(DISTINCT customer_id) AS rows_failed_pk_duplicate,
			SUM(CASE WHEN country NOT IN('United States', 'Germany', 'Italy', 'France') THEN 1 ELSE 0 END) AS rows_failed_invalid_country,
			SUM(CASE WHEN score IS NULL OR score < 0 THEN 1 ELSE 0 END) AS rows_failed_invalid_score
			INTO #dq_metrics
		FROM silver_stg.customers

		-- Load vital data quality checks info
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
		SELECT @job_run_id, @step_run_id, @ingest_layer, @ingest_table, 'PK_NULL', rows_checked, rows_failed_pk_null AS rows_failed,
		CASE WHEN rows_failed_pk_null > 0 THEN 'FAILED' ELSE 'PASSED' END AS dq_status FROM #dq_metrics
		UNION ALL
		SELECT @job_run_id, @step_run_id, @ingest_layer, @ingest_table, 'PK_DUPLICATE', rows_checked, rows_failed_pk_duplicate AS rows_failed,
		CASE WHEN rows_failed_pk_duplicate > 0 THEN 'FAILED' ELSE 'PASSED' END AS dq_status FROM #dq_metrics
		UNION ALL
		SELECT @job_run_id, @step_run_id, @ingest_layer, @ingest_table, 'INVALID_COUNTRY', rows_checked, rows_failed_invalid_country AS rows_failed,
		CASE WHEN rows_failed_invalid_country > 0 THEN 'FAILED' ELSE 'PASSED' END AS dq_status FROM #dq_metrics
		UNION ALL
		SELECT @job_run_id, @step_run_id, @ingest_layer, @ingest_table, 'INVALID_SCORE', rows_checked, rows_failed_invalid_score AS rows_failed,
		CASE WHEN rows_failed_invalid_score > 0 THEN 'FAILED' ELSE 'PASSED' END AS dq_status FROM #dq_metrics;

		-- Stop transaction if critical dq rule is violated
		IF EXISTS(SELECT 1 FROM metadata.etl_data_quality_check WHERE (step_run_id = @step_run_id) 
		AND (dq_check_name = 'PK_NULL' OR dq_check_name = 'PK_DUPLICATE') AND (rows_failed > 0)) THROW 50001, 
		'Critical Data Quality Rule Violated: Unable to load due to PRIMARY KEY containing either NULLS or DUPLICATES.', 1;

		-- Begin Transaction
		BEGIN TRAN;

		-- Update outdated records
		UPDATE tgt
			SET
				tgt.first_name = src.first_name,
				tgt.last_name = src.last_name,
				tgt.postal_code = src.postal_code,
				tgt.city = src.city,
				tgt.country = src.country,
				tgt.score = src.score,
				tgt.dwh_row_hash = src.dwh_row_hash
				FROM silver.customers tgt
				LEFT JOIN silver_stg.customers src
				ON src.customer_id = tgt.customer_id
			WHERE src.dwh_row_hash != tgt.dwh_row_hash;

		-- Retrieve rows updated
		SET @rows_updated = @@ROWCOUNT;

		-- Inserted new records
		INSERT INTO silver.customers
		(
			customer_id,
			first_name,
			last_name,
			postal_code,
			city,
			country,
			score,
			dwh_row_hash
		)
		SELECT
			src.customer_id,
			src.first_name,
			src.last_name,
			src.postal_code,
			src.city,
			src.country,
			src.score,
			src.dwh_row_hash
		FROM silver_stg.customers src
		LEFT JOIN silver.customers tgt
		ON src.customer_id = tgt.customer_id
		WHERE tgt.customer_id IS NULL;

		-- Retrieve rows inserted
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

		-- Rollback transaction on failure
		IF @@TRANCOUNT > 0 ROLLBACK TRAN;

		-- Map zero to rows updated & inserted if NULL
		IF @rows_updated IS NULL SET @rows_updated = 0;
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

		-- Load vital error details into log table
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
