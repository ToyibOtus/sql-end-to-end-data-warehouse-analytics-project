/*
=============================================================================================
Stored Procedure: Load Bronze Table (Source -> Bronze)
=============================================================================================
Script Purpose:
	This script loads data from a CSV file into a bronze table [customers]. Additionaly, 
	it loads log tables with vital log details, which is essential for easy traceability 
	and debugging.

Parameter: @job_run_id

Usage: EXEC bronze.usp_load_bronze_customers @job_run_id

Note:
	* Running this script independently demands that you assign a integer value to @job_run_id.
	* It is imperative that this value already exist in the log table [metadata.etl_job_run] due
	  to the foreign key constraint set on log table [metadata.etl_step_run].
	* To test the working condition of this script, check folder titled "test_run".
=============================================================================================
*/
CREATE OR ALTER PROCEDURE bronze.usp_load_bronze_customers @job_run_id INT AS
BEGIN
	-- Abort transaction on severe error
	SET XACT_ABORT ON;

	-- Create and map values to variables where necessary
	DECLARE
	@step_run_id INT,
	@step_name NVARCHAR(50) = 'usp_load_bronze_customers',
	@load_type NVARCHAR(50) = 'FULL',
	@ingest_layer NVARCHAR(50) = 'BRONZE',
	@ingest_table NVARCHAR(50) = 'customers',
	@start_time DATETIME,
	@end_time DATETIME,
	@step_duration INT,
	@step_status NVARCHAR(50) = 'RUNNING',
	@source_path NVARCHAR(260) = 'C:\Users\PC\Documents\big-dataset\customers.csv',
	@rows_updated INT = 0,
	@rows_inserted INT = 0,
	@rows_loaded INT = 0,
	@sql NVARCHAR(MAX);

	-- Capture start time
	SET @start_time = GETDATE();

	-- Load into log table before transaction
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
		@rows_updated,
		@rows_inserted,
		@rows_loaded
	);
	-- Retrieve recently generated step_run_id from [metadata.etl_step_run]
	SET @step_run_id = SCOPE_IDENTITY();

	BEGIN TRY
		-- Begin Transaction
		BEGIN TRAN;

		-- Delete data from table
		TRUNCATE TABLE bronze.customers;

		-- Load data into table
		SET @sql = 'BULK INSERT bronze.customers FROM ''' + @source_path + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '';'', TABLOCK);';
		EXEC (@sql);

		-- Finalize transaction on success
		COMMIT TRAN;

		-- Map values to variables in try block
		SET @end_time = GETDATE();
		SET @step_duration = DATEDIFF(second, @start_time, @end_time);
		SET @step_status = 'SUCCESSFUL';
		SELECT @rows_inserted = COUNT(*) FROM bronze.customers;
		SET @rows_loaded = @rows_updated + @rows_inserted;

		-- Update log table on successful transaction
		UPDATE metadata.etl_step_run
			SET
				end_time = @end_time,
				step_duration_seconds = @step_duration,
				step_status = @step_status,
				rows_inserted = @rows_inserted,
				rows_loaded = @rows_loaded
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

		-- Calculate rows loaded
		SET @rows_loaded = @rows_updated + @rows_inserted;

		-- Update log table on failed transaction
		UPDATE metadata.etl_step_run
			SET
				end_time = @end_time,
				step_duration_seconds = @step_duration,
				step_status = @step_status,
				rows_inserted = @rows_inserted,
				rows_loaded = @rows_loaded
			WHERE step_run_id = @step_run_id;

		-- Load log table with error details
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
