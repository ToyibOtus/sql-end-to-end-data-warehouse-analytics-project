/*
========================================================================
Ad hoc: Test bronze procedures
========================================================================
Script Purpose:
	This procedure tests the working conditions of all bronze procedures
	in [SalesDatabase]. It firstly loads into metadata.etl_job_run, 
	satisfying the foreign key constraint set on dependent tables. 
========================================================================
*/
USE SalesDatabase;
GO

-- Create variable
DECLARE @job_run_id INT;

-- Load data into log table
INSERT INTO metadata.etl_job_run
(
	job_name,
	job_scope,
	start_time,
	job_status
)
VALUES
(
	'dev_test_bronze_run',
	'BRONZE_TEST',
	GETDATE(),
	'TEST_RUN'
);
-- Retrieve newly generated job_run_id from log table
SET @job_run_id = SCOPE_IDENTITY();

-- Load bronze layer
EXEC bronze.usp_load_bronze_customers @job_run_id;
EXEC bronze.usp_load_bronze_orders @job_run_id;
EXEC bronze.usp_load_bronze_products @job_run_id;
