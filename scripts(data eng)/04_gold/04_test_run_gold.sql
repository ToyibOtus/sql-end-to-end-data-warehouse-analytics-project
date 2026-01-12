/*
========================================================================
Ad hoc: Test bronze procedures
========================================================================
Script Purpose:
	This procedure tests the working conditions of all gold procedures
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
	'GOLD_TEST',
	GETDATE(),
	'TEST_RUN'
);
-- Retrieve newly generated job_run_id from log table
SET @job_run_id = SCOPE_IDENTITY();

-- Load gold layer 

EXEC gold.usp_load_gold_dim_customers @job_run_id;
EXEC gold.usp_load_gold_dim_products @job_run_id;
EXEC gold.usp_load_gold_fact_orders @job_run_id; 
