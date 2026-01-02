/*
================================================================================================
Drop Log tables
================================================================================================
Script Purpose:
	This script permanently deletes existing log tables.

Warning:
	Running this script deletes all data present in the log tables. Proceed with caution.
================================================================================================
*/
DROP TABLE IF EXISTS metadata.etl_data_quality_check;
DROP TABLE IF EXISTS metadata.etl_error_log;
DROP TABLE IF EXISTS metadata.etl_step_run;
DROP TABLE IF EXISTS metadata.etl_job_run;
