/*
================================================================================================
Build Log tables
================================================================================================
Script Purpose:
	This script builds key ETL log tables, which will be used to monitor and track every ETL
	step, enabling easy traceability & debugging.

	Run this script to change the structure and design of your ETL log tables.
================================================================================================
*/
-- Use relevant database
USE SalesDatabase;
GO

-- Create table [metadata.etl_job_run]
CREATE TABLE metadata.etl_job_run
(
	job_run_id INT IDENTITY(1001, 1) NOT NULL,
	job_name NVARCHAR(50) NOT NULL,
	start_time DATETIME NOT NULL,
	end_time DATETIME,
	job_status NVARCHAR(50) NOT NULL,
	CONSTRAINT pk_job_run_id PRIMARY KEY(job_run_id),
	CONSTRAINT chk_job_status CHECK(job_status IN('RUNNING', 'SUCCESSFUL', 'FAILED'))
);

-- Create table [metadata.etl_step_run]
CREATE TABLE metadata.etl_step_run
(
	step_run_id INT IDENTITY(1001, 1) NOT NULL,
	job_run_id INT NOT NULL,
	load_type NVARCHAR(50) NOT NULL,
	ingest_layer NVARCHAR(50) NOT NULL,
	ingest_table NVARCHAR(50) NOT NULL,
	step_name NVARCHAR(50) NOT NULL,
	start_time DATETIME NOT NULL,
	end_time DATETIME,
	step_status NVARCHAR(50) NOT NULL,
	source_path NVARCHAR(MAX) NOT NULL,
	rows_in_source INT,
	rows_loaded INT,
	CONSTRAINT pk_step_run_id PRIMARY KEY(step_run_id),
	CONSTRAINT fk_etl_step_run_job_run_id FOREIGN KEY(job_run_id) REFERENCES metadata.etl_job_run(job_run_id),
	CONSTRAINT chk_etl_step_run_step_status CHECK(step_status IN('RUNNING', 'NO OPERATION', 'SUCCESSFUL', 'FAILED'))
);

-- Create table [metadata.etl_error_log]
CREATE TABLE metadata.etl_error_log
(
	error_id INT IDENTITY(1001, 1) NOT NULL,
	job_run_id INT NOT NULL,
	step_run_id INT NOT NULL,
	error_time_stamp DATETIME DEFAULT GETDATE(),
	step_status NVARCHAR(50) NOT NULL,
	rows_in_source INT,
	rows_loaded INT,
	err_procedure NVARCHAR(50) NOT NULL,
	err_number INT NOT NULL,
	err_message NVARCHAR(MAX) NOT NULL,
	err_line INT NOT NULL,
	CONSTRAINT pk_error_id PRIMARY KEY(error_id),
	CONSTRAINT fk_etl_error_log_job_run_id FOREIGN KEY(job_run_id) REFERENCES metadata.etl_job_run(job_run_id),
	CONSTRAINT fk_etl_error_log_step_run_id FOREIGN KEY(step_run_id) REFERENCES metadata.etl_step_run(step_run_id),
	CONSTRAINT chk_etl_error_log_step_status CHECK(step_status IN('FAILED'))
);

-- Create table [metadata.etl_data_quality_check]
CREATE TABLE metadata.etl_data_quality_check
(
	dq_run_id INT IDENTITY(1001, 1) NOT NULL,
	job_run_id INT NOT NULL,
	step_run_id INT NOT NULL,
	dq_layer_name NVARCHAR(50) NOT NULL,
	dq_table_name NVARCHAR(50) NOT NULL,
	dq_check_name NVARCHAR(50) NOT NULL,
	rows_checked INT NOT NULL,
	rows_failed INT NOT NULL,
	CONSTRAINT pk_dq_run_id PRIMARY KEY(dq_run_id),
	CONSTRAINT fk_etl_data_quality_check_job_run_id FOREIGN KEY(job_run_id) REFERENCES metadata.etl_job_run(job_run_id),
	CONSTRAINT fk_etl_data_quality_check_step_run_id FOREIGN KEY(step_run_id) REFERENCES metadata.etl_step_run(step_run_id),
);
