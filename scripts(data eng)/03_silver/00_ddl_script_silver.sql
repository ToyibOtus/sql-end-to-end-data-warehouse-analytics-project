/*
==================================================================
DDL Script: Build Silver Tables
==================================================================
Script Purpose:
	This script builds and designs the structure of silver tables.
	Run this script to change the design of your silver tables.
==================================================================
*/
USE SalesDatabase;
GO

-- Drop table [silver.customers] if exists
IF OBJECT_ID('silver.customers', 'U') IS NOT NULL
DROP TABLE silver.customers;
GO

-- Create table [silver.customers]
CREATE TABLE silver.customers
(
	customer_id INT,
	first_name NVARCHAR(50),
	last_name NVARCHAR(50),
	postal_code INT,
	city NVARCHAR(50),
	country NVARCHAR(50),
	score INT,
	dwh_row_hash VARBINARY(64) NOT NULL,
	dwh_create_date DATETIME DEFAULT GETDATE() NOT NULL
);

-- Drop table [silver.orders] if exists
IF OBJECT_ID('silver.orders', 'U') IS NOT NULL
DROP TABLE silver.orders;
GO

-- Create table [silver.orders]
CREATE TABLE silver.orders
(
	order_id INT,
	customer_id INT,
	product_id INT,
	order_date DATE,
	shipping_date DATE,
	gross_sales DECIMAL(10, 3),
	net_sales DECIMAL(10, 3),
	quantity INT,
	discount DECIMAL(10, 3),
	profit DECIMAL(10, 3),
	unit_price DECIMAL(10, 3),
	dwh_row_hash VARBINARY(64) NOT NULL,
	dwh_create_date DATETIME DEFAULT GETDATE() NOT NULL
);

-- Drop table [silver.products] if exists
IF OBJECT_ID('silver.products', 'U') IS NOT NULL
DROP TABLE silver.products;
GO

-- Create table [silver.products]
CREATE TABLE silver.products
(
	product_id INT,
	product_name NVARCHAR(200),
	category NVARCHAR(50),
	sub_category NVARCHAR(50),
	dwh_row_hash VARBINARY(64) NOT NULL,
	dwh_create_date DATETIME DEFAULT GETDATE() NOT NULL
);
