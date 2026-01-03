/*
==================================================================
DDL Script: Build Bronze Tables
==================================================================
Script Purpose:
	This script builds and designs the structure of bronze tables.
	Run this script to change the design of your bronze tables.
==================================================================
*/
USE SalesDatabase;
GO

-- Drop table [bronze.customers] if exists
IF OBJECT_ID('bronze.customers', 'U') IS NOT NULL
DROP TABLE bronze.customers;
GO

-- Create table [bronze.customers]
CREATE TABLE bronze.customers
(
	customer_id INT,
	first_name NVARCHAR(50),
	last_name NVARCHAR(50),
	postal_code INT,
	city NVARCHAR(50),
	country NVARCHAR(50),
	score INT
);

-- Drop table [bronze.orders] if exists
IF OBJECT_ID('bronze.orders', 'U') IS NOT NULL
DROP TABLE bronze.orders;
GO

-- Create table [bronze.orders]
CREATE TABLE bronze.orders
(
	order_id INT,
	customer_id INT,
	product_id INT,
	order_date NVARCHAR(50),
	shipping_date NVARCHAR(50),
	sales NVARCHAR(50),
	quantity INT,
	discount NVARCHAR(50),
	profit NVARCHAR(50),
	unit_price NVARCHAR(50)
);

-- Drop table [bronze.products] if exists
IF OBJECT_ID('bronze.products', 'U') IS NOT NULL
DROP TABLE bronze.products;
GO

-- Create table [bronze.products]
CREATE TABLE bronze.products
(
	product_id INT,
	product_name NVARCHAR(200),
	category NVARCHAR(50),
	sub_category NVARCHAR(50)
);
