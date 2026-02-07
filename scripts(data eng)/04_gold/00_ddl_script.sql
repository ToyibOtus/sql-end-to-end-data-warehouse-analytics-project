/*
==================================================================
DDL Script: Build Gold Tables
==================================================================
Script Purpose:
	This script builds and designs the structure of gold tables.
	Run this script to change the design of your gold tables.
==================================================================
*/
USE SalesDatabase;
GO

DROP TABLE IF EXISTS gold.fact_orders;
DROP TABLE IF EXISTS gold.dim_customers;
DROP TABLE IF EXISTS gold.dim_products;

-- Drop table [gold.dim_customers] if exists
IF OBJECT_ID('gold.dim_customers', 'U') IS NOT NULL
DROP TABLE gold.dim_customers;
GO

-- Create table [gold.dim_customers]
CREATE TABLE gold.dim_customers
(
	customer_key INT IDENTITY(101, 1) PRIMARY KEY,
	customer_id INT NOT NULL,
	first_name NVARCHAR(50) NOT NULL,
	last_name NVARCHAR(50) NOT NULL,
	postal_code INT,
	city NVARCHAR(50),
	country NVARCHAR(50),
	score INT
);

-- Drop table [gold.dim_products] if exists
IF OBJECT_ID('gold.dim_products', 'U') IS NOT NULL
DROP TABLE gold.dim_products;
GO

-- Create table [gold.dim_products]
CREATE TABLE gold.dim_products
(
	product_key INT IDENTITY(101, 1) PRIMARY KEY,
	product_id INT NOT NULL,
	product_name NVARCHAR(200) NOT NULL,
	category NVARCHAR(50) NOT NULL,
	sub_category NVARCHAR(50) NOT NULL
);

-- Drop table [gold.fact_orders] if exists
IF OBJECT_ID('gold.fact_orders', 'U') IS NOT NULL
DROP TABLE gold.fact_orders;
GO

-- Create table [gold.fact_orders]
CREATE TABLE gold.fact_orders
(
	order_id INT NOT NULL,
	customer_key INT NOT NULL,
	product_key INT NOT NULL,
	order_date DATE NOT NULL,
	shipping_date DATE,
	gross_sales DECIMAL(10, 2) NOT NULL,
	net_sales DECIMAL(10, 2) NOT NULL,
	quantity INT NOT NULL,
	discount DECIMAL(10, 2) NOT NULL,
	profit DECIMAL(10, 2) NOT NULL,
	unit_price DECIMAL(10, 2) NOT NULL,
	CONSTRAINT uq_order_id_product_key UNIQUE (order_id, product_key),
	CONSTRAINT fk_gold_fact_orders_customer_key FOREIGN KEY(customer_key) REFERENCES gold.dim_customers(customer_key),
	CONSTRAINT fk_gold_fact_orders_product_key FOREIGN KEY(product_key) REFERENCES gold.dim_products(product_key)
);
