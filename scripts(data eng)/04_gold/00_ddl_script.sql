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

-- Create table [gold.customers]
CREATE TABLE gold.customers
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

-- Create table [gold.products]
CREATE TABLE gold.products
(
	product_key INT IDENTITY(101, 1) PRIMARY KEY,
	product_id INT NOT NULL,
	product_name NVARCHAR(200) NOT NULL,
	category NVARCHAR(50) NOT NULL,
	sub_category NVARCHAR(50) NOT NULL
);

-- Create table [gold.orders]
CREATE TABLE gold.orders
(
	order_id INT NOT NULL,
	customer_key INT NOT NULL,
	product_key INT NOT NULL,
	order_date DATE NOT NULL,
	shipping_date DATE,
	sales DECIMAL(10, 3) NOT NULL,
	quantity INT NOT NULL,
	discount DECIMAL(10, 2) NOT NULL,
	profit DECIMAL(10, 3) NOT NULL,
	unit_price DECIMAL(10, 3) NOT NULL,
	CONSTRAINT uq_order_id_product_key UNIQUE (order_id, product_key),
	CONSTRAINT fk_gold_orders_customer_key FOREIGN KEY(customer_key) REFERENCES gold.customers(customer_key),
	CONSTRAINT fk_gold_orders_product_key FOREIGN KEY(product_key) REFERENCES gold.products(product_key)
);
