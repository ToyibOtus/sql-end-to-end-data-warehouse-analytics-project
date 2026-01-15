/*
=======================================================================================
Date Exploration
=======================================================================================
Script Purpose:
	This script explores order dates in order to identify the scope of our datasets.
=======================================================================================
*/

SELECT 
	MIN(order_date) AS first_order_date,
	MAX(order_date) AS last_order_date,
	DATEDIFF(year, MIN(order_date), MAX(order_date)) AS order_date_scope_yr
FROM gold.fact_orders;
