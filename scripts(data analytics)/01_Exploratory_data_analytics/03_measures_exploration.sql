/*
========================================================================
Measure Exploration
========================================================================
Script Purpose:
	This script explores key business metrics, giving an overview of 
	the business performance.
========================================================================
*/
-- What is the total revenue generated?
SELECT SUM(gross_sales) AS total_gross_sales FROM gold.fact_orders;

-- How much of this revenue goes to the business?
SELECT SUM(net_sales) AS total_net_sales FROM gold.fact_orders;

-- How many quantities of product were sold to generate such revenue?
SELECT SUM(quantity) AS total_quantity FROM gold.fact_orders;

-- How many orders generated this revenue?
SELECT COUNT(DISTINCT order_id) AS total_orders FROM gold.fact_orders;

-- What is the total profit generated from this revenue?
SELECT SUM(profit) AS total_profit FROM gold.fact_orders;

-- What is the overall profit to sales ratio?
SELECT ROUND(CAST(SUM(profit) AS FLOAT)/SUM(gross_sales) * 100, 2) AS profit_ratio FROM gold.fact_orders;

-- What is the realized average selling price?
SELECT ROUND(CAST(SUM(gross_sales) AS FLOAT)/SUM(quantity), 2) AS weighted_avg_price FROM gold.fact_orders;

-- What is the highest selling price?
SELECT MAX(unit_price) AS highest_selling_price FROM gold.fact_orders;

-- What is the lowest selling price?
SELECT MIN(unit_price) AS lowest_selling_price FROM gold.fact_orders;

-- What is the percentage weighted discount?
SELECT ROUND((CAST(SUM(gross_sales * discount) AS FLOAT)/SUM(gross_sales)) * 100, 2) AS percent_weighted_discount FROM gold.fact_orders;

-- What is the higest discount?
SELECT MAX(discount) AS highest_discount FROM gold.fact_orders;

-- What is the lowest discount?
SELECT MIN(discount)AS lowest_discount FROM gold.fact_orders;

-- What is the average days to ship?
SELECT AVG(DATEDIFF(day, order_date, shipping_date)) AS avg_days_to_ship FROM gold.fact_orders;

-- What is the highest number of days to ship?
SELECT MAX(DATEDIFF(day, order_date, shipping_date)) AS highest_days_to_ship FROM gold.fact_orders;

-- What is the lowest number of days to ships?
SELECT MIN(DATEDIFF(day, order_date, shipping_date)) AS lowest_days_to_ship FROM gold.fact_orders;

-- How many customers does the company have?
SELECT COUNT(customer_key) AS total_customers FROM gold.dim_customers;

-- How many of these customers have ordered?
SELECT COUNT(DISTINCT customer_key) AS total_customers_ordered FROM gold.fact_orders;

-- How many unique products does the business have?
SELECT COUNT(product_key) AS total_products FROM gold.dim_products;

-- How many of these products have been ordered?
SELECT COUNT(DISTINCT product_key) AS total_products_ordered FROM gold.fact_orders;

-- What is the percentage of products not ordered?
SELECT ROUND(((CAST(COUNT(DISTINCT dp.product_key) AS FLOAT) - COUNT(DISTINCT fo.product_key))/COUNT(DISTINCT dp.product_key)) * 100, 2)
AS percent_product_not_ordered FROM gold.dim_products dp LEFT JOIN gold.fact_orders fo  ON dp.product_key = fo.product_key;




-- Generate a report consolidating key business metrics
SELECT 'Total Gross Sales' AS measure_name, SUM(gross_sales) AS measure_value FROM gold.fact_orders
UNION ALL
SELECT 'Total Net Sales', SUM(net_sales) FROM gold.fact_orders
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_orders
UNION ALL
SELECT 'Total Orders', COUNT(DISTINCT order_id) FROM gold.fact_orders
UNION ALL
SELECT 'Total Profit', SUM(profit) FROM gold.fact_orders
UNION ALL
SELECT 'Profit Ratio (%)', ROUND(CAST(SUM(profit) AS FLOAT)/SUM(gross_sales) * 100, 2) FROM gold.fact_orders
UNION ALL
SELECT 'Weighted Average Price', ROUND(CAST(SUM(gross_sales) AS FLOAT)/SUM(quantity), 2) FROM gold.fact_orders
UNION ALL
SELECT 'Highest Selling Price', MAX(unit_price) FROM gold.fact_orders
UNION ALL
SELECT 'Lowest Selling Price', MIN(unit_price) FROM gold.fact_orders
UNION ALL
SELECT 'Weighted Discount (%)', ROUND((CAST(SUM(gross_sales * discount) AS FLOAT)/SUM(gross_sales)) * 100, 2) FROM gold.fact_orders
UNION ALL
SELECT 'Highest Discount (%)', MAX(discount) FROM gold.fact_orders
UNION ALL
SELECT 'Lowest Discount (%)', MIN(discount) FROM gold.fact_orders
UNION ALL
SELECT 'Avg Days to Ship', AVG(DATEDIFF(day, order_date, shipping_date)) FROM gold.fact_orders
UNION ALL
SELECT 'Highest Shipping Days', MAX(DATEDIFF(day, order_date, shipping_date)) FROM gold.fact_orders
UNION ALL
SELECT 'Lowest Shipping Days', MIN(DATEDIFF(day, order_date, shipping_date)) FROM gold.fact_orders
UNION ALL
SELECT 'Total Customers', COUNT(customer_key) FROM gold.dim_customers
UNION ALL
SELECT 'Total Customers Ordered', COUNT(DISTINCT customer_key) FROM gold.fact_orders
UNION ALL
SELECT 'Total Products', COUNT(product_key) FROM gold.dim_products
UNION ALL
SELECT 'Total Products Ordered', COUNT(DISTINCT product_key) FROM gold.fact_orders
UNION ALL
SELECT 'Products Not Ordered (%)', ROUND(((CAST(COUNT(DISTINCT dp.product_key) AS FLOAT) - COUNT(DISTINCT fo.product_key))
/COUNT(DISTINCT dp.product_key)) * 100, 2) FROM gold.dim_products dp LEFT JOIN gold.fact_orders fo  ON dp.product_key = fo.product_key;
