/*
=======================================================================================
Magnitude Analysis
=======================================================================================
Script Purpose:
	This script performs magnitude analysis. It retrieves insights into how important
	business metrics are distributed across relevant dimensions.
=======================================================================================
*/
-- Which country generates the highest profit, 
-- and what other underlying factors contribute to this performance?  
SELECT
	dc.country,
	COUNT(DISTINCT dc.customer_key) AS total_customers,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	(CAST(COUNT(DISTINCT fo.customer_key) AS FLOAT))/COUNT(DISTINCT dc.customer_key) * 100 AS percent_customers_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.sales) AS total_sales,
	SUM(fo.profit) AS total_profit,
	SUM(COALESCE(fo.quantity * fo.unit_price, 0))/NULLIF(SUM(COALESCE(fo.quantity, 0)), 0) AS weighted_avg_price,
	AVG(fo.discount) AS avg_discount,
	AVG(score) AS avg_score
FROM gold.dim_customers dc
LEFT JOIN gold.fact_orders fo
ON dc.customer_key = fo.customer_key
GROUP BY dc.country
ORDER BY total_profit DESC;

-- Within each country, which cities are the largest contributors to profit,
-- and how do customer volume, order volume, quantity, revenue, and pricing differ?
SELECT
	dc.country,
	dc.city,
	COUNT(DISTINCT dc.customer_key) AS total_customers,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	(CAST(COUNT(DISTINCT fo.customer_key) AS FLOAT))/COUNT(DISTINCT dc.customer_key) * 100 AS percent_customers_ordered, 
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.sales) AS total_sales,
	SUM(fo.profit) AS total_profit,
	SUM(COALESCE(fo.quantity * fo.unit_price, 0))/NULLIF(SUM(COALESCE(fo.quantity, 0)), 0) AS weighted_avg_price,
	AVG(fo.discount) AS avg_discount,
	AVG(score) AS avg_score
FROM gold.dim_customers dc
LEFT JOIN gold.fact_orders fo
ON dc.customer_key = fo.customer_key
GROUP BY dc.country, dc.city
ORDER BY country, total_profit DESC;

-- Which delivery area brings in the most profit,
-- and what other driving factors are at play?
SELECT
	dc.country,
	dc.city,
	dc.postal_code,
	COUNT(DISTINCT dc.customer_key) AS total_customers,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	(CAST(COUNT(DISTINCT fo.customer_key) AS FLOAT))/COUNT(DISTINCT dc.customer_key) * 100 AS percent_customers_ordered, 
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.sales) AS total_sales,
	SUM(fo.profit) AS total_profit,
	SUM(COALESCE(fo.quantity * fo.unit_price, 0))/NULLIF(SUM(COALESCE(fo.quantity, 0)), 0) AS weighted_avg_price,
	AVG(fo.discount) AS avg_discount,
	AVG(score) AS avg_score
FROM gold.dim_customers dc
LEFT JOIN gold.fact_orders fo
ON dc.customer_key = fo.customer_key
GROUP BY dc.country, dc.city, dc.postal_code
ORDER BY country, total_profit DESC;


-- Which of our customers has the highest total profit,
-- and what factors contribute to this performance?
SELECT
	dc.customer_key,
	CONCAT(dc.first_name, ' ', dc.last_name) AS customer_name,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.sales) AS total_sales,
	SUM(fo.profit) AS total_profit,
	SUM(fo.quantity * fo.unit_price)/SUM(fo.quantity) AS weighted_avg_price,
	AVG(fo.discount) AS avg_discount,
	AVG(score) AS avg_score
FROM gold.fact_orders fo 
LEFT JOIN gold.dim_customers dc
ON dc.customer_key = fo.customer_key
GROUP BY dc.customer_key, CONCAT(dc.first_name, ' ', dc.last_name)
ORDER BY total_profit DESC;

-- What category of product brings in the most profit,
-- and what factors contribute to this performance?
SELECT
	dp.category,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	COUNT(DISTINCT fo.customer_key) AS total_customers,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.sales) AS total_sales,
	SUM(fo.profit) AS total_profit,
	AVG(fo.unit_price) AS avg_price,
	SUM(fo.quantity * fo.unit_price)/SUM(fo.quantity) AS weighted_avg_price,
	AVG(fo.discount) AS avg_discount
FROM gold.fact_orders fo 
LEFT JOIN gold.dim_products dp
ON dp.product_key = fo.product_key
GROUP BY dp.category
ORDER BY total_profit DESC;

-- Within each category, which sub category of product has the highest total profit,
-- and what other metrics played a substantial role in this performance?
SELECT
	dp.category,
	dp.sub_category,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	COUNT(DISTINCT fo.customer_key) AS total_customers,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.sales) AS total_sales,
	SUM(fo.profit) AS total_profit,
	AVG(fo.unit_price) AS avg_price,
	SUM(fo.quantity * fo.unit_price)/SUM(fo.quantity) AS weighted_avg_price,
	AVG(fo.discount) AS avg_discount
FROM gold.fact_orders fo 
LEFT JOIN gold.dim_products dp
ON dp.product_key = fo.product_key
GROUP BY dp.category, dp.sub_category
ORDER BY category, total_profit DESC;

-- Within each category & sub category, which product brings in the most profit,
-- and how do metrics like order volume, total customers, revenue, pricing, and discount contribute to this?
SELECT
	dp.category,
	dp.sub_category,
	dp.product_name,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	COUNT(DISTINCT fo.customer_key) AS total_customers,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.sales) AS total_sales,
	SUM(fo.profit) AS total_profit,
	AVG(fo.unit_price) AS avg_price,
	AVG(fo.discount) AS avg_discount
FROM gold.dim_products dp
LEFT JOIN gold.fact_orders fo
ON dp.product_key = fo.product_key
GROUP BY dp.category, dp.sub_category, dp.product_name
ORDER BY dp.category, dp.sub_category, total_profit DESC;
