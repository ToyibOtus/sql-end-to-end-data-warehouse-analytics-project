/*
========================================================================================
Magnitude Analysis
========================================================================================
Script Purpose:
	 This script performs magnitude analysis. It retrieves data that shows insight into
	 how key business metrics are distributed across relevant dimensions.
========================================================================================
*/
-- Which country generates the highest revenue
-- and what other metrics are at play?
SELECT
	dc.country,
	COUNT(DISTINCT dc.customer_key) AS total_customers,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	ROUND(CAST(COUNT(DISTINCT fo.customer_key) AS FLOAT)/COUNT(DISTINCT dc.customer_key) * 100, 2) AS percent_cust_ordered,
	(SELECT COUNT(product_key) FROM gold.dim_products) AS total_products,
	COUNT(DISTINCT fo.product_key) AS total_product_ordered,
	ROUND(CAST(COUNT(DISTINCT fo.product_key) AS FLOAT)/(SELECT COUNT(product_key) FROM gold.dim_products) * 100, 2) AS percent_product_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.sales) AS total_sales,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.profit) AS total_profit,
	ROUND(CAST(SUM(fo.unit_price * fo.quantity) AS FLOAT)/NULLIF(SUM(COALESCE(fo.quantity, 0)), 0), 2) AS weighted_avg_price,
	ROUND(CAST(AVG(fo.discount) AS FLOAT), 2) AS avg_discount,
	MAX(fo.discount) AS max_discount,
	AVG(DATEDIFF(day, order_date, shipping_date)) AS avg_days_to_ship
FROM gold.dim_customers dc
LEFT JOIN gold.fact_orders fo
ON dc.customer_key = fo.customer_key
GROUP BY dc.country
ORDER BY total_sales DESC;

-- Within each country, which city brings in the most revenue
-- and what other driving factors explain this performance?
SELECT
	dc.country,
	dc.city,
	COUNT(DISTINCT dc.customer_key) AS total_customers,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	ROUND(CAST(COUNT(DISTINCT fo.customer_key) AS FLOAT)/COUNT(DISTINCT dc.customer_key) * 100, 2) AS percent_cust_ordered,
	(SELECT COUNT(product_key) FROM gold.dim_products) AS total_products,
	COUNT(DISTINCT fo.product_key) AS total_product_ordered,
	ROUND(CAST(COUNT(DISTINCT fo.product_key) AS FLOAT)/(SELECT COUNT(product_key) FROM gold.dim_products) * 100, 2) AS percent_product_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.sales) AS total_sales,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.profit) AS total_profit,
	ROUND(CAST(SUM(fo.unit_price * fo.quantity) AS FLOAT)/NULLIF(SUM(COALESCE(fo.quantity, 0)), 0), 2) AS weighted_avg_price,
	ROUND(CAST(AVG(fo.discount) AS FLOAT), 2) AS avg_discount,
	MAX(fo.discount) AS max_discount,
	AVG(DATEDIFF(day, order_date, shipping_date)) AS avg_days_to_ship
FROM gold.dim_customers dc
LEFT JOIN gold.fact_orders fo
ON dc.customer_key = fo.customer_key
GROUP BY dc.country, dc.city
ORDER BY dc.country, total_sales DESC;

-- Zooming into each country & city, which delivery area (postal code) generates the highest revenue
-- and how do other metrics drive this?
SELECT
	dc.country,
	dc.city,
	dc.postal_code,
	COUNT(DISTINCT dc.customer_key) AS total_customers,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	ROUND(CAST(COUNT(DISTINCT fo.customer_key) AS FLOAT)/COUNT(DISTINCT dc.customer_key) * 100, 2) AS percent_cust_ordered,
	(SELECT COUNT(product_key) FROM gold.dim_products) AS total_products,
	COUNT(DISTINCT fo.product_key) AS total_product_ordered,
	ROUND(CAST(COUNT(DISTINCT fo.product_key) AS FLOAT)/(SELECT COUNT(product_key) FROM gold.dim_products) * 100, 2) AS percent_product_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.sales) AS total_sales,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.profit) AS total_profit,
	ROUND(CAST(SUM(fo.unit_price * fo.quantity) AS FLOAT)/NULLIF(SUM(COALESCE(fo.quantity, 0)), 0), 2) AS weighted_avg_price,
	ROUND(CAST(AVG(fo.discount) AS FLOAT), 2) AS avg_discount,
	MAX(fo.discount) AS max_discount,
	AVG(DATEDIFF(day, order_date, shipping_date)) AS avg_days_to_ship
FROM gold.dim_customers dc
LEFT JOIN gold.fact_orders fo
ON dc.customer_key = fo.customer_key
GROUP BY dc.country, dc.city, dc.postal_code
ORDER BY dc.country, dc.city, total_sales DESC;

-- Which category of products generates the most revenue
-- and what other business metrics drive this?
SELECT
	dp.category,
	COUNT(DISTINCT dp.product_key) AS total_products,
	COUNT(DISTINCT fo.product_key) AS total_products_ordered,
	ROUND(CAST(COUNT(DISTINCT fo.product_key) AS FLOAT)/COUNT(DISTINCT dp.product_key) * 100, 2) AS percent_products_ordered,
	(SELECT COUNT(customer_key) FROM gold.dim_customers) AS total_customers,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	ROUND(CAST(COUNT(DISTINCT fo.customer_key) AS FLOAT)/(SELECT COUNT(customer_key) FROM gold.dim_customers) * 100, 2) AS percent_customers_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.sales) AS total_sales,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.profit) AS total_profit,
	ROUND(CAST(AVG(fo.unit_price) AS FLOAT), 2) AS avg_selling_price,
	ROUND(CAST(SUM(fo.unit_price * fo.quantity) AS FLOAT)/NULLIF(SUM(COALESCE(fo.quantity, 0)), 0), 2) AS weighted_avg_price,
	ROUND(CAST(AVG(fo.discount) AS FLOAT), 2) AS avg_discount,
	MAX(fo.discount) AS max_discount,
	AVG(DATEDIFF(day, order_date, shipping_date)) AS avg_days_to_ship
FROM gold.dim_products dp
LEFT JOIN gold.fact_orders fo
ON dp.product_key = fo.product_key
GROUP BY dp.category
ORDER BY total_sales DESC;

-- Within each category of products, which sub category brings in the most revenue
-- and what other factors drive this performance?
SELECT
	dp.category,
	dp.sub_category,
	COUNT(DISTINCT dp.product_key) AS total_products,
	COUNT(DISTINCT fo.product_key) AS total_products_ordered,
	ROUND(CAST(COUNT(DISTINCT fo.product_key) AS FLOAT)/COUNT(DISTINCT dp.product_key) * 100, 2) AS percent_products_ordered,
	(SELECT COUNT(customer_key) FROM gold.dim_customers) AS total_customers,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	ROUND(CAST(COUNT(DISTINCT fo.customer_key) AS FLOAT)/(SELECT COUNT(customer_key) FROM gold.dim_customers) * 100, 2) AS percent_customers_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.sales) AS total_sales,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.profit) AS total_profit,
	ROUND(CAST(AVG(fo.unit_price) AS FLOAT), 2) AS avg_selling_price,
	ROUND(CAST(SUM(fo.unit_price * fo.quantity) AS FLOAT)/NULLIF(SUM(COALESCE(fo.quantity, 0)), 0), 2) AS weighted_avg_price,
	ROUND(CAST(AVG(fo.discount) AS FLOAT), 2) AS avg_discount,
	MAX(fo.discount) AS max_discount,
	AVG(DATEDIFF(day, order_date, shipping_date)) AS avg_days_to_ship
FROM gold.dim_products dp
LEFT JOIN gold.fact_orders fo
ON dp.product_key = fo.product_key
GROUP BY dp.category, dp.sub_category
ORDER BY dp.category, total_sales DESC;

-- Within each category & sub category, which product generates the most revenue
-- and what other driving factors are at play?
SELECT
	dp.category,
	dp.sub_category,
	dp.product_name,
	(SELECT COUNT(customer_key) FROM gold.dim_customers) AS total_customers,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	ROUND(CAST(COUNT(DISTINCT fo.customer_key) AS FLOAT)/(SELECT COUNT(customer_key) FROM gold.dim_customers) * 100, 2) AS percent_customers_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.sales) AS total_sales,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.profit) AS total_profit,
	ROUND(CAST(AVG(fo.unit_price) AS FLOAT), 2) AS avg_selling_price,
	ROUND(CAST(SUM(fo.unit_price * fo.quantity) AS FLOAT)/NULLIF(SUM(COALESCE(fo.quantity, 0)), 0), 2) AS weighted_avg_price,
	ROUND(CAST(AVG(fo.discount) AS FLOAT), 2) AS avg_discount,
	MAX(fo.discount) AS max_discount,
	AVG(DATEDIFF(day, order_date, shipping_date)) AS avg_days_to_ship
FROM gold.dim_products dp
INNER JOIN gold.fact_orders fo
ON dp.product_key = fo.product_key
GROUP BY dp.category, dp.sub_category, dp.product_name
ORDER BY dp.category, dp.sub_category, total_sales DESC;

-- Irrespective of category & sub category, what are the most revenue-generating products, and why?
SELECT
	dp.product_name,
	(SELECT COUNT(customer_key) FROM gold.dim_customers) AS total_customers,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	ROUND(CAST(COUNT(DISTINCT fo.customer_key) AS FLOAT)/(SELECT COUNT(customer_key) FROM gold.dim_customers) * 100, 2) AS percent_customers_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.sales) AS total_sales,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.profit) AS total_profit,
	ROUND(CAST(AVG(fo.unit_price) AS FLOAT), 2) AS avg_selling_price,
	ROUND(CAST(SUM(fo.unit_price * fo.quantity) AS FLOAT)/NULLIF(SUM(COALESCE(fo.quantity, 0)), 0), 2) AS weighted_avg_price,
	ROUND(CAST(AVG(fo.discount) AS FLOAT), 2) AS avg_discount,
	MAX(fo.discount) AS max_discount,
	AVG(DATEDIFF(day, order_date, shipping_date)) AS avg_days_to_ship
FROM gold.dim_products dp
INNER JOIN gold.fact_orders fo
ON dp.product_key = fo.product_key
GROUP BY dp.product_name
ORDER BY total_sales DESC;

-- Within each country, which category of product brings in most of the revenue, and why?
SELECT
	dc.country,
	dp.category,
	MAX(tcc.total_customers_country) AS total_customers_country,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	ROUND(CAST(COUNT(DISTINCT fo.customer_key) AS FLOAT)/MAX(tcc.total_customers_country), 2) * 100 AS percent_customers_ordered,
	MAX(tpc.total_products_category) AS total_products_category,
	COUNT(DISTINCT fo.product_key) AS total_products_ordered,
	ROUND(CAST(COUNT(DISTINCT fo.product_key) AS FLOAT)/MAX(tpc.total_products_category) * 100, 2) AS percent_products_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.sales) AS total_sales,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.profit) AS total_profit,
	ROUND(CAST(SUM(fo.unit_price * fo.quantity) AS FLOAT)/NULLIF(SUM(COALESCE(fo.quantity, 0)), 0), 2) AS weighted_avg_price,
	ROUND(CAST(AVG(fo.discount) AS FLOAT), 2) AS avg_discount,
	MAX(fo.discount) AS max_discount,
	AVG(DATEDIFF(day, order_date, shipping_date)) AS avg_days_to_ship
FROM gold.dim_customers dc
INNER JOIN 
(
	SELECT 
		country, 
		COUNT(customer_key) AS total_customers_country 
	FROM gold.dim_customers GROUP BY country
)tcc
ON dc.country = tcc.country
INNER JOIN gold.fact_orders fo
ON dc.customer_key = fo.customer_key
INNER JOIN gold.dim_products dp
ON fo.product_key = dp.product_key
INNER JOIN
(
	SELECT 
		category, 
		COUNT(product_key) AS total_products_category 
	FROM gold.dim_products 
	GROUP BY category
)tpc
ON dp.category = tpc.category
GROUP BY dc.country, dp.category
ORDER BY country, total_sales DESC;

-- What is currently our best year revenue-wise?
SELECT
	YEAR(fo.order_date) AS order_date_year,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	COUNT(DISTINCT fo.product_key) AS total_products_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.sales) AS total_sales,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.profit) AS total_profit,
	ROUND(CAST(SUM(fo.unit_price * fo.quantity) AS FLOAT)/NULLIF(SUM(COALESCE(fo.quantity, 0)), 0), 2) AS weighted_avg_price,
	ROUND(CAST(AVG(fo.discount) AS FLOAT), 2) AS avg_discount,
	MAX(fo.discount) AS max_discount,
	AVG(DATEDIFF(day, order_date, shipping_date)) AS avg_days_to_ship
FROM gold.dim_customers dc
LEFT JOIN gold.fact_orders fo
ON dc.customer_key = fo.customer_key
GROUP BY YEAR(order_date)
ORDER BY total_sales DESC;

-- Within each year, which month brings in the most revenue?
SELECT
	YEAR(fo.order_date) AS order_date_year,
	DATENAME(month, fo.order_date) AS month_name,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	COUNT(DISTINCT fo.product_key) AS total_products_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.sales) AS total_sales,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.profit) AS total_profit,
	ROUND(CAST(SUM(fo.unit_price * fo.quantity) AS FLOAT)/NULLIF(SUM(COALESCE(fo.quantity, 0)), 0), 2) AS weighted_avg_price,
	ROUND(CAST(AVG(fo.discount) AS FLOAT), 2) AS avg_discount,
	MAX(fo.discount) AS max_discount,
	AVG(DATEDIFF(day, order_date, shipping_date)) AS avg_days_to_ship
FROM gold.dim_customers dc
LEFT JOIN gold.fact_orders fo
ON dc.customer_key = fo.customer_key
GROUP BY YEAR(order_date), DATENAME(month, order_date)
ORDER BY order_date_year DESC, total_sales DESC;


-- Is our best year consistent across all countries?
SELECT
	dc.country,
	YEAR(fo.order_date) AS order_date_year,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	COUNT(DISTINCT fo.product_key) AS total_products_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.sales) AS total_sales,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.profit) AS total_profit,
	ROUND(CAST(SUM(fo.unit_price * fo.quantity) AS FLOAT)/NULLIF(SUM(COALESCE(fo.quantity, 0)), 0), 2) AS weighted_avg_price,
	ROUND(CAST(AVG(fo.discount) AS FLOAT), 2) AS avg_discount,
	MAX(fo.discount) AS max_discount,
	AVG(DATEDIFF(day, order_date, shipping_date)) AS avg_days_to_ship
FROM gold.dim_customers dc
INNER JOIN gold.fact_orders fo
ON dc.customer_key = fo.customer_key
GROUP BY dc.country, YEAR(fo.order_date)
ORDER BY country, total_sales DESC;

-- Which our customers generate the highest revenue?
SELECT
	customer_key,
	(SELECT COUNT(product_key) FROM gold.dim_products) AS total_products,
	COUNT(DISTINCT product_key) AS total_product_ordered,
	ROUND(CAST(COUNT(DISTINCT product_key) AS FLOAT)/(SELECT COUNT(product_key) FROM gold.dim_products) * 100, 2) AS percent_product_ordered,
	COUNT(DISTINCT order_id) AS total_orders,
	SUM(sales) AS total_sales,
	SUM(quantity) AS total_quantity,
	SUM(profit) AS total_profit,
	ROUND(CAST(SUM(unit_price * quantity) AS FLOAT)/NULLIF(SUM(COALESCE(quantity, 0)), 0), 2) AS weighted_avg_price,
	ROUND(CAST(AVG(discount) AS FLOAT), 2) AS avg_discount,
	MAX(discount) AS max_discount
FROM gold.fact_orders
GROUP BY customer_key
ORDER BY total_sales DESC;
