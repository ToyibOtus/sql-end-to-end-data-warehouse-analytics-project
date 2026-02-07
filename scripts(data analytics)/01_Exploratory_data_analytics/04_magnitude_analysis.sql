/*
========================================================================================
Magnitude Analysis
========================================================================================
Script Purpose:
	 This script performs magnitude analysis. It retrieves data that shows insight into
	 how key business metrics are distributed across relevant dimensions.
========================================================================================
*/
-- Which country brings in the highest revenue? Does high revenue mean high profit,
-- and how do other factors play a role in the observed correlation between sales & profit?
SELECT
	dc.country,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	COUNT(DISTINCT fo.product_key) AS total_products_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.gross_sales) AS total_gross_sales,
	SUM(fo.net_sales) AS total_net_sales,
	SUM(fo.profit) AS total_profit,
	ROUND(CAST(SUM(gross_sales) AS FLOAT)/SUM(quantity), 2) AS weighted_avg_price,
	ROUND(CAST(SUM(fo.profit)  AS FLOAT)/SUM(fo.gross_sales) * 100, 2) AS profit_margin_pct,
	ROUND(CAST(SUM(fo.discount * fo.gross_sales)  AS FLOAT)/SUM(fo.gross_sales) * 100, 2) AS weighted_discount_pct,
	AVG(DATEDIFF(day, order_date, shipping_date)) AS avg_days_to_ship
FROM gold.fact_orders fo
LEFT JOIN gold.dim_customers dc
ON fo.customer_key = dc.customer_key
GROUP BY dc.country
ORDER BY total_gross_sales DESC;


-- Within each country, which city generates the highest revenue, how does it compare to profit, 
-- and what other factors are responsible for the observed correlation btw sales & profit?
SELECT
	dc.country,
	dc.city,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	COUNT(DISTINCT fo.product_key) AS total_products_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.gross_sales) AS total_gross_sales,
	SUM(fo.net_sales) AS total_net_sales,
	SUM(fo.profit) AS total_profit,
	ROUND(CAST(SUM(gross_sales) AS FLOAT)/SUM(quantity), 2) AS weighted_avg_price,
	ROUND(CAST(SUM(fo.profit)  AS FLOAT)/SUM(fo.gross_sales) * 100, 2) AS profit_margin_pct,
	ROUND(CAST(SUM(fo.discount * fo.gross_sales)  AS FLOAT)/SUM(fo.gross_sales) * 100, 2) AS weighted_discount_pct,
	AVG(DATEDIFF(day, order_date, shipping_date)) AS avg_days_to_ship
FROM gold.fact_orders fo
LEFT JOIN gold.dim_customers dc
ON fo.customer_key = dc.customer_key
GROUP BY dc.country, dc.city
ORDER BY dc.country, total_gross_sales DESC;


-- Which category of product generates the highest revenue, how does it compare to profit, 
-- and what other factors are responsible for the observed correlation btw sales & profit?
SELECT
	dp.category,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.gross_sales) AS total_gross_sales,
	SUM(fo.net_sales) AS total_net_sales,
	SUM(fo.profit) AS total_profit,
	SUM(fo.quantity)/COUNT(DISTINCT fo.order_id) AS avg_quantity_per_order,
	ROUND(CAST(SUM(fo.net_sales) AS FLOAT)/COUNT(DISTINCT fo.order_id), 2) AS avg_net_sales_per_order,
	ROUND(CAST(SUM(gross_sales) AS FLOAT)/SUM(quantity), 2) AS weighted_avg_price,
	ROUND(CAST(SUM(fo.profit) AS FLOAT)/SUM(fo.quantity), 2) AS avg_profit_per_quantity,
	ROUND(CAST(SUM(fo.profit)  AS FLOAT)/SUM(fo.gross_sales) * 100, 2) AS profit_margin_pct,
	ROUND(CAST(SUM(fo.discount * fo.gross_sales)  AS FLOAT)/SUM(fo.gross_sales) * 100, 2) AS weighted_discount_pct,
	AVG(DATEDIFF(day, order_date, shipping_date)) AS avg_days_to_ship
FROM gold.fact_orders fo
LEFT JOIN gold.dim_products dp
ON fo.product_key = dp.product_key
GROUP BY dp.category
ORDER BY total_profit DESC;


-- Within each category, which subcategory generates the highest revenue, how does it compare to profit, 
-- and what other factors are responsible for the observed correlation btw sales & profit?
SELECT
	dp.category,
	dp.sub_category,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.gross_sales) AS total_gross_sales,
	SUM(fo.net_sales) AS total_net_sales,
	SUM(fo.profit) AS total_profit,
	SUM(fo.quantity)/COUNT(DISTINCT fo.order_id) AS avg_quantity_per_order,
	ROUND(CAST(SUM(fo.net_sales) AS FLOAT)/COUNT(DISTINCT fo.order_id), 2) AS avg_net_sales_per_order,
	ROUND(CAST(SUM(gross_sales) AS FLOAT)/SUM(quantity), 2) AS weighted_avg_price,
	ROUND(CAST(SUM(fo.profit) AS FLOAT)/SUM(fo.quantity), 2) AS avg_profit_per_quantity,
	ROUND(CAST(SUM(fo.profit)  AS FLOAT)/SUM(fo.gross_sales) * 100, 2) AS profit_margin_pct,
	ROUND(CAST(SUM(fo.discount * fo.gross_sales)  AS FLOAT)/SUM(fo.gross_sales) * 100, 2) AS weighted_discount_pct,
	AVG(DATEDIFF(day, order_date, shipping_date)) AS avg_days_to_ship
FROM gold.fact_orders fo
LEFT JOIN gold.dim_products dp
ON fo.product_key = dp.product_key
GROUP BY dp.category, dp.sub_category
ORDER BY dp.category, total_profit DESC;


-- Zooming into each category & subcategory, which product generates the highest revenue,
-- how does it compare to profit, and what other factors are responsible for the observed correlation btw sales & profit?
SELECT
	dp.category,
	dp.sub_category,
	dp.product_name,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.gross_sales) AS total_gross_sales,
	SUM(fo.net_sales) AS total_net_sales,
	SUM(fo.profit) AS total_profit,
	SUM(fo.quantity)/COUNT(DISTINCT fo.order_id) AS avg_quantity_per_order,
	ROUND(CAST(SUM(fo.net_sales) AS FLOAT)/COUNT(DISTINCT fo.order_id), 2) AS avg_net_sales_per_order,
	ROUND(CAST(SUM(gross_sales) AS FLOAT)/SUM(quantity), 2) AS weighted_avg_price,
	ROUND(CAST(SUM(fo.profit) AS FLOAT)/SUM(fo.quantity), 2) AS avg_profit_per_quantity,
	ROUND(CAST(SUM(fo.profit)  AS FLOAT)/NULLIF(SUM(fo.gross_sales), 0) * 100, 2) AS profit_margin_pct,
	ROUND(CAST(SUM(fo.discount * fo.gross_sales)  AS FLOAT)/SUM(fo.gross_sales) * 100, 2) AS weighted_discount_pct,
	AVG(DATEDIFF(day, order_date, shipping_date)) AS avg_days_to_ship
FROM gold.fact_orders fo
LEFT JOIN gold.dim_products dp
ON fo.product_key = dp.product_key
GROUP BY 
	dp.category, 
	dp.sub_category, 
	dp.product_name
ORDER BY dp.category, dp.sub_category, total_profit DESC;


-- What is our best year revenue-wise, how does it compare to profit,
-- and what other factors play a role in the observed correlation between revenue & profit?
SELECT
	YEAR(order_date) AS order_date_year,
	COUNT(DISTINCT customer_key) AS total_customers_ordered,
	COUNT(DISTINCT product_key) AS total_products_ordered,
	COUNT(DISTINCT order_id) AS total_orders,
	SUM(quantity) AS total_quantity,
	SUM(gross_sales) AS total_gross_sales,
	SUM(net_sales) AS total_net_sales,
	SUM(profit) AS total_profit,
	ROUND(CAST(SUM(gross_sales) AS FLOAT)/SUM(quantity), 2) AS weighted_avg_price,
	ROUND(CAST(SUM(profit) AS FLOAT)/SUM(gross_sales) * 100, 2) AS profit_margin_pct,
	ROUND(CAST(SUM(discount * gross_sales) AS FLOAT)/SUM(gross_sales) * 100, 2) AS weighted_discount_pct,
	AVG(DATEDIFF(day, order_date, shipping_date)) AS avg_days_to_ship
FROM gold.fact_orders
GROUP BY YEAR(order_date)
ORDER BY total_gross_sales DESC;


-- Zooming into each year, what is our best month revenue-wise?
SELECT
	YEAR(order_date) AS order_date_year,
	DATENAME(month, order_date) AS month_name,
	COUNT(DISTINCT customer_key) AS total_customers_ordered,
	COUNT(DISTINCT product_key) AS total_products_ordered,
	COUNT(DISTINCT order_id) AS total_orders,
	SUM(quantity) AS total_quantity,
	SUM(gross_sales) AS total_gross_sales,
	SUM(net_sales) AS total_net_sales,
	SUM(profit) AS total_profit,
	ROUND(CAST(SUM(gross_sales) AS FLOAT)/SUM(quantity), 2) AS weighted_avg_price,
	ROUND(CAST(SUM(profit) AS FLOAT)/SUM(gross_sales) * 100, 2) AS profit_margin_pct,
	ROUND(CAST(SUM(discount * gross_sales) AS FLOAT)/SUM(gross_sales) * 100, 2) AS weighted_discount_pct,
	AVG(DATEDIFF(day, order_date, shipping_date)) AS avg_days_to_ship
FROM gold.fact_orders
GROUP BY YEAR(order_date), DATENAME(month, order_date)
ORDER BY YEAR(order_date) DESC, total_gross_sales DESC;


-- What is the best year for each category profit-wise?
SELECT
	dp.category,
	YEAR(fo.order_date) AS order_date_year,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.gross_sales) AS total_gross_sales,
	SUM(fo.net_sales) AS total_net_sales,
	SUM(fo.profit) AS total_profit,
	ROUND(CAST(SUM(fo.gross_sales) AS FLOAT)/SUM(fo.quantity), 2) AS weighted_avg_price,
	ROUND(CAST(SUM(fo.profit) AS FLOAT)/SUM(fo.gross_sales) * 100, 2) AS profit_margin_pct,
	ROUND(CAST(SUM(fo.discount * fo.gross_sales) AS FLOAT)/SUM(fo.gross_sales) * 100, 2) AS weighted_discount_pct,
	AVG(DATEDIFF(day, fo.order_date, fo.shipping_date)) AS avg_days_to_ship
FROM gold.fact_orders fo
LEFT JOIN gold.dim_products dp
ON fo.product_key = dp.product_key
GROUP BY YEAR(fo.order_date), dp.category
ORDER BY dp.category, total_profit DESC; 


-- Is the profit generated by each subcategory of product increasing over the years?
SELECT
	dp.category,
	dp.sub_category,
	YEAR(fo.order_date) AS order_date_year,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.gross_sales) AS total_gross_sales,
	SUM(fo.net_sales) AS total_net_sales,
	SUM(fo.profit) AS total_profit,
	ROUND(CAST(SUM(fo.gross_sales) AS FLOAT)/SUM(fo.quantity), 2) AS weighted_avg_price,
	ROUND(CAST(SUM(fo.profit) AS FLOAT)/SUM(fo.gross_sales) * 100, 2) AS profit_margin_pct,
	ROUND(CAST(SUM(fo.discount * fo.gross_sales) AS FLOAT)/SUM(fo.gross_sales) * 100, 2) AS weighted_discount_pct,
	AVG(DATEDIFF(day, fo.order_date, fo.shipping_date)) AS avg_days_to_ship
FROM gold.fact_orders fo
LEFT JOIN gold.dim_products dp
ON fo.product_key = dp.product_key
GROUP BY YEAR(fo.order_date), dp.category, dp.sub_category
ORDER BY dp.category, dp.sub_category, total_profit DESC;



-- Within each category & subcategory, Is the profit generated by each product increasing over the years?
SELECT
	dp.category,
	dp.sub_category,
	dp.product_name,
	YEAR(fo.order_date) AS order_date_year,
	COUNT(DISTINCT fo.customer_key) AS total_customers_ordered,
	COUNT(DISTINCT fo.order_id) AS total_orders,
	SUM(fo.quantity) AS total_quantity,
	SUM(fo.gross_sales) AS total_gross_sales,
	SUM(fo.net_sales) AS total_net_sales,
	SUM(fo.profit) AS total_profit,
	ROUND(CAST(SUM(fo.gross_sales) AS FLOAT)/SUM(fo.quantity), 2) AS weighted_avg_price,
	ROUND(CAST(SUM(fo.profit) AS FLOAT)/NULLIF(SUM(fo.gross_sales), 0) * 100, 2) AS profit_margin_pct,
	ROUND(CAST(SUM(fo.discount * fo.gross_sales) AS FLOAT)/SUM(fo.gross_sales) * 100, 2) AS weighted_discount_pct,
	AVG(DATEDIFF(day, fo.order_date, fo.shipping_date)) AS avg_days_to_ship
FROM gold.fact_orders fo
LEFT JOIN gold.dim_products dp
ON fo.product_key = dp.product_key
GROUP BY 
	YEAR(fo.order_date), 
	dp.category, 
	dp.sub_category,
	dp.product_name
ORDER BY dp.category, dp.sub_category, dp.product_name, total_profit DESC;
