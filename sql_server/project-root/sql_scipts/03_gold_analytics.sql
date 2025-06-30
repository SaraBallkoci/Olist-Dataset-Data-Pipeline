
CREATE SCHEMA gold;
GO

CREATE TABLE gold.cumulative_sales_customer (
    customer_id VARCHAR(50),
    order_id VARCHAR(50),
    order_purchase_timestamp DATETIME,
    running_total_sales DECIMAL(12,2),
    order_purchase_date DATE,
    order_year AS YEAR(order_purchase_timestamp) PERSISTED,
    order_month AS MONTH(order_purchase_timestamp) PERSISTED,
    order_day AS DAY(order_purchase_timestamp) PERSISTED
)
ON sch_order_date(order_purchase_date);


CREATE TABLE gold.rolling_avg_delivery_time_category (
    product_category_name VARCHAR(100),
    order_id VARCHAR(50),
    delivery_time_days INT,
    rolling_avg_delivery_time DECIMAL(10,2),
    order_purchase_timestamp DATETIME,
    order_purchase_date DATE,
    order_year AS YEAR(order_purchase_timestamp) PERSISTED,
    order_month AS MONTH(order_purchase_timestamp) PERSISTED,
    order_day AS DAY(order_purchase_timestamp) PERSISTED
)
ON sch_order_date(order_purchase_date);

CREATE TABLE gold.kpi_summary (
    product_category_name VARCHAR(100),
    seller_id VARCHAR(50),
    customer_state VARCHAR(2),
    total_sales DECIMAL(12,2),
    avg_delivery_time DECIMAL(5,2),
    order_count INT,
    order_purchase_date DATE,
    order_year INT,
    order_month INT,
    order_day INT
)
ON sch_order_date(order_purchase_date);

INSERT INTO gold.cumulative_sales_customer (
    customer_id, order_id, order_purchase_timestamp,
    running_total_sales, order_purchase_date
)
SELECT
    customer_id,
    order_id,
    order_purchase_timestamp,
    SUM(total_price) OVER (
        PARTITION BY customer_id ORDER BY order_purchase_timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_sales,
    CAST(order_purchase_timestamp AS DATE)
FROM silver.orders_enriched;


INSERT INTO gold.rolling_avg_delivery_time_category (
    product_category_name, order_id, delivery_time_days,
    rolling_avg_delivery_time, order_purchase_timestamp, order_purchase_date
)
SELECT 
    p.product_category_name,
    o.order_id,
    o.delivery_time_days,
    AVG(o.delivery_time_days) OVER (
        PARTITION BY p.product_category_name
        ORDER BY o.order_purchase_timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_delivery_time,
    o.order_purchase_timestamp,
    CAST(o.order_purchase_timestamp AS DATE)
FROM silver.orders_enriched o
JOIN bronze.olist_order_items_dataset i ON o.order_id = i.order_id
JOIN bronze.olist_products_dataset p ON i.product_id = p.product_id;



INSERT INTO gold.kpi_summary (
    product_category_name, seller_id, customer_state,
    total_sales, avg_delivery_time, order_count, order_purchase_date,
    order_year, order_month, order_day
)
SELECT
    p.product_category_name,
    s.seller_id,
    c.customer_state,
    SUM(o.total_price) AS total_sales,
    AVG(o.delivery_time_days) AS avg_delivery_time,
    COUNT(DISTINCT o.order_id) AS order_count,
    CAST(o.order_purchase_timestamp AS DATE),
    YEAR(o.order_purchase_timestamp),
    MONTH(o.order_purchase_timestamp),
    DAY(o.order_purchase_timestamp)
FROM silver.orders_enriched o
JOIN bronze.olist_customers_dataset c ON o.customer_id = c.customer_id
JOIN bronze.olist_order_items_dataset i ON o.order_id = i.order_id
JOIN bronze.olist_products_dataset p ON i.product_id = p.product_id
JOIN bronze.olist_sellers_dataset s ON i.seller_id = s.seller_id
GROUP BY 
    p.product_category_name,
    s.seller_id,
    c.customer_state,
    CAST(o.order_purchase_timestamp AS DATE),
    YEAR(o.order_purchase_timestamp),
    MONTH(o.order_purchase_timestamp),
    DAY(o.order_purchase_timestamp);
