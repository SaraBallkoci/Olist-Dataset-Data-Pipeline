CREATE SCHEMA silver;
GO


DELETE FROM bronze.olist_order_payments_dataset
WHERE 
    order_id IS NULL
    OR customer_id IS NULL
    OR order_purchase_timestamp IS NULL
    OR order_delivered_customer_date IS NULL;


DELETE FROM bronze.olist_customers_dataset
WHERE 
    customer_id IS NULL
    OR customer_unique_id IS NULL
    OR customer_state IS NULL;



DELETE FROM bronze.olist_products_dataset
WHERE 
    product_id IS NULL
    OR product_category_name IS NULL;


DELETE FROM bronze.olist_sellers_dataset
WHERE 
    seller_id IS NULL;


DELETE FROM bronze.olist_order_reviews_dataset
WHERE 
    review_id IS NULL
    OR order_id IS NULL;



DELETE FROM bronze.olist_geolocations_dataset
WHERE 
    geolocation_zip_code_prefix IS NULL;


DELETE FROM bronze.product_category_name_transaltion
WHERE 
    product_category IS NULL
    OR product_category_name_english IS NULL;


    
CREATE TABLE silver.orders_enriched (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_purchase_timestamp DATETIME,
    order_delivered_customer_date DATETIME,
    order_status VARCHAR(20),
    total_price DECIMAL(10, 2),
    delivery_time_days INT,
    payment_count INT,
    profit_margin DECIMAL(10, 2),
    order_purchase_date DATE,
    order_year AS YEAR(order_purchase_timestamp) PERSISTED,
    order_month AS MONTH(order_purchase_timestamp) PERSISTED,
    order_day AS DAY(order_purchase_timestamp) PERSISTED
)
ON sch_order_date(order_purchase_date);




    INSERT INTO silver.orders_enriched (
    order_id,
    customer_id,
    order_purchase_timestamp,
    order_delivered_customer_date,
    order_status,
    total_price,
    delivery_time_days,
    payment_count,
    profit_margin,
    order_purchase_date
)
    SELECT 
    o.order_id,
    o.customer_id,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_status,
    i.price + i.freight_value AS total_price,
    DATEDIFF(DAY, o.order_purchase_timestamp, o.order_delivered_customer_date) AS delivery_time_days,
    p.total_installments AS payment_count,
    i.price - i.freight_value AS profit_margin,
    CAST(o.order_purchase_timestamp AS DATE) AS order_purchase_date
FROM bronze.olist_orders_partitioned o
JOIN bronze.olist_order_items_dataset i ON o.order_id = i.order_id
JOIN (
    SELECT order_id, SUM(payment_installments) AS total_installments
    FROM bronze.olist_order_payments_dataset
    GROUP BY order_id
) p ON o.order_id = p.order_id;