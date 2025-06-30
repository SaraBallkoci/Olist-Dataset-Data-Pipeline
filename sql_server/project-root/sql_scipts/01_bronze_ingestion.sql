CREATE DATABASE OlistEcommerce;
GO

USE OlistEcommerce;
GO

CREATE SCHEMA bronze;
GO


CREATE PARTITION FUNCTION partition_order_date (DATE)
AS RANGE LEFT FOR VALUES (
    '2016-12-31', '2017-12-31', '2018-12-31'
);
GO

CREATE PARTITION SCHEME sch_order_date
AS PARTITION partition_order_date ALL TO ([PRIMARY]);
GO



CREATE TABLE bronze.olist_orders_partitioned (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
    order_purchase_date AS CAST(order_purchase_timestamp AS DATE) PERSISTED
)
ON sch_order_date(order_purchase_date);


INSERT INTO bronze.olist_orders_partitioned (
    order_id, customer_id, order_status, order_purchase_timestamp,
    order_approved_at, order_delivered_carrier_date,
    order_delivered_customer_date, order_estimated_delivery_date
)
SELECT 
    order_id, customer_id, order_status, order_purchase_timestamp,
    order_approved_at, order_delivered_carrier_date,
    order_delivered_customer_date, order_estimated_delivery_date
FROM bronze.olist_orders_dataset;





SELECT 
    order_id,
    customer_id,
    order_purchase_date,
    $PARTITION.partition_order_date(order_purchase_date) AS PartitionNumber
FROM bronze.olist_orders_partitioned
ORDER BY PartitionNumber, order_purchase_date;



