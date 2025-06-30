# Olist E-Commerce Data Pipeline (SQL Server)

This project implements a full data engineering pipeline in **Microsoft SQL Server** using the Olist Brazilian E-Commerce dataset. It follows the **Medallion Architecture**: Bronze → Silver → Gold, using T-SQL for ingestion, transformation, and analytics.

---

## Project Layers

### Bronze Layer: Raw Ingestion
- Raw CSVs imported using SSMS Import Wizard.
- `order_purchase_date` used for partitioning via `PARTITION FUNCTION` and `PARTITION SCHEME`.
- Data stored in `[bronze]` schema.
- Cleaned using `DELETE` and `ROW_NUMBER()` to remove nulls and duplicates.

### Silver Layer: Enriched Data
- Data joined from multiple bronze tables (orders, items, payments).
- Added columns:
  - `total_price = price + freight_value`
  - `profit_margin = price - freight_value`
  - `delivery_time_days = DATEDIFF(...)`
  - `payment_count = SUM(payment_installments)`
- Output stored in `[silver].orders_enriched`, partitioned by `order_purchase_date`.

### Gold Layer: Analytics & KPIs
- Window and aggregation functions used to create:
  - `cumulative_sales_customer`: running total of total_price per customer.
  - `rolling_avg_delivery_time_category`: 3-row rolling average by category.
  - `kpi_summary`: sales, delivery times, and order counts grouped by seller, product, and region.
- Results stored in `[gold]` schema.

---

## Reporting Queries

Sample insights delivered:
- Total sales per product category
- Average delivery time per seller
- Order counts per customer state

---

## Requirements

- Microsoft SQL Server (Express or Developer Edition)
- SQL Server Management Studio (SSMS)
- Olist Dataset from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

