# Olist E-Commerce Data Pipeline

This project implements a complete data engineering pipeline on the **Olist Brazilian E-Commerce dataset**, following the **Medallion Architecture**: Bronze → Silver → Gold.

Two implementation approaches are provided:

---

## Method 1: PySpark + Delta Lake

- **Bronze Layer**: Loads raw CSVs, parses timestamps, partitions by year/month/day, and stores Delta tables.
- **Silver Layer**: Joins and enriches data (products, sellers, customers, etc.), adds calculated columns, and cleans the dataset.
- **Gold Layer**: Creates analytical tables with:
  - Cumulative sales per customer
  - Rolling average delivery time per category
  - KPI summary by seller, category, and state
- **SQL Reporting**: Queries Delta tables to extract insights.

### Requirements
- Python 3.10.18
- PySpark 4.0.0
- Delta Lake
- openjdk 17.0.15
- Jupyter Notebooks

---

## Method 2: Microsoft SQL Server (T-SQL)

- **Bronze Layer**: Raw CSVs imported into SQL Server via SSMS or `BULK INSERT`. Partitioned tables created using `PARTITION FUNCTION` and `PARTITION SCHEME`.
- **Silver Layer**: Cleaned and enriched data using joins and computed columns (`total_price`, `delivery_time`, `profit_margin`, etc.).
- **Gold Layer**: Aggregated views using `SUM() OVER`, `AVG() OVER`, and `GROUP BY` to generate cumulative metrics and KPIs.
- **Reporting Queries**: Standard T-SQL used to extract total sales, average delivery times, and order counts by region.

### Requirements
- Microsoft SQL Server (Express or Developer Edition)
- SQL Server Management Studio (SSMS)

---

## Dataset

[Kaggle - Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

## Project Structure

