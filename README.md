# OlistDataPipeline

## Olist E-Commerce Data Pipeline (PySpark + Delta Lake)

This project implements a full data engineering pipeline using PySpark and Delta Lake, based on the Olist Brazilian E-Commerce dataset. It follows the Medallion Architecture: **Bronze → Silver → Gold**.

##  Project Structure

![Example Image](https://github.com/SaraBallkoci/OlistDataPipeline/blob/main/project-root/Capture.PNG)


## The project does the following:

- **Bronze Layer**: Loads raw CSVs, parses timestamps, partitions orders by date and saves data to delta tables .
- **Silver Layer**: Joins and enriches data (products, sellers, customers, etc.), adds calculated columns, and cleans the dataset.
- **Gold Layer**: Creates analytical tables with:
  -  Cumulative sales per customer
  -  Rolling average delivery time per category
  -  KPI summary by seller, category, and state
- **SQL Reporting**: Runs queries to extract business insights.

## Dataset

[Kaggle - Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

## Requirements

- Python 3.10.18
- PySpark 4.0.0
- Delta Lake
- openjdk 17.0.15
- Jupyter Notebooks


