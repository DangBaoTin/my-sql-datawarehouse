# Building SQL Data Warehouse
## Introduction
This project showcases my practicing of data warehousing solution built with Supabase, designed to make analytics easier and more efficient. By combining scalable storage, fast data processing, and seamless querying, it helps businesses turn raw data into valuable insights. With a focus on consistency, security, and performance, this solution is perfect for batch analytics.

## Project Overview

This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.

## Building the Data Warehouse (Data Engineering)

### Objective
Develop a modern data warehouse using PostgreSQL (using Supabase Database) to consolidate sales data, enabling analytical reporting and informed decision-making.

### Specifications
- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.

## Data Architecture

The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:
![Data Architecture](docs/data_architecture.png)

1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.

## Future Development
Automated ETL scripts using Airflow.

## Installing and activate virtual environment
- Init virt env:
```bash
python3 -m venv venv
```
- Activate virtual environment:
```bash
source venv/bin/activate
```
- Install `requirements.txt` for the required libraries:
```bash
pip install -r requirements.txt
```
- To delete any unnecessary libraries:
```bash
pip uninstall <package-name>
```
- To deactivate virtual environment:
```bash
deactivate
rm -rf venv
```