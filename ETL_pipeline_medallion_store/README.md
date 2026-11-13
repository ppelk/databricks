# 🧱Databricks ETL-pipeline for online store sales data

This Project demonstrates a data processing ETL-pipeline solution that adheres to the Medallion architecture.

## Project Requirements
### Building the pipeline
#### Objective
Develop a modern up-to-date data processing pipeline solution to consolidate sales data, executing data quality checks, data cleansing and transformations. Demonstrate PySpark writing skills, Databricks declarative pipelines (formerly known as Delta Live Tables),understanding of data manipulation conventions, best practices understanding and code documentation skills. 

### Specifications
#### Data Sources
Dirty Customer and Order Data for ETL Pipeline Testing from https://databasesample.com/. Data set was edited before this pipeline to remove randomly generated insufficient dates. This data set consists of generated online store sales data: 2 .csv files, one for customers and one for orders. 

#### Data Quality
Preprocess and clean the data according to the medallion architecture. In the gold layer, implement a data model according to star schema (dim_customer, dim_order). Ensure necessary data quality and adherence to business logic: completeness and uniqueness, business logic and referential integrity.

#### Data Manipulation
Refine values into end-user-friendly format. Derive new variables from existing ones.

#### Scope
Data loading, processing according to medallion architecture and transforming. 

### Results
#### ETL-Pipeline
Result of the project is an optimized ETL-pipeline built using Databricks declarative pipelines, functions and best practices. Data processing pipeline offers reliable data for further use in BI Analytics etc. 
#### Quality checks:
✅ Completeness\
✅ Uniqueness\
✅ Validity\
✅ Business Logics
#### Table Facts
##### 🥉Bronze Tables
- bronze.customers: 395 rows
- bronze.orders: 900 rows
##### 🥈Silver Tables
- silver.customers: 395 rows
- silver.orders: 900 rows
##### 🥇Gold Tables
- gold.dim_customer: 395 rows
- gold.fact_order: 900 rows
- gold.dim_date: 4334 rows (changes depending on running date)
