# PSX Automated Cloud ETL Pipeline

An end-to-end, automated data engineering project pipeline built on AWS and Databricks. This project automatically scrapes market data from the Pakistan Stock Exchange (PSX), ingests raw CSV files into Amazon S3, and incrementally processes the data into a Medallion Architecture (Delta table) using PySpark and Databricks.

![Image](https://github.com/ahmedmoizch/psx-de/blob/76b618ff026026eebefe76674d1041475bc58b41/setup/images/Gemini_Generated_Image_1pw9u21pw9u21pw9%20(1).jpg)

## Pipeline Execution
Data Ingestion (AWS Lambda): A lightweight function runs on a scheduled trigger 2 times a day to scrape live market table data from the PSX website and saves the output data csv directly into an Amazon S3 raw bucket with a timestamp CSV.

File Arrival Trigger: The presence of new CSV files in S3 dynamically triggers the processing job in databricks and run the notebooks.

Incremental ETL: Databricks picks up only the newly arrived CSV files using a python script, avoiding full-table re-computations.

Medallion Data Store (Delta Table): Data is sequentially transformed across Bronze, Silver, and Gold Delta tables to ensure data quality and auditability.

## Stack

Cloud Services: AWS (Lambda, S3, IAM)

Orchestration & Triggering: Databricks Jobs / Event Triggers in Lambda

Data Processing: PySpark

Storage & Table Format: Amazon S3, Delta Tables

Libraries: Python (requests, beautifulsoup4, pandas boto3)

![](https://github.com/ahmedmoizch/psx-de/blob/b25926ed05dedb40d7947430003fa34613defe81/setup/images/Capture.PNG)
