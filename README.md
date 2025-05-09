# Luxury Watch Market Financial Metrics Data Pipeline

<img src="diagrams\website_screenshot_rolex.jpg" alt="Website Screenshot 1">

<img src="diagrams\website_screeenshot_rolex_2.jpg" alt="Website Screenshot 12">

## 1. Introduction
This document provides a comprehensive overview of the Luxury Watch Market Financial Metrics Calculations Data Pipeline, an Airflow-based system designed to process, analyze, and derive market insights from watch sales listing data across multiple e-commerce platforms and forums.

### 1.1 Purpose
The pipeline transforms raw watch listing data into actionable market intelligence, including current market prices, price trends, market composition, and volatility metrics for luxury watches at various levels of aggregation (reference number, specific model, parent model, and brand).

### 1.2 System Context
This pipeline operates as the second stage in a two-stage data processing workflow:

**Stage 1:** External scraping pipeline (outside scope) that collects raw listing data from 55+ websites <br>
**Stage 2:** This data processing pipeline that transforms raw data into market intelligence

## 2. Architecture Overview

### 2.1 Infrastructure: AWS-Docker-Airflow 
The pipeline runs on an AWS EC2 instance that is activated by the completion of the external scraping pipeline. The system utilizes Docker containers to host Airflow components.

<img src="diagrams\docker-airflow-architecture-314mini.drawio.svg" alt="Docker Airflow Architecture">

### 2.2 A Complete Task Lifecycle Example<br>
**0. Initialization**
- The initialization container first checks system resources, then connects to PostgreSQL to run database migrations, creating all necessary tables and schemas. It also creates default connections and the admin user. This is a prerequisite for all other services. <br>

**1. Scheduler --> ./dags volume**
- The Scheduler parses a DAG file from the DAGs volume that says "Run task X Sundays 12:00AM".

**2. Webserver --> ./dags volume**
- At 12:00AM Sunday, the Scheduler checks the database to confirm tasks's Xs dependencies are met.
- The Webserver reads DAG files to display their structure and code in the UI.

**3. Scheduler --> Postgres**
- After parsing DAGs, the Scheduler updates the database with information about DAG structure, schedule intervals, and any changes it detects. 
- It also reads from the Database to determine  which tasks are ready to be scheduled (confirms that taks X's dependencies are met).

**4. Scheduler --> Redis**
- When the Scheduler identifies a task that needs to run, it sends a message to Redis (the Celery broker) with the task information. 
- The Scheduler writes a message to Redis saying "task X is ready to run."
- Akin to putting a work order in a shared queue.

**5. Redis --> Celery Worker**
- After The Scheduler writes a message to Redis saying "task X is ready to run", a Celery Worker pulls this message from Redis.

**6. Celery Worker --> ./dags volume**
- The worker reads the DAG definition to understand exactly what code to execute.
  
**7. Celery Worker --> Postgres** 
- The Worker updates PostgreSQL to mark the task as "running".

**8. Celery Worker --> ./logs**
- The Worker writes to the logs volume as the task is executed by the Worker.

**9. Celery Worker --> Postgres** 
- Upon completion, the Worker updates PostgreSQL to mark the task as "success".

**10. Webserver --> Postgres**
- The Webserver reads the status from PostgreSQL and displays it in the UI.

**11. Webserver --> ./logs volume**
- The Webserver reads from the logs volume to display task logs in the web UI. 

<br>

### 2.3 Directory Structure

```
./dags
├── modules
│   ├── advanced_financial_metrics.py
│   ├── config.py
│   ├── currency_conversion.py
│   ├── data_aggregation.py
│   ├── data_cleaning.py
│   ├── data_enrichment.py
│   ├── data_loading.py
│   ├── database_operations.py
│   ├── datalake_operations.py
│   ├── outlier_thresholds.py
│   ├── ref_num_clean_up.py
│   ├── scraped_listings_clean_up.py
│   ├── update_db_tables.py
│   └── utilities.py
├── sql
│   ├── brand_get_latest_week_listings.sql
│   ├── brand_get_price_hist_for_the_last_1_dot_5_yrs.sql
│   ├── get_max_date_for_load_historical_data.sql
│   ├── parent_mdl_get_latest_week_listings.sql
│   ├── parent_mdl_get_price_hist_for_the_last_1_dot_5_yrs.sql
│   ├── ref_num_listings_weeks_on_market_get_required_hist_listings.sql
│   ├── ref_num_volatility_get_required_price_hist.sql
│   ├── specific_mdl_get_latest_week_listings.sql
│   └── specific_mdl_get_price_hist_for_the_last_1_dot_5_yrs.sql
├── topic1_historic_listings_db
│   └── dag1_upload_to_historic_listings_db
│       ├── dag1_upload_to_historic_listings_db.py
│       ├── step_1_wkly_csvs_to_parquet.py
│       └── step_2_upload_to_historic_listings_db_and_dl.py
├── topic2_reference_number_metrics
│   ├── listings_numb_weeks_on_market
│   │   └── dag_listings_numb_weeks_on_market.py
│   ├── price_hist_and_stats_incremental_mixed_cond
│   │   ├── dag1_ref_num_price_hist_stats_incremental_mixed_cond.py
│   │   └── util_ref_num_price_hist_and_stats_incremental_mixed_cond.py
│   └── volatility_of_hist_prices
│       ├── dag_price_volatility_of_ref_num_mixed_cond.py
│       ├── get_s3_buckets_subfolders.ipynb
│       └── util_price_volatility_of_ref_num_mixed_cond.py
├── topic3_specific_model_metrics
│   └── price_hist_and_stats_mixed_cond
│       ├── dag_specific_mdl_price_hist_and_stats_incremental_mixed_cond.py
│       └── util_specific_mdl_price_hist_and_stats_incremental_mixed_cond.py
├── topic4_parent_model_metrics
│   ├── price_hist_and_stats_incremental_mixed_cond
│   │   ├── dag_parent_mdl_price_hist_and_stats_incremental_mixed_cond.py
│   │   └── util_parent_mdl_price_hist_and_stats_incremental_mixed_cond.py
│   └── test.ipynb
├── topic5_brand_metrics
│   └── price_hist_and_stats_incremental_mixed_cond
│       ├── dag_brand_price_hist_and_stats_incremental_mixed_cond.py
│       └── util_brand_price_hist_and_stats_incremental_mixed_cond.py
├── topic6_market_makeup_stats
│   ├── dag_mrkt_makeup_pipeline.py
│   └── util_mrkt_makeup_pipeline.py
└── topic7_stop_instance
    └── dag_stop_instance.py

```


## 3. Pipeline Components

### 3.1 DAG Workflow
The pipeline consists of 9 sequential DAGs that process data in a specific order:

<img src="diagrams\dag-execution-order-mini.drawio.svg" alt="DAGs Execution Order">

## 4. Process Description

### 4.1 Data Ingestion and Preparation (DAG 1)
The first DAG performs several critical functions:

- Extracts approximately 70 CSV files from S3 (representing ~1 million watch listings).
- Aggregates data into a single parquet file stored in S3 staging area.
- Performs data cleansing, format standardization, currency conversion, and outlier detection and removal.
- Loads processed data to a PostgreSQL RDS database.

### 4.2 Market Price Calculations & Price Trend Analysis (DAGs 2-5)
These DAGs calculate current market prices at different levels of aggregation:

**Reference Number Level (DAG 2)**
- Aggregates current listings by brand and reference number.
- Calculates median price for the current week.
- Combines the new data with historical data to compute 30-day rolling average of median prices.
- Calculates price changes over 1 week, 1 month, 3 months, 6 months, and 1 year.

**Specific Model Level (DAG 3)**
- Aggregates listings by brand and specific model
- Follows similar calculation methodology as reference number level

**Parent Model Level (DAG 4)**
- Aggregates listings by brand and parent model (e.g., Rolex Daytona, Omega Constellation)
- Follows similar calculation methodology as reference number level

**Brand Level (DAG 5)**
- Aggregates all listings by brand
- Follows similar calculation methodology as reference number level

### 4.3 Market Composition Analysis (DAG 6)
Calculates market makeup metrics for each aggregation level:

- Total number of unique listings
- Number and percentage of pre-owned vs. brand new watches
- Distribution of listings across condition categories

### 4.4 Price Volatility Analysis (DAG 7)
Calculates volatility metrics for reference number level data:

- Measures the rate of fluctuations in the price of the asset over time, a key factor in assessing investment risk.
- Computes volatility over 1 month, 3 months, 6 months, and 1 year periods.

### 4.5 Growth Rate Analysis (DAG **Coming Soon**)
Calculates average annual growth rates for reference number level data:

- Computes growth trends over 1 month, 3 months, 6 months, and 1 year periods
- Provides annualized performance metrics

### 4.6 Liquidity of Watch Models (DAG 8)
This DAG calculates how long watch listings remain on the market by analyzing historical listing data. It processes data biweekly and handles brands in chunks for efficient computation.

Calculates key metrics for each watch model:
- Number of listings
- Average days/weeks listed
- Median days/weeks listed
- Standard deviation of listing duration
- Minimum/maximum listing duration

### 4.7 Stop Instance (DAG 9)
Upon completion of the pipeline, this dag stops all docker containers and shuts down Ec2 instance.

## 5. Technical Implementation
### 5.1 Chunking Strategy
The pipeline implements a chunking strategy to process large datasets efficiently:

- Data is divided into manageable chunks
- Each chunk is processed in parallel within resource constraints
- TaskGroups are used to organize chunk processing

### 5.2 Resource Management
The pipeline uses Airflow pools to control concurrency:

- ref_num_volatility_pool
- specific_model_metrics_pool
- parent_model_metrics_pool
- brand_metrics_pool

### 5.3 Data Storage
The pipeline utilizes multiple storage systems:

- S3 for raw data, intermediate results, and final outputs
- PostgreSQL RDS for structured data and historical records
- Local volumes for Airflow logs and configuration

## 6. Operational Considerations
### 6.1 Scheduling
While the Ec2 instance start is triggered by the completion an external pipline (in a separate Ec2 instance), This pipeline, is triggered automatically on schedule at 12:00 AM on Sundays.

### 6.2 Resource Optimization
Worker containers are configured to efficiently utilize EC2 resources based on task requirements.

### 6.3 Error Handling
The pipeline implements comprehensive error handling and logging to ensure data integrity.

## 7. Conclusion
This data pipeline transforms raw watch listing data into comprehensive market intelligence, enabling detailed analysis of the luxury watch market across multiple dimensions. The architecture ensures efficient processing of approximately 1 million listings per week while maintaining data integrity and analytical accuracy.

The modular design allows for future expansion to include additional metrics or aggregation levels as business requirements evolve.
