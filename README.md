# ETL Workflow for Retail and Customer Data using Cloud Functions
## Project Overview
This project demonstrates an end-to-end data pipeline built on **Google Cloud Platform (GCP)**. The pipeline automates the generation, storage, and processing of synthetic retail and customer data for analytical purposes. The workflow includes:
1. Generating synthetic retail and customer data using Python.
2. Uploading the generated data to **Google Cloud Storage (GCS)**.
3. Triggering a Google Cloud Function to load the uploaded data into **BigQuery** for further analysis.

This solution is scalable, serverless, and cost-efficient, leveraging GCP services to build robust and automated pipelines.

## Table of Contents
1. Prerequisites
2. Technologies Used
3. Project Architecture
4. Data Flow
5. Input Files
6. Installation & Setup
7. Execution
8. Conclusion

## Prerequisites
Before you start, ensure you have the following:
1. A GCP account with **Google Cloud Storage** and **BigQuery** enabled.
2. **Google Cloud SDK** installed and configured.
3. Proper IAM roles assigned to your service account:
   - **Storage Admin:** For managing GCS files.
   - **BigQuery Data Editor:** For loading data into BigQuery tables.
  
## Technologies Used
The following tools and technologies are used in this project:
- **Google Cloud Platform (GCP) :**
  - **Google Cloud Storage (GCS):** To store generated data files.
  - **BigQuery:** To store and analyze data.
  - **Cloud Functions:** To automate data ingestion into BigQuery.
- **Python:** For data generation and pipeline scripting.
- **Faker Library:** To generate synthetic data.

## Project Architecture
Below is the high-level architecture of the pipeline:
![Project Architecture](https://github.com/malviya1908/gcp-cloud-function-bigquery-pipeline/blob/main/architecture/architecture1.png)
1. **Data Generator:** Python script generates synthetic retail and customer data and uploads it to GCS.
2. **Google Cloud Storage (GCS):** Stores uploaded CSV files as triggers for the Cloud Function.
3. **Cloud Function:** Processes uploaded files and loads them into BigQuery.
4. **BigQuery:** Stores structured data for analytics and querying.

## Data Flow
1. **Data Generation:** Synthetic retail and customer data are generated using the Faker library. This includes columns like transaction details, customer demographics, and more.
2. **File Upload:** Generated data is uploaded as CSV files to specific paths in a GCS bucket
   - **Retail data:** retail_data/retail/<date>/<file>.csv
   - **Customer data:** retail_data/customer/<date>/<file>.csv
3. **Triggering Cloud Function:** File uploads to GCS automatically trigger the Cloud Function, which validates and processes the files.
4. **BigQuery Loading:** The Cloud Function loads the data into corresponding BigQuery tables (retail_table or customer_table).

## Input Files
The pipeline handles two types of input files:
1. Retail Data Files:
   - transaction_id (STRING)
   - customer_id (STRING)
   - Product (STRING)
   - Category (STRING)
   - quantity (INTEGER)
   - price (FLOAT)
   - Total (FLOAT)
   - date (DATE)
   - Payment_Method (STRING)
2. Customer Data File:
   - customer_id (STRING)
   - first_name (STRING)
   - last_name (STRING)
   - email (STRING)
   - phone (STRING)
   - address (STRING)
   - state (STRING)
   - country (STRING)

## Conclusion
This project showcases how to build a robust, serverless data pipeline on GCP using Cloud Functions, Google Cloud Storage, and BigQuery. It demonstrates automation, scalability, and ease of integration between GCP services, providing a framework that can be extended for real-time data processing and analytics.
Feel free to extend this project by adding:
- Data validation and transformation logic in the Cloud Function.
- Visualization using tools like Google Data Studio.
- Monitoring and alerting for pipeline failures.
  
