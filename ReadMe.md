# Stock ETL Pipeline – Data Engineering Project

## Project Overview
This project demonstrates an end-to-end **batch ETL pipeline** built using modern **Data Engineering tools and best practices**.

The pipeline ingests historical stock market data, applies transformations using Python, stores optimized data in **Amazon S3 as partitioned Parquet files**, and enables analytics using **Amazon Athena**. The entire pipeline is containerized using Docker and orchestrated using Prefect.

---

## Architecture Overview

CSV Data  
↓  
Python ETL (Extract, Transform, Load)  
↓  
Prefect (Workflow Orchestration)  
↓  
Docker & Docker Compose  
↓  
Amazon S3 (Partitioned Parquet Data Lake)  
↓  
Amazon Athena (SQL Analytics)

---

## Technologies Used

### Core
- Python (pandas)
- SQL
- MySQL (local warehouse / dimensional modeling)
- Docker & Docker Compose
- Prefect (Workflow orchestration)

### AWS
- Amazon S3 (Data Lake)
- Amazon Athena (Query Engine)
- AWS IAM (Access Control)
- AWS Glue Data Catalog

### Data Engineering Concepts
- Batch ETL
- Incremental Loads
- Idempotent Pipelines
- Dimensional Modeling (Fact & Dimension tables)
- Parquet & Partitioning
- CI/CD with GitHub Actions

---

## Project Workflow

1. **Extract**
   - Reads historical stock data from CSV

2. **Transform**
   - Cleans data
   - Converts date formats
   - Adds stock symbol
   - Removes duplicates

3. **Load**
   - Loads data into MySQL (dimensional model)
   - Writes partitioned Parquet files to Amazon S3

4. **Analytics**
   - Athena queries directly on S3 Parquet data

5. **Automation**
   - Orchestrated using Prefect
   - Containerized using Docker
   - CI pipeline via GitHub Actions

---

## How to Run (Docker)

```bash
docker compose up --build

---

## Monitoring & Logging

This project includes multiple layers of observability to ensure reliability, debugging, and traceability of the ETL pipeline.

### Application-level Logging
- Python `logging` module is used inside ETL scripts
- Tracks execution flow, errors, and data processing steps

### Task-level Observability
- Prefect provides task-level and flow-level monitoring
- Each ETL stage (extract, transform, load) is tracked with execution state and logs

### Container-level Logging
- Docker captures logs for ETL and database containers
- Logs can be accessed using:
  ```bash
  docker logs <container_name>

