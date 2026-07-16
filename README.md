# QuickCommerce Lakehouse (QuickLake)

**Real-Time CDC-to-Lakehouse Platform for Modern Retail Analytics**

[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)]()
[![Apache Iceberg](https://img.shields.io/badge/Apache_Iceberg-1E90FF?style=for-the-badge)]()
[![Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)]()

> A production-inspired real-time data engineering project that captures transactional changes from PostgreSQL using CDC and continuously builds an analytics-ready **Apache Iceberg Lakehouse**.

---

## Overview

QuickLake streams live business events from a simulated Quick Commerce platform into a scalable, analytics-ready Lakehouse — **without impacting the source OLTP system**.

Instead of slow nightly ETL jobs or running expensive queries on production databases, QuickLake delivers near real-time insights using modern Data Engineering best practices.

**Key Concepts Demonstrated**:
- Real-Time Change Data Capture (CDC)
- Apache Iceberg Lakehouse
- Spark ETL (deduplication, schema evolution, aggregations)
- Airflow Workflow Orchestration
- Data Quality Validation
- Partition Optimization & Incremental Processing
- Schema Evolution without downtime
- Docker-based reproducible infrastructure

---

## Problem Statement

Modern applications generate thousands of transactions per minute, yet many organizations still:
- Run BI dashboards directly on production databases
- Execute heavy analytical queries on OLTP systems
- Rely on stale nightly batch jobs
- Struggle with schema changes breaking pipelines

This leads to increased latency, poor application performance, outdated insights, and low data reliability.

QuickLake shows how a **CDC-driven Lakehouse architecture** solves these challenges.

---

## Solution & Architecture

![Architecture](docs/architecture.png)

**End-to-End Pipeline**:
Customer Order → PostgreSQL (OLTP) → OLake CDC → Apache Iceberg → Spark ETL →
Data Validation → Airflow Orchestration → Analytics-Ready Tables → Power BI / Trino / SQL
text---

## Business Domain

The project simulates a fast-paced **Quick Commerce company** (similar to Blinkit, Zepto, Instamart, or DoorDash).

**Core Business Entities**:

| Table                | Description                          |
|----------------------|--------------------------------------|
| Customers            | Customer profiles                    |
| Orders               | Complete order lifecycle             |
| Inventory            | Warehouse stock levels               |
| Payments             | Payment transactions                 |
| Returns              | Refund & return requests             |
| Delivery Partners    | Delivery tracking & performance      |

**Live Traffic Includes**:
- New orders & confirmations
- Cancellations & payment failures
- Refunds, inventory updates, delivery delays

---

## Production Engineering Scenarios

- **Schema Evolution**: New `customer_phone` column added live. Iceberg evolves the schema automatically.
- **Duplicate CDC Events**: Intentionally generated and cleaned by Spark (see `docs/dedup-results.md`).
- **Partition Optimization**: Iceberg tables partitioned by `year/month/day`.
- **Incremental Processing**: Only new changes are processed.
- **Data Quality Validation**: Airflow DAG validates nulls, PK/FK integrity, cross-table consistency, and row count anomalies.

---

## Project Structure


quickcommerce-lakehouse/
├── data_replayer/          # Olist dataset download + realistic replay engine
├── db/                     # Schema init & migrations (incl. schema evolution)
├── cdc/                    # OLake CDC configuration
├── spark_jobs/             # ETL jobs (dedup, aggregations, compaction)
├── airflow/                # DAGs + custom data quality plugins
├── tests/                  # Unit & integration tests
├── docs/                   # Architecture, results, schema mapping
├── notebooks/              # Exploratory analytics
├── data/                   # Raw data (gitignored)
├── docker-compose.yml
├── .env.example
├── requirements.txt
└── README.md

Getting Started
1. Clone the Repository
Bashgit clone https://github.com/<username>/quickcommerce-lakehouse.git
cd quickcommerce-lakehouse
2. Environment Setup
Bashcp .env.example .env
# Edit .env with your credentials (Postgres, MinIO, etc.)
3. Start Infrastructure
Bashdocker compose up -d
Services: PostgreSQL, MinIO, Spark, Airflow
4. Load & Replay Data
Bash# Download Olist dataset
python data_replayer/download_dataset.py

# Start realistic replay (simulates live traffic)
python data_replayer/replay_engine.py
5. Configure & Start CDC

Update cdc/olake_config.yaml
Start the OLake connector

6. Run the Pipeline

Open Airflow UI: http://localhost:8080
Enable and trigger quickcommerce_pipeline


Demo
<img src="docs/architecture.png" alt="Architecture">
<img src="docs/demo.gif" alt="Demo GIF">
(Screenshots: Airflow DAG, Spark Jobs, Iceberg Tables, Sample Dashboard)
Sample Analytics Queries

Total Sales Today / Revenue by Category
Hourly Order Volume & Cancellation Rate
Average Delivery Time & Delivery Partner Performance
Refund Percentage & Inventory Turnover
Top Selling Products & Payment Success Rate

Explore live in notebooks/exploratory_queries.ipynb
Testing
Bashpytest
Tests cover schema evolution, deduplication, data quality rules, and ETL logic.

Key Learnings & Resume Highlights

Designed a real-time CDC pipeline using OLake to stream PostgreSQL changes into Apache Iceberg.
Built Spark ETL jobs for deduplication, schema evolution, incremental processing, and aggregations.
Automated orchestration, retries, and comprehensive data quality validation with Apache Airflow.
Simulated production-scale Quick Commerce workloads using a configurable replay engine based on real Olist data.
Implemented a fully Dockerized, production-inspired Lakehouse architecture.


Future Roadmap

Late-arriving data handling & Iceberg Time Travel
Great Expectations integration
Trino query engine + Superset dashboards
Pipeline monitoring with Prometheus + Grafana
AWS / Cloud deployment


Built with ❤️ for modern Data Engineering
Feel free to explore, fork, or reach out with questions!
