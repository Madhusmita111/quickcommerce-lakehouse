# QuickCommerce Lakehouse
### Real-Time CDC-to-Lakehouse Platform for Modern Retail Analytics

<p align="center">

![Python](https://img.shields.io/badge/Python-3.11-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.x-orange)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-red)
![Apache Iceberg](https://img.shields.io/badge/Apache-Iceberg-0A5B94)
![Docker](https://img.shields.io/badge/Docker-Ready-2496ED)
![CDC](https://img.shields.io/badge/CDC-OLake-success)
![Lakehouse](https://img.shields.io/badge/Architecture-Lakehouse-brightgreen)
![License](https://img.shields.io/badge/License-MIT-green)

</p>

---

## Overview

**QuickLake** is a production-inspired **real-time data engineering platform** that captures transactional database changes using **Change Data Capture (CDC)** and continuously builds an analytics-ready **Apache Iceberg Lakehouse** without impacting the source application.

Instead of relying on slow nightly ETL jobs or running expensive analytical queries against production databases, QuickLake streams live business events into a scalable Lakehouse where they can be transformed, validated, and queried independently.

The project demonstrates modern Data Engineering concepts including:

- Real-Time Change Data Capture
- Apache Iceberg Lakehouse
- Spark ETL
- Airflow Workflow Orchestration
- Data Quality Validation
- Schema Evolution
- Partition Optimization
- Docker-based Infrastructure

Rather than using a static CSV dataset, the project simulates a real Quick Commerce company where orders, payments, deliveries, cancellations, refunds, and inventory updates continuously flow through the pipeline.

---

# Problem Statement

Modern applications generate thousands of transactions every minute.

Unfortunately, many organizations still:

- Run BI dashboards directly against production databases.
- Execute expensive analytical queries on OLTP systems.
- Depend on nightly ETL jobs.
- Work with stale business data.
- Experience broken pipelines whenever schemas change.

These approaches create several problems:

- Increased database latency
- Poor application performance
- Outdated dashboards
- Difficult schema management
- Low data reliability

QuickLake demonstrates how a modern CDC-driven Lakehouse architecture solves these challenges.

---

# Solution

QuickLake separates operational workloads from analytical workloads.

Instead of querying the production PostgreSQL database, all database changes are streamed into Apache Iceberg where Spark transforms the data into analytics-ready tables.

The production application remains unaffected while business teams receive near real-time insights.

---

# Architecture

```
                    +-------------------------------+
                    |   Python Traffic Generator    |
                    |-------------------------------|
                    | Orders                        |
                    | Inventory                     |
                    | Payments                      |
                    | Returns                       |
                    | Deliveries                    |
                    +---------------+---------------+
                                    |
                           INSERT / UPDATE / DELETE
                                    |
                                    ▼
                    +-------------------------------+
                    |      PostgreSQL (OLTP)        |
                    |-------------------------------|
                    | Customers                     |
                    | Orders                        |
                    | Inventory                     |
                    | Payments                      |
                    | Returns                       |
                    | Delivery Partners             |
                    +---------------+---------------+
                                    |
                         WAL / Logical Replication
                                    |
                                    ▼
                    +-------------------------------+
                    |           OLake               |
                    |     CDC Connector             |
                    +---------------+---------------+
                                    |
                                    ▼
                    +-------------------------------+
                    |     Apache Iceberg            |
                    |        Lakehouse              |
                    |       S3 / MinIO              |
                    +---------------+---------------+
                                    |
                                    ▼
                    +-------------------------------+
                    |        Apache Spark           |
                    |-------------------------------|
                    | ETL                           |
                    | Deduplication                 |
                    | Schema Evolution              |
                    | Aggregations                  |
                    | Compaction                    |
                    +---------------+---------------+
                                    |
                                    ▼
                    +-------------------------------+
                    |      Apache Airflow           |
                    |-------------------------------|
                    | DAG Scheduling                |
                    | Validation                    |
                    | Retry                         |
                    | Logging                       |
                    +---------------+---------------+
                                    |
                                    ▼
                    +-------------------------------+
                    | Analytics Ready Iceberg Tables|
                    +-------------------------------+
```

---

# End-to-End Pipeline

```
Customer Places Order

        │

        ▼

 PostgreSQL

        │

   CDC (OLake)

        │

        ▼

Apache Iceberg

        │

        ▼

 Apache Spark

        │

        ▼

 Data Validation

        │

        ▼

 Airflow

        │

        ▼

 Analytics Tables

        │

        ▼

 Power BI / SQL / Trino
```

---

# Business Domain

The project simulates a **10-minute Quick Commerce company** similar to Blinkit, Zepto, Instamart, or DoorDash.

## Core Business Entities

| Table | Description |
|--------|-------------|
| Customers | Customer information |
| Orders | Complete order lifecycle |
| Inventory | Warehouse stock |
| Payments | Payment transactions |
| Returns | Refund requests |
| Delivery Partners | Delivery tracking |

The traffic generator continuously produces:

- New Orders
- Order Confirmations
- Order Cancellations
- Payment Failures
- Refund Requests
- Inventory Updates
- Delivery Delays

creating realistic production-like workloads.

---

# Production Engineering Scenarios

## Schema Evolution

A new column

```
customer_phone
```

is added while the pipeline is running.

Apache Iceberg automatically evolves the schema without requiring table recreation or pipeline downtime.

---

## Duplicate CDC Events

Duplicate events are intentionally generated.

Spark removes duplicates before data reaches analytics tables.

The repository includes before/after row counts.

---

## Partition Optimization

Iceberg tables are partitioned using

```
Year
Month
Day
```

to improve scan performance and reduce query costs.

---

## Incremental Processing

Only new CDC events are processed.

Historical data is never reloaded unnecessarily.

---

## Data Quality Validation

Every Airflow execution validates:

- Null values
- Duplicate Orders
- Primary Key uniqueness
- Foreign Key integrity
- Unexpected row count changes
- Payment ↔ Order consistency
- Return ↔ Payment consistency

Pipeline failures are logged and retried automatically.

---

# Example Analytical Queries

QuickLake enables queries such as:

- Total Sales Today
- Revenue by Category
- Hourly Order Volume
- Cancellation Rate
- Refund Percentage
- Average Delivery Time
- Inventory Turnover
- Top Selling Products
- Delivery Partner Performance
- Payment Success Rate

---

# Technology Stack

| Layer | Technology |
|---------|------------|
| Programming | Python |
| Database | PostgreSQL |
| CDC | OLake |
| Lakehouse | Apache Iceberg |
| Processing | Apache Spark |
| Workflow | Apache Airflow |
| Storage | MinIO / AWS S3 |
| Infrastructure | Docker |
| Version Control | Git |
| Analytics | Spark SQL |

---

# Project Structure

```
quicklake/

├── airflow/
├── cdc/
├── db/
├── docs/
├── notebooks/
├── spark_jobs/
├── tests/
├── traffic_generator/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

# Getting Started

## Clone Repository

```bash
git clone https://github.com/<username>/quicklake.git

cd quicklake
```

---

## Configure Environment

```bash
cp .env.example .env
```

Update

- PostgreSQL credentials
- MinIO endpoint
- Iceberg catalog
- Airflow variables

---

## Start Infrastructure

```bash
docker compose up -d
```

Services started:

- PostgreSQL
- MinIO
- Spark
- Airflow

---

## Generate Live Traffic

```bash
python traffic_generator/generator.py
```

This continuously creates realistic business events.

---

## Configure CDC

Configure OLake using

```
cdc/olake_config.yaml
```

Start the connector.

---

## Execute Airflow DAG

Open

```
http://localhost:8080
```

Enable

```
quicklake_pipeline
```

The DAG performs

- CDC ingestion
- Spark ETL
- Data Validation
- Aggregation

---

# Sample Dashboard

The Lakehouse can power dashboards showing:

- Revenue Trends
- Sales by Hour
- Inventory Health
- Delivery KPIs
- Cancellation Analytics
- Refund Analysis
- Customer Growth

*(Screenshots will be added after implementation.)*

---

# Testing

Unit tests validate

- Schema Evolution
- Deduplication
- Data Quality Rules
- ETL Logic

Run

```bash
pytest
```

---

# Future Improvements

- [x] CDC Pipeline
- [x] Apache Iceberg
- [x] Spark ETL
- [x] Airflow DAG
- [x] Data Quality Checks
- [x] Schema Evolution

Future Roadmap

- [ ] Late Arriving Data
- [ ] Iceberg Time Travel
- [ ] CDC Recovery Testing
- [ ] Great Expectations Integration
- [ ] Pipeline Monitoring Dashboard
- [ ] Trino Query Engine
- [ ] Natural Language Analytics
- [ ] Prometheus + Grafana Monitoring
- [ ] AWS Deployment

---

# Key Learnings

This project demonstrates practical experience with:

- Real-Time Change Data Capture
- Apache Iceberg Lakehouse
- Spark-based ETL
- Airflow Orchestration
- Data Validation
- Partition Optimization
- Schema Evolution
- Dockerized Infrastructure
- Production-style Data Engineering

---

# Resume Highlights

- Designed a real-time CDC pipeline using OLake to capture PostgreSQL changes into Apache Iceberg.
- Built Spark ETL jobs for deduplication, schema evolution, incremental processing, and daily aggregations.
- Automated orchestration, retries, and data quality validation using Apache Airflow.
- Simulated production-scale Quick Commerce traffic using a configurable Python event generator.
- Built a production-inspired Lakehouse architecture using Docker, MinIO, PostgreSQL, Spark, and Iceberg.

---

# Demo

| Component | Preview |
|------------|---------|
| Architecture | docs/architecture.png |
| Airflow DAG | docs/airflow.png |
| Spark Jobs | docs/spark.png |
| Iceberg Tables | docs/iceberg.png |
| Dashboard | docs/dashboard.png |
| Demo GIF | docs/demo.gif |

---

# Contributing

Contributions, suggestions, and improvements are welcome.

Feel free to open an issue or submit a pull request.

---

# License

This project is licensed under the MIT License.

---

> **Note:** This is a portfolio project built for learning and demonstration purposes. Business traffic is simulated using a Python event generator rather than real production users. All core infrastructure (PostgreSQL, OLake, Apache Iceberg, Spark, Airflow, Docker) is real and runs locally using containerized services. The project is designed to reflect production engineering practices while remaining reproducible on a personal development machine.
