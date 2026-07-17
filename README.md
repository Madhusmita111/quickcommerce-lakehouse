# QuickCommerce Lakehouse

<div align="center">

### Real-Time CDC → Apache Iceberg Lakehouse for Modern Retail Analytics

A production-inspired Data Engineering project that captures live transactional changes from PostgreSQL using Change Data Capture (CDC) and continuously builds an analytics-ready Apache Iceberg Lakehouse with Apache Spark and Apache Airflow.

<p>

<img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white">

<img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white">

<img src="https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white">

<img src="https://img.shields.io/badge/Apache_Iceberg-1E90FF?style=for-the-badge">

<img src="https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white">

<img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white">

<img src="https://img.shields.io/badge/MinIO-C72E49?style=for-the-badge">

<img src="https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black">

</p>

</div>

---

# Overview

Modern retail platforms generate millions of transactional events every day. Running analytical workloads directly on transactional databases leads to slower applications, stale insights, and expensive infrastructure.

**QuickLake** demonstrates how modern organizations solve this challenge using a **Change Data Capture (CDC) driven Lakehouse Architecture**.

Instead of relying on nightly ETL jobs, QuickLake continuously captures changes from PostgreSQL, processes them using Apache Spark, stores them in Apache Iceberg, validates data quality, and orchestrates the complete pipeline with Apache Airflow.

The result is an analytics-ready Lakehouse that supports near real-time business intelligence while keeping production databases lightweight and responsive.

---

# Key Features

-  Real-Time Change Data Capture (CDC)
-  Apache Iceberg Lakehouse
-  Incremental Spark ETL
-  Schema Evolution
-  Data Quality Validation
-  Dockerized Infrastructure
-  Apache Airflow Orchestration
-  Analytics-ready Tables
-  Deduplication of CDC Events
-  Partitioned Iceberg Tables

---

#  Problem Statement

Traditional analytics architectures face several limitations:

- Heavy BI queries executed directly on production databases
- High database latency during analytical workloads
- Nightly ETL jobs causing stale reports
- Duplicate events leading to inconsistent analytics
- Schema changes breaking ETL pipelines
- Limited support for incremental processing
- Increasing infrastructure costs as data volume grows

These challenges reduce system reliability and delay business decision-making.

---

#  Solution

QuickLake implements a modern CDC-based Data Engineering pipeline that continuously captures database changes and transforms them into analytics-ready datasets.

The platform consists of:

- PostgreSQL as the OLTP database
- OLake for Change Data Capture
- Apache Spark for ETL and transformations
- Apache Iceberg as the Lakehouse storage layer
- Apache Airflow for orchestration
- MinIO as object storage
- Power BI or SQL engines for analytics

The architecture separates transactional workloads from analytical workloads, enabling scalable, low-latency reporting without affecting production systems.

---

#  Architecture

```
                   Customer Orders

                          │
                          ▼

                PostgreSQL (OLTP)

                          │
                          ▼

                  OLake CDC Connector

                          │
                          ▼

                Apache Iceberg Tables

                          │
                          ▼

                  Apache Spark ETL

         • Cleaning
         • Deduplication
         • Transformations
         • Incremental Processing
         • Aggregations

                          │
                          ▼

               Data Quality Validation

                          │
                          ▼

               Apache Airflow DAG

                          │
                          ▼

             Analytics Ready Lakehouse

                 │                    │

                 ▼                    ▼

           Power BI              SQL / Trino
```

---

##  Architecture Diagram

> Replace this placeholder after creating your architecture diagram.

```text
docs/architecture.png
```

```markdown
![Architecture](docs/architecture.png)
```

---

#  End-to-End Data Flow

1. Customers place orders through the Quick Commerce application.

2. Transactions are stored in PostgreSQL.

3. OLake continuously monitors database changes using Change Data Capture.

4. CDC events are streamed into Apache Iceberg.

5. Spark reads incremental CDC events.

6. Spark performs:

- Data Cleaning
- Deduplication
- Schema Evolution
- Incremental Processing
- Business Aggregations

7. Airflow orchestrates the complete workflow.

8. Data Quality rules validate the transformed datasets.

9. Curated Iceberg tables become available for reporting.

10. Business users query data using Power BI or SQL engines.

---

#  Business Domain

QuickLake simulates a modern **Quick Commerce** platform similar to:

- Blinkit
- Zepto
- Instamart
- DoorDash

The platform continuously generates realistic operational events including:

- Customer registrations
- Order placements
- Inventory updates
- Payment transactions
- Refund requests
- Delivery status updates
- Order cancellations
- Payment failures

These events are streamed into the Lakehouse in near real-time.

---

#  Business Tables

| Table | Description |
|--------|-------------|
| Customers | Customer information |
| Orders | Complete order lifecycle |
| Products | Product catalog |
| Inventory | Warehouse inventory |
| Payments | Payment records |
| Returns | Return and refund details |
| Delivery Partners | Delivery operations |
| Warehouses | Fulfillment centers |

---

#  Tech Stack

| Category | Technology |
|-----------|------------|
| Programming Language | Python |
| Database | PostgreSQL |
| Change Data Capture | OLake |
| Processing Engine | Apache Spark |
| Lakehouse | Apache Iceberg |
| Workflow Orchestration | Apache Airflow |
| Object Storage | MinIO |
| Containerization | Docker Compose |
| Analytics | Power BI |
| Testing | PyTest |
| Version Control | Git & GitHub |

---
