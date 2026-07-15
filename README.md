# QuickCommerce Lakehouse

**A real-time CDC-to-lakehouse pipeline built around a simulated quick-commerce business — orders, cancellations, refunds, inventory, and delivery data streamed live from PostgreSQL into Apache Iceberg, transformed with Spark, and orchestrated with Airflow.**

> This project answers a question every quick-commerce company actually has: *"Can we see what's happening in the business right now, without hammering the production database?"*

---

## Why this project exists

Quick-commerce platforms (think 10-minute grocery delivery) generate a constant stream of order, inventory, and delivery events. Running analytics queries directly against the production database slows down the live app. Nightly batch ETL means the business is always looking at yesterday's data. This project solves that by capturing changes as they happen (CDC) and landing them in a lakehouse that's queryable without touching the source system.

It's built as a portfolio project to demonstrate production-style data engineering — not a toy dataset, but a business with realistic entities, realistic failure modes (duplicates, schema changes, delayed events), and realistic engineering answers to each.

---

## Architecture

```
                         ┌──────────────────────────┐
                         │   Traffic Generator       │
                         │   (Python)                │
                         │   orders, cancellations,  │
                         │   refunds, inventory,     │
                         │   delayed deliveries      │
                         └────────────┬─────────────┘
                                      │ INSERT / UPDATE / DELETE
                                      ▼
                         ┌──────────────────────────┐
                         │   PostgreSQL (OLTP)       │
                         │   Orders · Customers      │
                         │   Inventory · Payments    │
                         │   Returns · Delivery Ptns │
                         │   (WAL / logical decoding)│
                         └────────────┬─────────────┘
                                      │ CDC stream
                                      ▼
                         ┌──────────────────────────┐
                         │   OLake (CDC connector)   │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │   Apache Iceberg tables   │
                         │   on S3 (or MinIO local)  │
                         │   partitioned by          │
                         │   year / month / day      │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │   Spark ETL jobs          │
                         │   • dedup                 │
                         │   • schema evolution       │
                         │   • daily aggregations    │
                         │   • partition compaction  │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │   Apache Airflow          │
                         │   DAG: ingest → transform  │
                         │   → validate → aggregate  │
                         │   (data quality checks,   │
                         │    retries, logging)      │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │   Analytics-ready tables  │
                         │   (query via Spark SQL /  │
                         │    Trino / notebook)      │
                         └──────────────────────────┘
```

---

## The business: QuickCommerce

A simulated 10-minute delivery business, with six core entities:

| Table | Description |
|---|---|
| `customers` | Customer profiles (name, address, signup date, phone — added mid-project to demonstrate schema evolution) |
| `orders` | Order lifecycle: placed → confirmed → out for delivery → delivered / cancelled |
| `inventory` | Warehouse stock levels per SKU, updated on every order and restock |
| `payments` | Payment attempts, successes, failures, linked to orders |
| `returns` | Refund requests linked to orders and payments |
| `delivery_partners` | Partner assignment, delivery status, delay tracking |

Simulated live traffic includes: new orders, cancellations, refunds, inventory decrements/restocks, and delayed deliveries — continuously generated to mimic a live app rather than a static dump.

---

## Engineering challenges implemented

These are the details that separate "I used Iceberg" from "I understand Iceberg":

1. **Schema evolution** — `customer_phone` is added to the `customers` table mid-stream, after the pipeline is already running. The README documents how Iceberg absorbs this without breaking downstream queries.
2. **Duplicate event handling** — Duplicate CDC events are deliberately generated and deduplicated in the Spark layer, with before/after row counts to prove it.
3. **Partitioning** — Iceberg tables are partitioned by `year/month/day` to keep query scans efficient as data grows.
4. **Data quality checks in Airflow** — row-count deltas, null checks, and referential integrity checks between `orders`, `payments`, and `returns` run as part of the DAG, with failures logged and retried.

### Roadmap (deliberately scoped out of v1, listed honestly rather than rushed)
- Late-arriving data handling (yesterday's order landing today)
- CDC connector recovery testing (stop/restart, verify no data loss)
- Compaction / VACUUM demonstration on small files
- Pipeline monitoring dashboard (CDC lag, rows/min, DAG status, job duration)
- Natural-language query layer (LLM → SQL) for self-service analytics

---

## Tech stack

`Python` · `PostgreSQL` · `OLake` · `Apache Iceberg` · `Apache Spark` · `Apache Airflow` · `Docker` · `S3 / MinIO` · `Git`

---

## Folder structure

```
quickcommerce-lakehouse/
│
├── README.md
├── .gitignore
├── docker-compose.yml              # spins up Postgres, MinIO, Airflow, Spark
├── .env.example                    # sample env vars (DB creds, S3 endpoint, etc.)
│
├── docs/
│   ├── architecture.png            # exported version of the diagram above
│   ├── schema-evolution.md         # write-up + screenshots of the customer_phone case
│   ├── dedup-results.md            # before/after row counts, methodology
│   └── demo.gif                    # short screen capture of the pipeline running
│
├── db/
│   ├── init/
│   │   ├── 01_schema.sql           # CREATE TABLE statements for all 6 entities
│   │   └── 02_seed.sql             # initial seed data
│   └── migrations/
│       └── 001_add_customer_phone.sql
│
├── traffic_generator/
│   ├── generator.py                # main script producing continuous CDC-worthy events
│   ├── scenarios/
│   │   ├── new_orders.py
│   │   ├── cancellations.py
│   │   ├── refunds.py
│   │   ├── inventory_updates.py
│   │   └── delayed_deliveries.py
│   └── config.yaml
│
├── cdc/
│   ├── olake_config.yaml           # OLake source/destination config
│   └── notes.md                    # setup gotchas, connector config decisions
│
├── spark_jobs/
│   ├── dedup_orders.py
│   ├── schema_evolution_handler.py
│   ├── daily_aggregations.py
│   ├── partition_compaction.py
│   └── utils/
│       └── iceberg_helpers.py
│
├── airflow/
│   ├── dags/
│   │   └── quickcommerce_pipeline_dag.py
│   ├── plugins/
│   │   └── data_quality_checks.py
│   └── requirements.txt
│
├── tests/
│   ├── test_dedup.py
│   ├── test_schema_evolution.py
│   └── test_data_quality_checks.py
│
├── notebooks/
│   └── exploratory_queries.ipynb   # sample analytical queries against Iceberg tables
│
└── requirements.txt
```

---

## Setup & running locally

```bash
# 1. Clone and configure
git clone https://github.com/<your-username>/quickcommerce-lakehouse.git
cd quickcommerce-lakehouse
cp .env.example .env

# 2. Spin up infrastructure
docker-compose up -d          # Postgres, MinIO (S3-compatible), Airflow, Spark

# 3. Start the traffic generator
python traffic_generator/generator.py

# 4. Configure and start OLake CDC
# see cdc/notes.md for connector setup

# 5. Trigger the Airflow DAG
# visit localhost:8080, enable quickcommerce_pipeline_dag
```

> Note: this project runs against MinIO locally to avoid AWS costs during development. The Iceberg/Spark/S3 configuration is S3-compatible and deployable to real AWS S3 with a config change — this is called out explicitly rather than implied.

---

## What this project demonstrates

`Python` · `SQL` · `Apache Spark` · `Apache Airflow` · `Apache Iceberg` · `Change Data Capture (CDC)` · `PostgreSQL` · `Docker` · `ETL` · `Data Quality & Validation` · `Lakehouse Architecture` · `Git`

---

## Author's note

This is a portfolio project built to learn and demonstrate real-time data engineering patterns. Traffic is simulated (a Python generator standing in for a live production app) rather than pulled from a real business — this is stated here plainly rather than implied otherwise. Everything else — the CDC mechanics, schema evolution handling, dedup logic, and orchestration — runs against real infrastructure (Postgres, OLake, Iceberg, Spark, Airflow), just not real user traffic.
