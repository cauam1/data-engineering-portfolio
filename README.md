🌟 Hybrid OLAP → Lakehouse + Warehouse Pipeline
🚀 Project Overview

This is a highly advanced data engineering pipeline, built for enterprise-level OLAP, Data Warehousing, and BI integration. It extracts data from multiple OLAP cubes, transforms it through Bronze → Silver → Gold layers, implements SCD Type 2, enforces strict data quality, calculates KPIs, and exposes the data via a FastAPI interface for analytics and dashboards.

This project demonstrates architecture-level design, production-ready standards, and senior-level Python engineering skills.

🎯 Key Features

Multi-Cube Extraction: Automatically extracts data from multiple MDX cubes with dynamic schema detection.

Layered Architecture:

Bronze: Raw, minimally processed Parquet files.

Silver: Cleaned, validated, SCD1/SCD2 applied, historical tracking.

Gold: Aggregated tables with derived metrics and KPIs.

SCD Type 2: Tracks historical changes with effective dates, end dates, and current flags.

Delta-Like Incremental Load: Only new or changed rows are processed.

Data Quality & Validation: 30+ rules including null ratio, duplicates, and anomaly detection.

Parallel Processing: Optional PySpark integration for large datasets.

Data Lineage: Complete traceability of transformations and sources.

Metrics & KPIs: Automatic computation of business-critical metrics (Sales Growth, Cumulative Sales, etc.).

API Access: FastAPI endpoints for querying Gold tables and metrics.

Logging: Structured JSON logs for auditing, monitoring, and debugging.

BI-Ready: Compatible with Power BI, Tableau, or other visualization tools.

Fully Configurable: YAML-based central configuration for all pipeline parameters.

📂 Project Structure
project_hybrid_pipeline/
│
├─ config/
│   └─ config.yaml          # Central configuration (cubes, paths, SCD, logging)
│
├─ extract/
│   └─ extract_mdx.py       # Multi-cube MDX extraction
│
├─ transform/
│   ├─ bronze_transform.py  # Bronze layer
│   ├─ silver_transform.py  # Silver layer with validation & SCD2
│   ├─ gold_transform.py    # Gold layer with KPIs & metrics
│
├─ utils/
│   ├─ logger.py            # Structured JSON logging
│   ├─ data_quality.py      # 30+ data quality rules
│   └─ metrics.py           # KPI calculations
│
├─ api/
│   └─ api.py               # FastAPI endpoints for Gold layer access
│
├─ notebooks/
│   └─ exploration.ipynb    # Data exploration & lineage verification
│
└─ README.md

⚡ Installation
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux / Mac
venv\Scripts\activate     # Windows

# Install dependencies
pip install pandas pyarrow pyodbc fastapi uvicorn pyyaml
pip install pyspark  # Optional for parallelization

🏗 Pipeline Execution

Extract Data from OLAP Cubes

python extract/extract_mdx.py


Transform to Bronze Layer

python transform/bronze_transform.py


Transform to Silver Layer

python transform/silver_transform.py


Transform to Gold Layer (KPIs & Metrics)

python transform/gold_transform.py


Start FastAPI for Gold Data Access

uvicorn api.api:app --reload

🌐 API Endpoints

List Gold Tables:
GET /tables → Returns all available Gold tables.

Query Gold Table Sample:
GET /table/{table_name} → Returns first 50 rows of specified Gold table.

Get Metrics:
GET /metrics/{table_name} → Returns aggregated metrics (sum, average, KPIs).

📊 Data Quality & Logging

Logs stored in JSON format: logs/pipeline.json.

Tracks every stage: extraction, transformation, validation, KPIs, API queries.

Enforces 30+ data quality rules, including nulls, duplicates, and outliers.

🛠 Configuration

Centralized in config/config.yaml.

Define:

Cube connections (OLAP server, credentials)

Data lake paths (Bronze/Silver/Gold)

Partitioning and incremental load

Logging and SCD2 parameters

Parallelization (PySpark)

Power BI / Tableau integration

👥 Intended Audience

Data Engineers: Build enterprise-grade ETL/ELT pipelines.

Analytics / BI Teams: Consume validated, KPI-ready Gold tables.

Data Architects: Reference pipeline demonstrates scalable, maintainable architecture.

🏆 Highlights

Enterprise-grade, production-ready pipeline.

Supports millions of rows, multiple cubes, and parallel processing.

Full historical tracking (SCD2) and incremental updates.

FastAPI interface for dashboards and API-driven analytics.

JSON structured logging and auditable transformations.

Demonstrates senior-level Python, architecture, and data engineering skills.
