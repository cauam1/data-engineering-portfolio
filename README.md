Hybrid OLAP → Lakehouse + Warehouse Pipeline
Project Overview

This project is a highly advanced data pipeline designed for enterprise-level OLAP and data warehouse environments. It extracts data from multiple MDX cubes, transforms it through Bronze → Silver → Gold layers, implements SCD Type 2, applies full data quality checks, calculates KPIs, and exposes the data via a FastAPI layer for BI integration.

This pipeline is production-ready, highly modular, and follows best practices for data engineering, analytics, and architecture-level design.

Key Features

Multi-Cube Extraction: Supports multiple OLAP cubes with dynamic schema detection.

Layered Architecture:

Bronze: Raw, minimally cleaned parquet files.

Silver: Cleaned, validated, SCD1/SCD2 implemented.

Gold: Aggregated tables, metrics, KPIs.

SCD Type 2: Full historical tracking of data changes.

Delta-Like Incremental Load: Only updated/new rows are processed.

Data Quality & Validation: 30+ rules including nulls, duplicates, and anomaly detection.

Parallelization: PySpark or pandas multiprocessing support for large-scale data.

Data Lineage Tracking: Track origin, transformations, and version of all records.

Derived Metrics & KPIs: Automatically computed key business metrics.

API Access: FastAPI endpoints to query tables and metrics.

Logging: Structured JSON logs for monitoring and audit.

BI Integration: Ready for Power BI, Tableau, or other visualization tools.

Configurable: YAML-based configuration for servers, partitions, and pipeline parameters.

Project Structure
project_hybrid_pipeline/
│
├─ config/
│   └─ config.yaml          # Central pipeline configuration
│
├─ extract/
│   ├─ extract_mdx.py       # Multi-cube MDX extraction
│
├─ transform/
│   ├─ bronze_transform.py  # Bronze layer transformations
│   ├─ silver_transform.py  # Silver layer with validation & SCD2
│   ├─ gold_transform.py    # Gold layer with metrics & KPIs
│
├─ utils/
│   ├─ logger.py            # Structured logging in JSON
│   ├─ data_quality.py      # Data quality and validation rules
│   └─ metrics.py           # KPI and derived metric calculations
│
├─ api/
│   └─ api.py               # FastAPI endpoints for Gold data access
│
├─ notebooks/
│   └─ exploration.ipynb    # Data exploration & lineage checks
│
└─ README.md

Installation & Requirements
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Install dependencies
pip install pandas pyarrow pyodbc fastapi uvicorn pyyaml
pip install pyspark  # Optional for parallelization

Running the Pipeline

Extract data from cubes:

python extract/extract_mdx.py


Transform to Bronze layer:

python transform/bronze_transform.py


Transform to Silver layer:

python transform/silver_transform.py


Transform to Gold layer & compute KPIs:

python transform/gold_transform.py


Start API to query Gold data:

uvicorn api.api:app --reload

API Endpoints

List available Gold tables:
GET /tables

Query Gold table sample (first 50 rows):
GET /table/{table_name}

Get metrics for table (sum/avg Sales, etc.):
GET /metrics/{table_name}

Data Quality & Logging

All processing is logged in JSON structured logs (logs/pipeline.json).

Logs include extraction, transformation, validation, metrics calculation, and API queries.

30+ data quality rules enforce integrity at every stage.

Configuration

All pipeline parameters are in config/config.yaml.

Configure cube connections, SQL Server, paths, partitions, logging, SCD2, PySpark parallelism, and Power BI integration.

Intended Users

Data Engineers building scalable ETL/ELT pipelines.

Analytics & BI Teams needing clean, validated, and metric-ready data.

Enterprise Architects wanting fully documented and auditable pipelines.

Highlights

Fully modular, production-ready architecture.

Supports millions of rows and multiple cubes in parallel.

Ready for enterprise-grade BI dashboards.

Complete lineage, incremental load, SCD2, KPIs, and API access.

Example of enterprise-level Python pipeline, ideal for portfolio showcasing senior / architect skills.
