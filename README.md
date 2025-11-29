# Hybrid OLAP → Lakehouse & Data Warehouse ETL Pipeline

This project implements a robust, enterprise-grade ETL pipeline that extracts data from OLAP cubes, transforms it through multiple layers, and delivers Gold-level datasets ready for analytics and BI dashboards such as Power BI and Tableau. The pipeline is structured in three layers: Bronze for raw data extraction, Silver for cleaned and normalized data with historical tracking (SCD Type 1 & Type 2), and Gold for aggregated, KPI-enriched tables ready for analytics.

The pipeline features MDX cube extraction capable of handling multiple cubes simultaneously with 20+ columns, hierarchies, and measures. Data is transformed through Bronze, Silver, and Gold layers, enriched with dimensions like DimDate, DimRegion, and DimProduct, and aggregated into meaningful KPIs. Data quality validations include null checks, duplicate detection, and numeric range validations, ensuring enterprise-level data integrity. The Gold layer computes metrics such as totals, averages, max, min, cumulative, rolling averages, and derived metrics like SalesPerUnit. Logging and auditing are implemented via structured JSON logs capturing ETL steps, data quality results, and KPI calculations.

The project structure is organized as follows:

├── config/
│ └── config.yaml # Global configuration for paths, cubes, partitions, and KPIs
├── data/
│ ├── bronze/ # Raw data extracted from cubes
│ ├── silver/ # Cleaned and normalized data
│ └── gold/ # Aggregated datasets for BI
├── transform/
│ ├── bronze_extract.py # Cube extraction logic
│ ├── bronze_api.py # API integration for extraction
│ ├── bronze_transform.py # Initial transformation to Bronze layer
│ ├── silver_transform.py # Silver layer transformations and SCD
│ └── gold_transform.py # Gold layer aggregations and KPIs
├── utils/
│ ├── logger.py # Structured logging utilities
│ ├── data_quality.py # Data validation functions
│ └── metrics.py # KPI and metric calculations
└── README.md


To install, clone the repository and install dependencies:

```bash
git clone https://github.com/yourusername/enterprise-etl-pipeline.git
cd enterprise-etl-pipeline
pip install -r requirements.txt
Update config/config.yaml with paths, cube credentials, partitions, KPIs, and other settings before running the pipeline. The pipeline can be executed layer by layer: first run Bronze extraction with bronze_extract.py and bronze_api.py, then transform Bronze to Silver using silver_transform.py, and finally generate Gold tables using gold_transform.py.

Data quality is continuously validated with null checks per column, duplicate detection, and numeric range validations. Advanced versions can include outlier detection. All validation and ETL steps are logged in JSON format to logs/pipeline.log and console, capturing timestamp, event type, messages, and optional metadata.

KPIs and metrics include totals, averages, min, max, cumulative sums, rolling averages, and derived metrics such as SalesPerUnit. The pipeline supports multi-cube aggregation and is designed to be configuration-driven, making it scalable and adaptable to multiple environments and cubes.

For contribution, fork the repository, create feature branches, and submit pull requests with clear descriptions. This project is authored by Cauam Pavonne, specializing in enterprise ETL, data engineering, and analytics pipelines. 
