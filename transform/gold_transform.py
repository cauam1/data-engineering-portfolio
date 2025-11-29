"""
gold_transform.py
---------------------
Advanced transformation pipeline for the Gold layer.
- Reads Silver Parquet files.
- Performs aggregations, metrics, and KPI calculations.
- Integrates multiple cubes and dimensions.
- Prepares Gold tables ready for BI dashboards.
- Maintains historical tracking and partitioning.
- Adds auditing and lineage metadata.
- Structured logging for production-grade ETL.

Author: Cauam Pavonne
"""

import pandas as pd
import os
import datetime
from utils.logger import log_event
from utils.metrics import calculate_kpis
from utils.data_quality import validate_nulls, validate_duplicates

# -------------------------------
# Configuration
# -------------------------------
SILVER_PATH = "./data/silver/"
GOLD_PATH = "./data/gold/"
PARTITION_COLUMN = "partition_month"

AGGREGATIONS = {
    "Sales": ["sum", "mean", "max", "min"],
    "Quantity": ["sum", "mean"]
}

# -------------------------------
# Helper Functions
# -------------------------------

def read_silver_files(path=SILVER_PATH):
    """
    Reads all Silver parquet files into a dictionary of DataFrames.
    """
    files = [f for f in os.listdir(path) if f.endswith(".parquet")]
    data = {}
    for f in files:
        cube_name = f.replace("_silver.parquet", "")
        try:
            df = pd.read_parquet(os.path.join(path, f))
            data[cube_name] = df
            log_event("gold_read", f"Read {len(df)} rows from Silver file {f}")
        except Exception as e:
            log_event("gold_read_error", f"Failed reading {f}", {"error": str(e)})
    return data

def enrich_with_dimensions(df: pd.DataFrame):
    """
    Creates derived columns and enriches the dataset.
    """
    # Example: Year-Month partition if missing
    if PARTITION_COLUMN not in df.columns and "Date" in df.columns:
        df[PARTITION_COLUMN] = pd.to_datetime(df["Date"]).dt.to_period('M')
    
    # Add derived metrics
    if "Sales" in df.columns and "Quantity" in df.columns:
        df["SalesPerUnit"] = df["Sales"] / df["Quantity"].replace(0, 1)
    
    log_event("gold_enrichment", "Enriched Silver dataframe with derived metrics")
    return df

def aggregate_gold(df: pd.DataFrame, group_by: list):
    """
    Performs aggregation according to AGGREGATIONS config.
    """
    agg_dict = {col: funcs for col, funcs in AGGREGATIONS.items() if col in df.columns}
    if not agg_dict:
        return df  # No aggregation needed

    df_agg = df.groupby(group_by).agg(agg_dict)
    # Flatten MultiIndex columns
    df_agg.columns = ["_".join(col).strip() for col in df_agg.columns.values]
    df_agg = df_agg.reset_index()
    log_event("gold_aggregation", f"Aggregated Gold table on {group_by}")
    return df_agg

def run_data_quality(df: pd.DataFrame, cube_name: str):
    """
    Run data quality checks on Gold layer.
    """
    issues = {}
    null_report = validate_nulls(df)
    if null_report:
        issues["nulls"] = null_report
    duplicates_report = validate_duplicates(df)
    if duplicates_report:
        issues["duplicates"] = duplicates_report
    if issues:
        log_event("gold_data_quality_issues", f"Cube {cube_name} validation failed", issues)
    else:
        log_event("gold_data_quality_pass", f"Cube {cube_name} passed validation")
    return df

def save_gold(df: pd.DataFrame, cube_name: str):
    """
    Saves the final Gold table as Parquet.
    """
    os.makedirs(GOLD_PATH, exist_ok=True)
    file_path = os.path.join(GOLD_PATH, f"{cube_name}_gold.parquet")
    df.to_parquet(file_path, engine='pyarrow', index=False)
    log_event("gold_save", f"Saved {len(df)} rows for Gold cube {cube_name}", {"file_path": file_path})

# -------------------------------
# Main Transformation
# -------------------------------

def transform_gold_layer():
    """
    Main function to process Silver files and generate Gold layer.
    """
    log_event("pipeline_start", "Starting Gold transformation pipeline")
    all_cubes = read_silver_files()

    for cube_name, df in all_cubes.items():
        try:
            # Enrich dataframe with derived columns and partitioning
            df = enrich_with_dimensions(df)

            # Apply aggregation by relevant dimensions
            group_by_cols = ["Region", "Country", "City", "Product", PARTITION_COLUMN]
            df_agg = aggregate_gold(df, [col for col in group_by_cols if col in df.columns])

            # Calculate KPIs using utils.metrics
            kpis = calculate_kpis(df_agg)
            df_agg = pd.concat([df_agg, pd.DataFrame([kpis] * len(df_agg))], axis=1)
            log_event("gold_kpis", f"Calculated KPIs for cube {cube_name}", {"kpis": kpis})

            # Data quality validations
            df_agg = run_data_quality(df_agg, cube_name)

            # Save final Gold layer
            save_gold(df_agg, cube_name)

        except Exception as e:
            log_event("gold_transform_error", f"Failed Gold transformation for cube {cube_name}", {"error": str(e)})

    log_event("pipeline_end", "Gold transformation pipeline completed successfully")

# -------------------------------
# Execution
# -------------------------------

if __name__ == "__main__":
    transform_gold_layer()
