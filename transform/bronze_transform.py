"""
bronze_transform.py
---------------------
Advanced transformation pipeline for the Bronze layer.
- Cleans, validates, and enriches raw data from MDX extraction.
- Applies data quality rules.
- Generates partitions for downstream layers.
- Adds metadata for auditing and lineage.
- Saves data as Parquet files in Bronze layer.
- Handles multiple cubes.
- Includes structured logging and error handling.

Author: Cauam Pavonne
"""

import pandas as pd
import os
import glob
import datetime
from utils.logger import log_event
from utils.data_quality import validate_nulls, validate_duplicates, validate_ranges

# -------------------------------
# Configuration
# -------------------------------
BRONZE_PATH = "./data/bronze/"
VALIDATION_REPORT_PATH = "./logs/bronze_validation/"
PARTITION_COLUMN = "partition_month"

# Optional: List of columns to enforce types
COLUMN_TYPES = {
    "Sales": "float",
    "Quantity": "int",
    "Date": "datetime64[ns]",
    "Region": "string",
    "Country": "string",
    "City": "string",
    "Product": "string",
}

# -------------------------------
# Helper Functions
# -------------------------------

def read_bronze_files(path=BRONZE_PATH):
    """
    Read all parquet files in Bronze directory into a dictionary of DataFrames.
    """
    files = glob.glob(os.path.join(path, "*.parquet"))
    data = {}
    for f in files:
        cube_name = os.path.basename(f).replace("_bronze.parquet", "")
        try:
            df = pd.read_parquet(f)
            data[cube_name] = df
            log_event("bronze_read", f"Read {len(df)} rows from {f}")
        except Exception as e:
            log_event("bronze_read_error", f"Failed reading file {f}", {"error": str(e)})
    return data

def enforce_column_types(df: pd.DataFrame):
    """
    Enforce predefined column types. Handles missing columns gracefully.
    """
    for col, dtype in COLUMN_TYPES.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except Exception as e:
                log_event("type_cast_error", f"Failed to cast column {col} to {dtype}", {"error": str(e)})
    return df

def add_metadata_columns(df: pd.DataFrame, cube_name: str):
    """
    Add auditing and lineage metadata.
    """
    df["cube_name"] = cube_name
    df["bronze_extraction_time"] = datetime.datetime.now()
    if PARTITION_COLUMN not in df.columns:
        if "Date" in df.columns:
            df[PARTITION_COLUMN] = pd.to_datetime(df["Date"]).dt.to_period('M')
        else:
            df[PARTITION_COLUMN] = pd.Period(datetime.datetime.today().strftime("%Y-%m"), freq='M')
    return df

def run_data_quality(df: pd.DataFrame, cube_name: str):
    """
    Run multiple data quality checks.
    """
    issues = {}
    # Nulls
    null_report = validate_nulls(df)
    if null_report:
        issues["nulls"] = null_report

    # Duplicates
    duplicates_report = validate_duplicates(df)
    if duplicates_report:
        issues["duplicates"] = duplicates_report

    # Range checks (for numeric columns)
    ranges_report = validate_ranges(df)
    if ranges_report:
        issues["range_violations"] = ranges_report

    if issues:
        log_event("data_quality_issues", f"Cube {cube_name} failed validation", issues)
    else:
        log_event("data_quality_pass", f"Cube {cube_name} passed all validation checks")
    return df

def save_bronze(df: pd.DataFrame, cube_name: str):
    """
    Save the cleaned Bronze data as Parquet.
    """
    os.makedirs(BRONZE_PATH, exist_ok=True)
    file_path = os.path.join(BRONZE_PATH, f"{cube_name}_bronze.parquet")
    df.to_parquet(file_path, engine='pyarrow', index=False)
    log_event("bronze_save", f"Saved {len(df)} rows for cube {cube_name}", {"file_path": file_path})

# -------------------------------
# Main Transformation Function
# -------------------------------

def transform_bronze_layer():
    """
    Main function to process all Bronze files.
    """
    log_event("pipeline_start", "Starting Bronze transformation pipeline")
    all_cubes = read_bronze_files()
    for cube_name, df in all_cubes.items():
        try:
            # Enforce column types
            df = enforce_column_types(df)
            log_event("type_enforce", f"Enforced column types for cube {cube_name}")

            # Add metadata
            df = add_metadata_columns(df, cube_name)
            log_event("metadata_added", f"Added metadata columns for cube {cube_name}")

            # Run data quality validations
            df = run_data_quality(df, cube_name)

            # Save back to Bronze
            save_bronze(df, cube_name)
        except Exception as e:
            log_event("transform_error", f"Failed transformation for cube {cube_name}", {"error": str(e)})

    log_event("pipeline_end", "Bronze transformation pipeline completed successfully")

# -------------------------------
# Execution
# -------------------------------

if __name__ == "__main__":
    transform_bronze_layer()
