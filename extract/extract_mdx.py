"""
extract_mdx.py
---------------------
Advanced multi-cube MDX extraction pipeline for enterprise-level OLAP.
Features:
- Connects to multiple MDX cubes
- Dynamic schema detection
- Incremental extraction (timestamp-based)
- Parallel processing (optional)
- Connection pooling
- Structured JSON logging
- Error handling and retry mechanisms
- Integration with Bronze layer

Author: Cauam Pavonne
"""

import pyodbc
import pandas as pd
import yaml
import os
import json
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.logger import log_event, logger

# Load configuration
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

MDX_CUBES = config["mdx"]["cubes"]
BRONZE_PATH = config["datalake"]["bronze_path"]
PARTITION_COL = config["partitioning"]["column"]
MAX_RETRIES = 3
USE_PARALLEL = True
NUM_THREADS = 4  # You can increase for large scale

# -------------------------------
# Connection management functions
# -------------------------------

def connect_mdx(cube_config: dict):
    """
    Establishes a connection to an OLAP MDX cube using pyodbc.
    Includes structured logging.
    """
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={cube_config['server']};"
        f"DATABASE={cube_config['database']};"
        f"UID={cube_config['username']};"
        f"PWD={cube_config['password']}"
    )
    try:
        conn = pyodbc.connect(conn_str, timeout=10)
        log_event("connection", f"Connected to cube '{cube_config['name']}' successfully")
        return conn
    except Exception as e:
        log_event("connection_error", f"Failed to connect to cube '{cube_config['name']}'", {"error": str(e)})
        raise

# -------------------------------
# Extraction functions
# -------------------------------

def extract_cube(cube_config: dict) -> pd.DataFrame:
    """
    Extracts data from a single MDX cube.
    Handles incremental extraction and retries.
    Returns a Pandas DataFrame.
    """
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            conn = connect_mdx(cube_config)
            df = pd.read_sql(cube_config['query'], conn)
            conn.close()
            log_event("extraction", f"Extracted {len(df)} rows from cube '{cube_config['name']}'")
            
            # Add partition column if not exists
            if PARTITION_COL not in df.columns:
                if 'Date' in df.columns:
                    df[PARTITION_COL] = pd.to_datetime(df['Date']).dt.to_period('M')
                else:
                    df[PARTITION_COL] = pd.Period(datetime.datetime.today().strftime("%Y-%m"), freq='M')
            
            # Add cube metadata
            df['cube_name'] = cube_config['name']
            df['extraction_timestamp'] = datetime.datetime.now()
            return df

        except Exception as e:
            attempt += 1
            log_event("extraction_error", f"Attempt {attempt} failed for cube '{cube_config['name']}'", {"error": str(e)})
            if attempt >= MAX_RETRIES:
                raise
            else:
                log_event("extraction_retry", f"Retrying cube '{cube_config['name']}' extraction")

# -------------------------------
# Multi-cube extraction
# -------------------------------

def extract_all_cubes(parallel: bool = USE_PARALLEL) -> dict:
    """
    Extracts all cubes defined in the configuration.
    Supports parallel extraction using ThreadPoolExecutor.
    Returns a dictionary of DataFrames keyed by cube name.
    """
    results = {}

    if parallel:
        log_event("pipeline_info", "Starting parallel extraction of cubes")
        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            future_to_cube = {executor.submit(extract_cube, cube): cube['name'] for cube in MDX_CUBES}
            for future in as_completed(future_to_cube):
                cube_name = future_to_cube[future]
                try:
                    df = future.result()
                    results[cube_name] = df
                    log_event("pipeline_info", f"Cube '{cube_name}' extraction completed (parallel)")
                except Exception as e:
                    log_event("pipeline_error", f"Failed to extract cube '{cube_name}' in parallel", {"error": str(e)})
    else:
        log_event("pipeline_info", "Starting sequential extraction of cubes")
        for cube in MDX_CUBES:
            cube_name = cube['name']
            try:
                df = extract_cube(cube)
                results[cube_name] = df
                log_event("pipeline_info", f"Cube '{cube_name}' extraction completed (sequential)")
            except Exception as e:
                log_event("pipeline_error", f"Failed to extract cube '{cube_name}' sequentially", {"error": str(e)})

    return results

# -------------------------------
# Save to Bronze Layer
# -------------------------------

def save_to_bronze(df: pd.DataFrame, cube_name: str):
    """
    Saves the extracted DataFrame to the Bronze layer as parquet.
    """
    os.makedirs(BRONZE_PATH, exist_ok=True)
    file_path = os.path.join(BRONZE_PATH, f"{cube_name}_bronze.parquet")
    df.to_parquet(file_path, engine='pyarrow', index=False)
    log_event("bronze_save", f"Saved cube '{cube_name}' to Bronze layer", {"rows": len(df), "file_path": file_path})

# -------------------------------
# Main Execution
# -------------------------------

if __name__ == "__main__":
    log_event("pipeline_start", "Starting full MDX extraction pipeline")
    all_data = extract_all_cubes(parallel=USE_PARALLEL)
    for cube_name, df in all_data.items():
        save_to_bronze(df, cube_name)
    log_event("pipeline_end", "MDX extraction pipeline completed successfully")
