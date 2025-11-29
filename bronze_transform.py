import os
import pandas as pd
import yaml
from utils.logger import log_event

with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

BRONZE_PATH = config["datalake"]["bronze_path"]
PARTITION_COL = config["partitioning"]["column"]

def save_bronze(df: pd.DataFrame, table_name: str):
    os.makedirs(BRONZE_PATH, exist_ok=True)
    if PARTITION_COL not in df.columns:
        if 'Date' in df.columns:
            df[PARTITION_COL] = pd.to_datetime(df['Date']).dt.to_period('M')
        else:
            df[PARTITION_COL] = pd.Period('2025-11', freq='M')
    file_path = os.path.join(BRONZE_PATH, f"{table_name}.parquet")
    df.to_parquet(file_path, engine='pyarrow', index=False)
    log_event("bronze_save", f"Saved Bronze table '{table_name}'", {"rows": len(df), "file_path": file_path})
