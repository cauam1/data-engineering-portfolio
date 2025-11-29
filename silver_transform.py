import os
import pandas as pd
import yaml
from utils.logger import log_event
from utils.data_quality import validate_dataframe

with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

SILVER_PATH = config["datalake"]["silver_path"]

def transform_silver(df: pd.DataFrame, table_name: str):
    # Fill numeric nulls
    numeric_cols = df.select_dtypes(include='number').columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    # Validate dataframe
    validate_dataframe(df, table_name)
    # Save Silver
    os.makedirs(SILVER_PATH, exist_ok=True)
    silver_file = os.path.join(SILVER_PATH, f"{table_name}.parquet")
    df.to_parquet(silver_file, engine='pyarrow', index=False)
    log_event("silver_save", f"Saved Silver table '{table_name}'", {"rows": len(df), "file_path": silver_file})
