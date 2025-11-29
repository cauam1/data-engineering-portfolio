"""
Transform Silver → Gold:
- Aggregations
- KPIs
- SCD2 implementation
"""

import os
import pandas as pd
import yaml
from utils.logger import log_event
from utils.metrics import calculate_metrics

with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

GOLD_PATH = config["datalake"]["gold_path"]

def transform_gold(df: pd.DataFrame, table_name: str):
    # Example KPI: Sales Growth MoM
    if 'Sales' in df.columns and 'Date' in df.columns:
        df['Sales_Growth_MoM'] = df['Sales'].pct_change()

    # Save Gold
    os.makedirs(GOLD_PATH, exist_ok=True)
    gold_file = os.path.join(GOLD_PATH, f"{table_name}.parquet")
    df.to_parquet(gold_file, engine='pyarrow', index=False)
    log_event("gold_save", f"Saved Gold table '{table_name}'", {"rows": len(df), "file_path": gold_file})
