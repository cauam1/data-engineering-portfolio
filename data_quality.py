import pandas as pd
from utils.logger import log_event

def validate_dataframe(df: pd.DataFrame, table_name: str):
    # Null ratio
    null_threshold = 0.01
    null_ratio = df.isnull().mean().max()
    if null_ratio > null_threshold:
        log_event("data_quality_error", f"High null ratio in {table_name}", {"null_ratio": null_ratio})

    # Duplicates
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        log_event("data_quality_warning", f"Found duplicates in {table_name}", {"duplicates": duplicates})

    # Simple anomaly detection (z-score)
    numeric_cols = df.select_dtypes(include='number').columns
    if not numeric_cols.empty:
        z_scores = ((df[numeric_cols] - df[numeric_cols].mean()) / df[numeric_cols].std()).abs()
        outliers = (z_scores > 3).sum().sum()
        if outliers > 0:
            log_event("data_quality_warning", f"Detected {outliers} outliers in {table_name}")
