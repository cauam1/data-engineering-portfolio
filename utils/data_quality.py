"""
data_quality.py
---------------------
Advanced Data Quality utilities for ETL pipeline.
- Validate null values
- Check for duplicates
- Validate numeric ranges
- Returns structured reports for logging
- Designed for Bronze, Silver, Gold layers

Author: Cauam Pavonne
"""

import pandas as pd

# -------------------------------
# NULL VALUE VALIDATION
# -------------------------------

def validate_nulls(df: pd.DataFrame, threshold: float = 0.0) -> dict:
    """
    Check for null values in each column.
    Returns a dictionary of columns exceeding the threshold.
    
    Args:
        df: pandas DataFrame to validate
        threshold: float, max allowed null ratio (0.05 = 5%)
    Returns:
        dict: {column_name: null_ratio}
    """
    null_report = {}
    for col in df.columns:
        null_ratio = df[col].isna().mean()
        if null_ratio > threshold:
            null_report[col] = round(null_ratio, 4)
    return null_report

# -------------------------------
# DUPLICATE RECORD VALIDATION
# -------------------------------

def validate_duplicates(df: pd.DataFrame, subset: list = None) -> dict:
    """
    Check for duplicate rows in the DataFrame.
    
    Args:
        df: pandas DataFrame to validate
        subset: list of columns to check duplicates on; if None, checks entire row
    Returns:
        dict: { "num_duplicates": int, "sample_rows": list of dicts }
    """
    if subset:
        dup_mask = df.duplicated(subset=subset)
    else:
        dup_mask = df.duplicated()
    num_duplicates = dup_mask.sum()
    report = {}
    if num_duplicates > 0:
        report["num_duplicates"] = int(num_duplicates)
        report["sample_rows"] = df[dup_mask].head(5).to_dict(orient="records")
    return report

# -------------------------------
# RANGE VALIDATION FOR NUMERIC COLUMNS
# -------------------------------

def validate_ranges(df: pd.DataFrame, ranges: dict = None) -> dict:
    """
    Validate numeric columns against expected ranges.
    
    Args:
        df: pandas DataFrame to validate
        ranges: dict with column names and min/max values
            Example: {"Sales": {"min":0, "max":100000}, "Quantity": {"min":0, "max":1000}}
    Returns:
        dict: {column_name: {"min_violation": int, "max_violation": int}}
    """
    if not ranges:
        return {}
    range_report = {}
    for col, limits in ranges.items():
        if col in df.columns:
            min_val = limits.get("min", float("-inf"))
            max_val = limits.get("max", float("inf"))
            min_violations = df[df[col] < min_val].shape[0]
            max_violations = df[df[col] > max_val].shape[0]
            if min_violations > 0 or max_violations > 0:
                range_report[col] = {
                    "min_violation": int(min_violations),
                    "max_violation": int(max_violations)
                }
    return range_report

# -------------------------------
# FULL DATA QUALITY CHECK
# -------------------------------

def full_data_quality_check(df: pd.DataFrame, null_threshold=0.0, ranges: dict = None, subset_duplicates: list = None) -> dict:
    """
    Runs all data quality validations and returns structured report.
    
    Args:
        df: pandas DataFrame
        null_threshold: max allowed null ratio per column
        ranges: dict of ranges for numeric columns
        subset_duplicates: list of columns to check duplicates
    Returns:
        dict: { "nulls": ..., "duplicates": ..., "range_violations": ... }
    """
    report = {}
    nulls = validate_nulls(df, threshold=null_threshold)
    if nulls:
        report["nulls"] = nulls
    duplicates = validate_duplicates(df, subset=subset_duplicates)
    if duplicates:
        report["duplicates"] = duplicates
    range_violations = validate_ranges(df, ranges=ranges)
    if range_violations:
        report["range_violations"] = range_violations
    return report
