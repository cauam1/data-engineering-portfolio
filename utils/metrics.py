"""
metrics.py
---------------------
Advanced metrics calculation utilities for ETL pipelines.
- Calculates KPIs: totals, averages, max, min
- Computes rolling and cumulative metrics
- Supports multi-cube aggregation
- Designed for Silver and Gold layers
- Returns dictionary of metrics for integration or logging

Author: Cauam Pavonne
"""

import pandas as pd

# -------------------------------
# KPI CALCULATION FUNCTIONS
# -------------------------------

def total(df: pd.DataFrame, column: str) -> float:
    """Compute total sum of a column"""
    if column in df.columns:
        return df[column].sum()
    return 0.0

def mean(df: pd.DataFrame, column: str) -> float:
    """Compute mean/average of a column"""
    if column in df.columns and len(df) > 0:
        return df[column].mean()
    return 0.0

def maximum(df: pd.DataFrame, column: str) -> float:
    """Compute maximum value of a column"""
    if column in df.columns and len(df) > 0:
        return df[column].max()
    return 0.0

def minimum(df: pd.DataFrame, column: str) -> float:
    """Compute minimum value of a column"""
    if column in df.columns and len(df) > 0:
        return df[column].min()
    return 0.0

def cumulative(df: pd.DataFrame, column: str, group_by: list = None) -> pd.Series:
    """
    Compute cumulative sum over a column.
    Optionally grouped by specific columns.
    """
    if column not in df.columns:
        return pd.Series([0]*len(df))
    if group_by:
        return df.groupby(group_by)[column].cumsum()
    return df[column].cumsum()

def rolling_average(df: pd.DataFrame, column: str, window: int = 3, group_by: list = None) -> pd.Series:
    """
    Compute rolling average over a column.
    Optionally grouped by specific columns.
    """
    if column not in df.columns:
        return pd.Series([0]*len(df))
    if group_by:
        return df.groupby(group_by)[column].transform(lambda x: x.rolling(window, min_periods=1).mean())
    return df[column].rolling(window, min_periods=1).mean()

# -------------------------------
# MAIN KPI FUNCTION
# -------------------------------

def calculate_kpis(df: pd.DataFrame) -> dict:
    """
    Calculate multiple KPIs for Gold/Silver layer.
    Returns a dictionary with all metrics.
    """
    metrics = {}
    
    # Example KPIs
    for col in ["Sales", "Quantity", "Stock"]:
        if col in df.columns:
            metrics[f"total_{col.lower()}"] = total(df, col)
            metrics[f"avg_{col.lower()}"] = mean(df, col)
            metrics[f"max_{col.lower()}"] = maximum(df, col)
            metrics[f"min_{col.lower()}"] = minimum(df, col)
            metrics[f"cumulative_{col.lower()}"] = cumulative(df, col).iloc[-1]  # last cumulative value
    
    # Rolling metrics example (3-period rolling)
    for col in ["Sales", "Quantity"]:
        if col in df.columns:
            metrics[f"rolling_avg_{col.lower()}"] = rolling_average(df, col, window=3).iloc[-1]  # last rolling avg

    return metrics

# -------------------------------
# EXAMPLE USAGE
# -------------------------------

if __name__ == "__main__":
    # Sample DataFrame
    df = pd.DataFrame({
        "Date": pd.date_range(start="2025-01-01", periods=5, freq="D"),
        "Sales": [100, 150, 200, 130, 170],
        "Quantity": [10, 15, 20, 13, 17],
        "Stock": [500, 480, 460, 450, 440]
    })
    
    kpis = calculate_kpis(df)
    print(kpis)
