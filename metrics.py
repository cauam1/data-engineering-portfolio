import pandas as pd

def calculate_metrics(df: pd.DataFrame):
    """
    Calculate derived metrics / KPIs
    """
    if 'Sales' in df.columns:
        df['Sales_Growth_MoM'] = df['Sales'].pct_change()
        df['Cumulative_Sales'] = df['Sales'].cumsum()
    return df
