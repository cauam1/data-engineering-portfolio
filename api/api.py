"""
api.py
---------------------
Advanced FastAPI application for accessing Gold layer tables.
Features:
- List all Gold tables
- Query tables with filters, pagination, and column selection
- Retrieve KPIs and aggregated metrics
- Full JSON structured logging
- Basic token-based authentication
- Caching in memory for frequent queries
- Integration with pipeline logs for audit
- Ready for BI dashboards

Author: Cauam Pavonne
"""

from fastapi import FastAPI, HTTPException, Query, Depends, Header
from fastapi.responses import JSONResponse
import pandas as pd
import os
import datetime
from typing import List, Optional
from utils.logger import log_event

# -------------------------------
# Configuration
# -------------------------------

GOLD_PATH = "./data/gold/"
API_TOKEN = "securetoken123"  # Simple token-based auth for demo
CACHE = {}  # Simple in-memory cache
CACHE_TTL = 300  # seconds

# -------------------------------
# Authentication
# -------------------------------

def verify_token(x_api_key: str = Header(...)):
    if x_api_key != API_TOKEN:
        log_event("api_auth_failed", "Invalid API token", {"token": x_api_key})
        raise HTTPException(status_code=401, detail="Unauthorized")
    log_event("api_auth_success", "Valid API token used")

# -------------------------------
# Helper Functions
# -------------------------------

def get_gold_tables():
    """
    Returns all parquet files in the Gold folder.
    """
    tables = [f for f in os.listdir(GOLD_PATH) if f.endswith(".parquet")]
    return tables

def load_table(table_name: str) -> pd.DataFrame:
    """
    Load a table from the Gold layer with caching.
    """
    file_path = os.path.join(GOLD_PATH, f"{table_name}.parquet")
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    
    # Check cache
    now = datetime.datetime.now()
    if table_name in CACHE:
        df, timestamp = CACHE[table_name]
        if (now - timestamp).total_seconds() < CACHE_TTL:
            log_event("api_cache_hit", f"Table '{table_name}' served from cache")
            return df

    df = pd.read_parquet(file_path)
    CACHE[table_name] = (df, now)
    log_event("api_cache_miss", f"Table '{table_name}' loaded from Gold layer")
    return df

def calculate_metrics(df: pd.DataFrame) -> dict:
    """
    Compute KPIs for given DataFrame.
    """
    metrics = {}
    if "Sales" in df.columns:
        metrics["total_sales"] = df["Sales"].sum()
        metrics["avg_sales"] = df["Sales"].mean()
        metrics["max_sales"] = df["Sales"].max()
        metrics["min_sales"] = df["Sales"].min()
    if "Quantity" in df.columns:
        metrics["total_quantity"] = df["Quantity"].sum()
        metrics["avg_quantity"] = df["Quantity"].mean()
    return metrics

# -------------------------------
# FastAPI App Initialization
# -------------------------------

app = FastAPI(
    title="Gold Layer API",
    description="Advanced API for accessing Gold layer tables and KPIs",
    version="1.0.0"
)

# -------------------------------
# API Endpoints
# -------------------------------

@app.get("/tables", summary="List all Gold tables", tags=["Tables"])
def list_tables(api_key: str = Depends(verify_token)):
    tables = get_gold_tables()
    log_event("api_list_tables", f"{len(tables)} tables listed")
    return {"gold_tables": tables}

@app.get("/table/{table_name}", summary="Query a Gold table", tags=["Tables"])
def query_table(
    table_name: str,
    columns: Optional[List[str]] = Query(None, description="List of columns to return"),
    limit: int = Query(50, ge=1, le=1000, description="Limit rows returned"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    filters: Optional[str] = Query(None, description="Filter expression in Pandas query format"),
    api_key: str = Depends(verify_token)
):
    """
    Query a Gold table with optional:
    - Column selection
    - Pagination
    - Filtering
    """
    df = load_table(table_name)
    
    # Apply filters
    if filters:
        try:
            df = df.query(filters)
        except Exception as e:
            log_event("api_filter_error", f"Failed filter '{filters}' on table '{table_name}'", {"error": str(e)})
            raise HTTPException(status_code=400, detail=f"Invalid filter: {str(e)}")
    
    # Column selection
    if columns:
        missing_cols = [c for c in columns if c not in df.columns]
        if missing_cols:
            raise HTTPException(status_code=400, detail=f"Columns not found: {missing_cols}")
        df = df[columns]
    
    # Pagination
    df_paginated = df.iloc[offset: offset + limit]
    
    log_event("api_query_table", f"Table '{table_name}' queried", {"rows_returned": len(df_paginated)})
    return df_paginated.to_dict(orient="records")

@app.get("/metrics/{table_name}", summary="Retrieve KPIs for a table", tags=["Metrics"])
def get_metrics(
    table_name: str,
    api_key: str = Depends(verify_token)
):
    df = load_table(table_name)
    metrics = calculate_metrics(df)
    log_event("api_metrics", f"Metrics calculated for table '{table_name}'", {"metrics": metrics})
    return metrics

@app.get("/search", summary="Search across all tables", tags=["Search"])
def search_tables(
    column: str,
    value: str,
    limit: int = 100,
    api_key: str = Depends(verify_token)
):
    """
    Search for a value across all Gold tables in a specific column.
    Returns matches up to limit.
    """
    results = []
    tables = get_gold_tables()
    for table_name in tables:
        df = load_table(table_name)
        if column in df.columns:
            matches = df[df[column].astype(str).str.contains(value, case=False, na=False)]
            if not matches.empty:
                results.append({"table": table_name, "rows": matches.head(limit).to_dict(orient="records")})
    log_event("api_search", f"Searched column '{column}' for value '{value}'", {"tables_searched": len(tables)})
    return results

@app.get("/health", summary="Health check", tags=["System"])
def health_check():
    """
    Returns system health status.
    """
    status = {"status": "OK", "time": datetime.datetime.now().isoformat()}
    log_event("api_health_check", "Health check requested")
    return status

# -------------------------------
# Run API (if executed directly)
# -------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.api:app", host="0.0.0.0", port=8000, reload=True)
