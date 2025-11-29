from fastapi import FastAPI
import pandas as pd
import os
from fastapi.responses import JSONResponse

app = FastAPI(title="Hybrid Pipeline API")

GOLD_PATH = "./data/gold/"

@app.get("/tables")
def list_tables():
    tables = [f for f in os.listdir(GOLD_PATH) if f.endswith(".parquet")]
    return {"gold_tables": tables}

@app.get("/table/{table_name}")
def get_table(table_name: str):
    file_path = os.path.join(GOLD_PATH, f"{table_name}.parquet")
    if not os.path.exists(file_path):
        return JSONResponse(status_code=404, content={"error": "Table not found"})
    df = pd.read_parquet(file_path)
    return df.head(50).to_dict(orient='records')

@app.get("/metrics/{table_name}")
def get_metrics(table_name: str):
    file_path = os.path.join(GOLD_PATH, f"{table_name}.parquet")
    if not os.path.exists(file_path):
        return JSONResponse(status_code=404, content={"error": "Table not found"})
    df = pd.read_parquet(file_path)
    # Example: return sum and mean of Sales if exists
    metrics = {}
    if 'Sales' in df.columns:
        metrics['total_sales'] = df['Sales'].sum()
        metrics['avg_sales'] = df['Sales'].mean()
    return metrics
