import pyodbc
import pandas as pd
import yaml
from utils.logger import log_event

with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

def connect_mdx(cube_config: dict):
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={cube_config['server']};"
        f"DATABASE={cube_config['database']};"
        f"UID={cube_config['username']};"
        f"PWD={cube_config['password']}"
    )
    conn = pyodbc.connect(conn_str)
    log_event("connection", f"Connected to cube {cube_config['name']}")
    return conn

def extract_cube(cube_config: dict) -> pd.DataFrame:
    conn = connect_mdx(cube_config)
    df = pd.read_sql(cube_config['query'], conn)
    conn.close()
    log_event("extraction", f"Extracted {len(df)} rows from {cube_config['name']}")
    return df

def extract_all_cubes():
    dfs = {}
    for cube in config['mdx']['cubes']:
        dfs[cube['name']] = extract_cube(cube)
    return dfs

if __name__ == "__main__":
    dataframes = extract_all_cubes()
    for name, df in dataframes.items():
        print(df.head())
