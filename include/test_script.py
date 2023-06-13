"""This is a short convenience script to test the connection to MotherDuck and
local DuckDB instances."""

import duckdb
import os

# to connect to motherduck you will need version 0.8.0 or higher
print(duckdb.__version__)
# !pip install duckdb==0.8.0

CSV_PATH = "ducks.csv"
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
LOCAL_DUCKDB_STORAGE_PATH = "ultimate_duck"
TABLE_NAME = "duck_names"

#motherduck connection
#conn = duckdb.connect(f"md:?token={MOTHERDUCK_TOKEN}")
conn = duckdb.connect(LOCAL_DUCKDB_STORAGE_PATH)
r = conn.sql(f"SELECT * FROM {TABLE_NAME}").fetchone()[0]
print(r)