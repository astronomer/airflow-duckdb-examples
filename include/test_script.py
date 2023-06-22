"""This is a short convenience script to test the connection to MotherDuck and
local DuckDB instances. Run 'astro dev bash -s' to test from within the scheduler container"""

import duckdb
import os

# to connect to motherduck you will need version 0.8.1 or higher
print(duckdb.__version__)
# !pip install duckdb==0.8.1

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
DATABASE_NAME = ""
TABLE_NAME = "duckdb_tables"

# MotherDuck connection
conn = duckdb.connect(f"motherduck:{DATABASE_NAME}?token={MOTHERDUCK_TOKEN}")
r = conn.sql(f"SELECT * FROM {TABLE_NAME}").fetchone()[0]

print(r)

# test a local duckdb connection
# LOCAL_DUCKDB_STORAGE_PATH = "my_local_ducks.db"
# TABLE_NAME = "duckdb_tables"
# conn = duckdb.connect(LOCAL_DUCKDB_STORAGE_PATH)
# r = conn.sql(f"SELECT * FROM {TABLE_NAME}").fetchone()[0]
# print(r)