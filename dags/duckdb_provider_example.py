"""
### Use the DuckDB provider to connect a DuckDB database

This toy DAG shows how to use the DuckDBHook to connect to a local DuckDB database 
and a database in MotherDuck via an Airflow connection.
"""

from airflow.decorators import dag, task
from pendulum import datetime
import os
from duckdb_provider.hooks.duckdb_hook import DuckDBHook

CSV_PATH = "include/ducks.csv"
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
LOCAL_DUCKDB_STORAGE_PATH = "include/my_local_ducks.db"


@dag(start_date=datetime(2023, 6, 1), schedule=None, catchup=False)
def duckdb_provider_example():
    @task
    def query_local_duckdb():
        my_duck_hook = DuckDBHook.get_hook("my_duckdb_conn")
        conn = my_duck_hook.get_conn()

        r = conn.execute(f"SELECT * FROM persistent_duck_table;").fetchall()
        print(r)

        return r

    @task
    def query_motherduck():
        my_duck_hook = DuckDBHook.get_hook("my_motherduck_conn")
        conn = my_duck_hook.get_conn()

        r = conn.execute(f"SELECT * FROM ducks_garden;").fetchall()
        print(r)

        return r

    query_local_duckdb()
    query_motherduck()


duckdb_provider_example()
