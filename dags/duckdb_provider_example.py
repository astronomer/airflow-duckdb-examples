"""
### Use the DuckDB provider to connect a DuckDB database

This toy DAG shows how to use the DuckDBHook to connect to a local DuckDB database 
and a database in MotherDuck via an Airflow connection. You will have to create two 
Airflow connections to DuckDB in order to use this DAG.
"""

from airflow.decorators import dag, task
from pendulum import datetime
import os
from duckdb_provider.hooks.duckdb_hook import DuckDBHook

CSV_PATH = "include/ducks.csv"
MOTHERDUCK_CONN_ID = "my_motherduck_conn"
MOTHERDUCK_TABLE_NAME = "my_motherduck_table"
LOCAL_DUCKDB_CONN_ID = "my_local_duckdb_conn"
LOCAL_DUCKDB_TABLE_NAME = "my_local_duckdb_table"


@dag(start_date=datetime(2023, 6, 1), schedule=None, catchup=False)
def duckdb_provider_example():
    @task
    def query_local_duckdb(my_table):
        my_duck_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
        conn = my_duck_hook.get_conn()

        r = conn.execute(f"SELECT * FROM {my_table};").fetchall()
        print(r)

        return r

    @task
    def query_motherduck(my_table):
        my_duck_hook = DuckDBHook.get_hook(MOTHERDUCK_CONN_ID)
        conn = my_duck_hook.get_conn()

        r = conn.execute(f"SELECT * FROM {my_table};").fetchall()
        print(r)

        return r

    query_local_duckdb(my_table=LOCAL_DUCKDB_TABLE_NAME)
    query_motherduck(my_table=MOTHERDUCK_TABLE_NAME)


duckdb_provider_example()
