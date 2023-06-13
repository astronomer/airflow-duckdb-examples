"""
### Use DuckDB with the Astro Python SDK

This DAG shows a simple Astro Python SDK pipeline which uses DuckDB to store data ingested
from a CSV file and runs two simple transformations on the data. The DAG also
creates an Airflow pool for DuckDB with one slot in case one does not exists yet in order to prevent 
the tasks from trying to write to the same DuckDB database at the same time.
"""

from airflow.decorators import dag
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table
from pendulum import datetime
import pandas as pd
from airflow.operators.bash import BashOperator

CSV_PATH = "include/ducks.csv"
DUCKDB_CONN_ID = "my_duckdb_conn"
DUCKDB_POOL_NAME = "duckdb_pool"


@aql.transform(pool=DUCKDB_POOL_NAME)
def count_ducks(in_table):
    return "SELECT count(*) FROM {{ in_table }}"


@aql.dataframe(pool=DUCKDB_POOL_NAME)
def select_ducks(df: pd.DataFrame):
    three_random_ducks = df.sample(n=3, replace=False)
    print(three_random_ducks)
    return three_random_ducks


@dag(start_date=datetime(2023, 6, 1), schedule=None, catchup=False)
def duckdb_and_astro_sdk_example():
    create_duckdb_pool = BashOperator(
        task_id="create_duckdb_pool",
        bash_command=f"airflow pools list | grep -q '{DUCKDB_POOL_NAME}' || airflow pools set {DUCKDB_POOL_NAME} 1 'Pool for duckdb'",
    )

    load_ducks = aql.load_file(
        File(CSV_PATH), output_table=Table(conn_id=DUCKDB_CONN_ID)
    )

    create_duckdb_pool >> load_ducks

    count_ducks(load_ducks)
    select_ducks(
        load_ducks,
        output_table=Table(conn_id=DUCKDB_CONN_ID, name="three_random_ducks"),
    )


duckdb_and_astro_sdk_example()
