"""
### Use the custom ExcelToDuckDBOperator based upon the DuckDBHook

This simple DAG shows the custom ExcelToDuckDBOperator in use. The custom operator
can be found in include/custom_operators/duckdb_operator.py.
"""

from airflow.decorators import dag
from pendulum import datetime
from include.custom_operators.duckdb_operator import ExcelToDuckDBOperator

CONNECTION = "my_local_duckdb_conn"  # Set to your connection ID of a DuckDB or MotherDuck connection


@dag(start_date=datetime(2023, 6, 1), schedule=None, catchup=False)
def duckdb_custom_operator_example():
    ExcelToDuckDBOperator(
        task_id="excel_to_duckdb",
        table_name="ducks_in_the_pond",
        excel_path="include/ducks_in_the_pond.xlsx",
        sheet_name="Sheet 1",
        duckdb_conn_id=CONNECTION,
    )


duckdb_custom_operator_example()
