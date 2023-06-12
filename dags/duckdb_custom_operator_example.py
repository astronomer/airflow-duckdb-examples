"""
### Use a custom ExcelToDuckDBOperator

"""

from airflow.decorators import dag
from pendulum import datetime
from include.custom_operators.duckdb_operator import ExcelToDuckDBOperator


@dag(start_date=datetime(2023, 6, 1), schedule=None, catchup=False)
def duckdb_custom_operator_example():
    ExcelToDuckDBOperator(
        task_id="excel_to_duckdb",
        table_name="ducks_in_the_pond",
        excel_path="include/ducks_in_the_pond.xlsx",
        sheet_name="Sheet 1",
        duckdb_conn_id="my_motherduck_conn",
    )


duckdb_custom_operator_example()
