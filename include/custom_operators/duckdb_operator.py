from airflow.models.baseoperator import BaseOperator
from duckdb_provider.hooks.duckdb_hook import DuckDBHook


class ExcelToDuckDBOperator(BaseOperator):
    """
    Operator that loads an Excel file into a new table in a DuckDB database.
    :param duckdb_conn: Airflow connection to duckdb.
    :param table_name: Name of the table to create in DuckDB.
    :param excel_path: Path to the Excel file to load.
    :param sheet_name: Name of the sheet in the Excel file to load.
    """

    def __init__(
        self,
        table_name,
        excel_path,
        sheet_name: str = "Sheet1",
        duckdb_conn_id: str = "duckdb_default",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.excel_path = excel_path
        self.sheet_name = sheet_name
        self.duckdb_conn_id = duckdb_conn_id

    def execute(self, context):
        ts = context["ts"]
        duckdb_hook = DuckDBHook(duckdb_conn_id=self.duckdb_conn_id)
        duckdb_conn = duckdb_hook.get_conn()
        duckdb_conn.execute("install spatial;")
        duckdb_conn.execute("load spatial;")
        duckdb_conn.execute(
            f"""CREATE TABLE IF NOT EXISTS {self.table_name} AS 
            SELECT * FROM st_read('{self.excel_path}', layer='{self.sheet_name}');"""
        )
        self.log.info(f"{ts}: created table {self.table_name} from {self.excel_path}!")
        return self.table_name, self.excel_path

