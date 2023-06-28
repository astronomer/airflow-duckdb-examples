# Airflow DuckDB examples

Welcome :wave: !

This is a small toy repository for you to test different ways of connection [Airflow](https://airflow.apache.org/), [DuckDB](https://duckdb.org/) and [MotherDuck](https://motherduck.com/). :duck: 

> If you are new to Airflow consider checking out our [quickstart repository](https://github.com/astronomer/airflow-quickstart) and [Get started tutorial](https://docs.astronomer.io/learn/get-started-with-airflow).  

## How to use this repository

1. Make sure you have [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.
2. Install the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli).
3. Clone this repository.
4. Create a `.env` file with the contents of the provided `.env.example` file. If you are using MotherDuck, provide your MotherDuck token.
5. Start Airflow by running `astro dev start`.

6. In the Airflow UI define the following [Airflow connections](https://docs.astronomer.io/learn/connections):
    - `my_local_duckdb_conn` with the following parameters:
        - **Conn Type**: `duckdb`
        - **Path to local database file**: `include/my_garden_ducks.db`
    - `my_motherduck_conn` with the following parameters:
        - **Conn Type**: `duckdb`
        - **MotherDuck Service token**: your [MotherDuck Service token](https://motherduck.com/docs/authenticating-to-motherduck/)
        - **MotherDuck database name**: optionally specify a MotherDuck database name
    
    You can double check your connection credentials using the `include/test_script.py` script. To run the script inside of the Airflow scheduler container run `astro dev bash -s` and then `python include/test_script.py`.

7. Manually trigger DAGs by clicking the play button for each DAG on the right side of the screen.

## DAGs

This repo contains 4 DAGs showing different ways to interact with DuckDB and MotherDuck from within Airflow:

- `duckdb_in_taskflow`: This DAG uses the `duckdb` Python package directly to connect. Note that some tasks will fail if no MotherDuck token was provided.
- `duckdb_provider_example`: This DAG uses the DuckDBHook from the DuckDB Airflow provider to connect to DuckDB and MotherDuck.
- `duckdb_custom_operator_example`: This DAG uses the custom local operator `ExcelToDuckDBOperator` which is stored in `include/duckdb_operator.py` to load the contents of an Excel file (`include/ducks_in_the_pond`) into a DuckDB or MotherDuck database.
- `duckdb_and_astro_sdk_example`: This DAG uses the Astro SDK to connect to perform a simple ELT pipeline either a local DuckDB database or a MotherDuck database.

## See also

- [DuckDB Airflow provider](https://registry.astronomer.io/providers/airflow-provider-duckdb/versions/latest)
- [Airflow and DuckDB tutorial](https://docs.astronomer.io/learn/airflow-duckdb)
- [MotherDuck](https://motherduck.com/)
- [DuckDB](https://duckdb.org/)

