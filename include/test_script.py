import duckdb
import os

# to connect to motherduck you will need version 0.8.0 or higher
print(duckdb.__version__)

CSV_PATH = "ducks.csv"
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

conn = duckdb.connect(f"md:?token={MOTHERDUCK_TOKEN}")
conn.sql(f"CREATE TABLE duck_temp AS SELECT * FROM '{CSV_PATH}'")
duck_species_count = conn.sql("SELECT count(*) FROM duck_temp").fetchone()[0]
print(duck_species_count)