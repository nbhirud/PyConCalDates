# from duckdb import read_csv
# from duckdb.duckdb import DuckDBPyRelation
import duckdb
import logging
from pathlib import Path


# import pandas as pd

# from duck_constants import DUCKDB_FILEPATH
# DUCKDB_FILEPATH = "data/contacts.ddb"

# using loadtxt()
# arr = pd.read_csv(contacts_csv)
# print(arr)
# df.select_dtypes(include=['datetime64'])

# TODO - dbckdb singleton connection


def get_duck_connection() -> duckdb.DuckDBPyConnection | None:
    """Create and return an in-memory DuckDB connection"""
    try:
        # btw duckdb.connect() would also create in-memory db
        con = duckdb.connect(database=":memory:")  # in-memory db
        logging.info("Connected successfully to in-memory DuckDB")
        return con
    except Exception as ex:
        logging.exception(f"Connection to in-memory DuckDB unsuccessful. {ex}")
        return None


def duck_read(
    con: duckdb.DuckDBPyConnection, csv_path: Path
):  # -> duckdb.duckdb.DuckDBPyRelation:
    """Read csv and load into the in-memory DuckDB"""
    try:
        # ddb = duckdb.from_csv_auto(csv_path)
        # ddb.to_table(DUCKDB_FILEPATH)
        con.execute(
            f"CREATE TABLE contacts AS SELECT * FROM read_csv_auto('{csv_path}')"
        )
        logging.info("csv read and loaded into in-memory DuckDB successfully")
    except Exception as ex:
        logging.exception(f"loading csv into db failed. {ex}")


# def duck_csv_to_db(csv_path: Path):
#     with duckdb.connect(DUCKDB_FILEPATH) as conn:
#         conn.sql(
#             """
#             CREATE OR REPLACE TABLE contacts AS
#             SELECT * FROM 'data/contacts.csv';
#         """
#         )

#         # conn.execute("CREATE OR REPLACE TABLE contacts AS SELECT * FROM $csv_path;", {"csv_path": csv_path})
#         # conn.execute("CREATE OR REPLACE TABLE contacts AS SELECT * FROM '?';", [csv_path])
#         # con.execute("SELECT item FROM items WHERE value > ?", [400])
#         # print(con.fetchall())
#         conn.commit()


# FIXME - mypy error: Name "duckdb.duckdb.DuckDBPyRelation" is not defined  [name-defined]
def duck_run_query(
    con: duckdb.DuckDBPyConnection, sql_query: str
) -> duckdb.duckdb.DuckDBPyRelation | None:
    """Run the requested query on DuckDB and return the result"""
    try:
        logging.info(f"query executed successfully: {sql_query}")
        return con.sql(sql_query)

    except Exception as ex:
        logging.exception(f"Failed to run the query on DuckDB. {ex}")
        return None


# def duck_to_csv(con: duckdb.DuckDBPyConnection) -> str | None:
#     """Create new csv with all data in DuckDB contacts table"""
#     try:
#         date_time = datetime.now().strftime("%Y%m%d_%H%M%S")
#         con.execute("")

#     except Exception as ex:
#         logging.exception(f"Failed to export DuckDB contacts to csv. {ex}")
#         return None
