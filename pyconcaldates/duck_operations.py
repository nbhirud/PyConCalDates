# from duckdb import read_csv
# from duckdb.duckdb import DuckDBPyRelation
import duckdb
import logging

# import pandas as pd

# from duck_constants import DUCKDB_FILEPATH
DUCKDB_FILEPATH = "data/contacts.ddb"

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
    con: duckdb.DuckDBPyConnection, csv_path: str
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


# def duck_csv_to_db(csv_path: str):
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


def duck_run_query(sql_query: str):
    result = None
    with duckdb.connect(DUCKDB_FILEPATH) as conn:
        result = conn.sql(sql_query)
    return result


# SELECT     "First Name",
#     "Middle Name",
#     "Last Name",
#     "Birthday",
#     "Event 1 - Label",
#     "Event 1 - Value",
#     "Custom Field 1 - Label",
#     "Custom Field 1 - Value"
#     FROM contacts
#     WHERE "Birthday" NOTNULL
#     OR "Event 1 - Label" NOTNULL
#     OR "Event 1 - Value" NOTNULL
#     OR "Custom Field 1 - Label" NOTNULL
#     OR "Custom Field 1 - Value" NOTNULL;


# def duck_csv_to_table(csv_path: str):
#     DUCKDB_FILEPATH
# CREATE TABLE new_tbl AS
# SELECT * FROM read_csv('input.csv');

# import duckdb
# import pandas as pd
# pandas_df = pd.DataFrame({"a": [42]})
# duckdb.sql("SELECT * FROM pandas_df")
