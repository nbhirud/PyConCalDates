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
    try:
        con = duckdb.connect(database=":memory:")  # in-memory db
        logging.info("Connected successfully to in-memory DuckDB")
        return con
    except Exception:
        logging.exception("Connection to in-memory DuckDB unsuccessful")
        return None


def duck_read(csv_path: str):  # -> duckdb.duckdb.DuckDBPyRelation:
    # con = get_duck_connection()
    ddb = duckdb.from_csv_auto(csv_path)
    # ddb.to_table(DUCKDB_FILEPATH)
    return ddb


def duck_csv_to_db(csv_path: str):
    with duckdb.connect(DUCKDB_FILEPATH) as conn:
        conn.sql(
            """
            CREATE OR REPLACE TABLE contacts AS
            SELECT * FROM 'data/contacts.csv';
        """
        )

        # conn.execute("CREATE OR REPLACE TABLE contacts AS SELECT * FROM $csv_path;", {"csv_path": csv_path})
        # conn.execute("CREATE OR REPLACE TABLE contacts AS SELECT * FROM '?';", [csv_path])
        # con.execute("SELECT item FROM items WHERE value > ?", [400])
        # print(con.fetchall())
        conn.commit()


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
