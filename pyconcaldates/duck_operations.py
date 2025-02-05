# from duckdb import read_csv
# from duckdb.duckdb import DuckDBPyRelation
import duckdb
import logging
from datetime import datetime
from pathlib import Path


# import pandas as pd

# from duck_constants import DUCKDB_FILEPATH
# DUCKDB_FILEPATH = "data/contacts.ddb"

# using loadtxt()
# arr = pd.read_csv(contacts_csv)
# print(arr)
# df.select_dtypes(include=['datetime64'])


class ContactsDB:
    def __init__(self):
        try:
            self.con = duckdb.connect(database=":memory:")  # in-memory db
            logging.info("Connected successfully to in-memory DuckDB")

        except Exception as ex:
            logging.exception(f"Connection to in-memory DuckDB unsuccessful. {ex}")

    def duck_load_csv_inmemory(
        self, csv_path: Path
    ):  # -> duckdb.duckdb.DuckDBPyRelation:
        """Read csv and load into the in-memory DuckDB"""
        try:
            # ddb = duckdb.from_csv_auto(csv_path)
            # ddb.to_table(DUCKDB_FILEPATH)
            self.con.execute(
                f"CREATE TABLE contacts AS SELECT * FROM read_csv_auto('{csv_path}')"
            )
            logging.info("csv read and loaded into in-memory DuckDB successfully")
        except Exception as ex:
            logging.exception(f"loading csv into db failed. {ex}")

    def duck_run_query(self, sql_query: str) -> duckdb.duckdb.DuckDBPyRelation | None:
        """Run the requested query on DuckDB and return the result"""
        try:
            logging.info(f"query executed successfully: {sql_query}")
            return self.con.sql(sql_query)

        except Exception as ex:
            logging.exception(f"Failed to run the query on DuckDB. {ex}")
            return None

    def duck_to_csv(
        self, export_base_path: Path, export_filename: str | None = None
    ) -> Path | None:
        """Create new csv with all data in DuckDB contacts table"""
        try:
            output_path: Path | None = None
            if export_filename:
                output_path = export_base_path.joinpath(export_filename)
            else:
                date_time = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_path = export_base_path.joinpath(f"new_contacts_{date_time}.csv")

            self.con.execute(
                f"COPY (SELECT * FROM contacts) TO '{output_path}' (FORMAT CSV, HEADER TRUE)"
            )
            logging.info(f"contacts table exported to {output_path}")
            return output_path

        except Exception as ex:
            logging.exception(f"Failed to export DuckDB contacts to csv. {ex}")
            return None

    def duck_query_to_csv(self):
        pass


# # TODO - dbckdb singleton connection
# def get_duck_connection() -> duckdb.DuckDBPyConnection | None:
#     """Create and return an in-memory DuckDB connection"""
#     try:
#         # btw duckdb.connect() would also create in-memory db
#         con = duckdb.connect(database=":memory:")  # in-memory db
#         logging.info("Connected successfully to in-memory DuckDB")
#         return con
#     except Exception as ex:
#         logging.exception(f"Connection to in-memory DuckDB unsuccessful. {ex}")
#         return None


# def duck_load_csv_inmemory(
#     con: duckdb.DuckDBPyConnection, csv_path: Path
# ):  # -> duckdb.duckdb.DuckDBPyRelation:
#     """Read csv and load into the in-memory DuckDB"""
#     try:
#         # ddb = duckdb.from_csv_auto(csv_path)
#         # ddb.to_table(DUCKDB_FILEPATH)
#         con.execute(
#             f"CREATE TABLE contacts AS SELECT * FROM read_csv_auto('{csv_path}')"
#         )
#         logging.info("csv read and loaded into in-memory DuckDB successfully")
#     except Exception as ex:
#         logging.exception(f"loading csv into db failed. {ex}")


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


# # FIXME - mypy error: Name "duckdb.duckdb.DuckDBPyRelation" is not defined  [name-defined]
# def duck_run_query(
#     con: duckdb.DuckDBPyConnection, sql_query: str
# ) -> duckdb.duckdb.DuckDBPyRelation | None:
#     """Run the requested query on DuckDB and return the result"""
#     try:
#         logging.info(f"query executed successfully: {sql_query}")
#         return con.sql(sql_query)

#     except Exception as ex:
#         logging.exception(f"Failed to run the query on DuckDB. {ex}")
#         return None


# def duck_to_csv(
#     con: duckdb.DuckDBPyConnection,
#     export_base_path: Path,
#     export_filename: str | None = None,
# ) -> Path | None:
#     """Create new csv with all data in DuckDB contacts table"""
#     try:
#         output_path: Path | None = None
#         if export_filename:
#             output_path = export_base_path.joinpath(export_filename)
#         else:
#             date_time = datetime.now().strftime("%Y%m%d_%H%M%S")
#             output_path = export_base_path.joinpath(f"new_contacts_{date_time}.csv")

#         con.execute(
#             f"COPY (SELECT * FROM contacts) TO '{output_path}' (FORMAT CSV, HEADER TRUE)"
#         )
#         logging.info(f"contacts table exported to {output_path}")
#         return output_path

#     except Exception as ex:
#         logging.exception(f"Failed to export DuckDB contacts to csv. {ex}")
#         return None


# def duck_query_to_csv():
#     pass
