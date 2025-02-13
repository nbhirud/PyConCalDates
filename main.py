from pyconcaldates.duck_operations import ContactsDB

# from duckdb.duckdb import DuckDBPyRelation
import duckdb
from pathlib import Path

# TODO - mypy (static type checker)
# TODO - ruff (linter, formatter)(instead of Flake8, isort, and Black - check if it can really do black's work)
# TODO - bandit (security checker?)


# Extract dates from contacts and add them to calendar
# GNU GENERAL PUBLIC LICENSE Version 3
BASEPATH: Path = Path.cwd()
DATAPATH: Path = BASEPATH.joinpath("data")
MAINCSVPATH: Path = DATAPATH.joinpath(
    "contacts.csv"
)  # Take this as user input in future refinement
# data_path : str = "data/"
# csv_path: str = "data/contacts.csv"
# print(f"CSVPATH = {type(MAINCSVPATH)}")

# duck_csv_to_db(csv_path)

# ddb: duckdb.duckdb.DuckDBPyRelation = duck_read(csv_path)
# ddb = duckdb.duckdb.DuckDBPyRelation = duck_read_local_db()
contacts_db = ContactsDB()
# con: duckdb.DuckDBPyConnection | None = get_duck_connection()
if True:
    contacts_db.duck_load_csv_inmemory(MAINCSVPATH)
    # duck_to_csv(con, DATAPATH)

    # print(type(ddb))
    # print(ddb.types)
    # print(ddb.columns)

    # FIXME - mypy error: Name "duckdb.duckdb.DuckDBPyRelation" is not defined  [name-defined]
    # ddb: duckdb.duckdb.DuckDBPyRelation  = con.sql("""SELECT * FROM contacts;""")
    ddb: duckdb.duckdb.DuckDBPyRelation | None = contacts_db.duck_run_query(
        """SELECT * FROM contacts;"""
    )

    if ddb:
        # FIXME - mypy error: Name "duckdb.duckdb.DuckDBPyRelation" is not defined  [name-defined]
        ddb2: duckdb.duckdb.DuckDBPyRelation = ddb.select(
            "First Name",
            "Middle Name",
            "Last Name",
            "Birthday",
            "Event 1 - Label",
            "Event 1 - Value",
            "Custom Field 1 - Label",
            "Custom Field 1 - Value",
        )
        # ddb2.
        ddb2.show()

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

        # ddb3: duckdb.duckdb.DuckDBPyRelation = ddb2.sql_query('SELECT * FROM ddb2 WHERE Birthday NOTNULL OR \"Event 1 - Value\" NOTNULL OR \"Custom Field 1 - Value\" NOTNULL')
        # ddb3: duckdb.duckdb.DuckDBPyRelation = ddb2.sql('SELECT * FROM ddb2 WHERE Birthday NOTNULL ')

        # ddb3: duckdb.duckdb.DuckDBPyRelation = ddb2.bool_and('Birthday')
        # ddb3.show()

        # print(ddb2.describe())
        # duckdb.sql("SELECT * FROM ddb2 WHERE ").show()
        # ddb2.filter('Birthday NOTNULL').filter('\"Event 1 - Value\" NOTNULL').filter('\"Custom Field 1 - Value\" NOTNULL').show()

    contacts_db.con.close()
