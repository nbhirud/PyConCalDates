from pyconcaldates.duck_operations import duck_read, get_duck_connection, duck_run_query

# from duckdb.duckdb import DuckDBPyRelation
import duckdb

# TODO - mypy (static type checker)
# TODO - ruff (linter, formatter)(instead of Flake8, isort, and Black - check if it can really do black's work)
# TODO - bandit (security checker?)


# Extract dates from contacts and add them to calendar
# GNU GENERAL PUBLIC LICENSE Version 3

csv_path: str = "data/contacts.csv"

# duck_csv_to_db(csv_path)

# ddb: duckdb.duckdb.DuckDBPyRelation = duck_read(csv_path)
# ddb = duckdb.duckdb.DuckDBPyRelation = duck_read_local_db()

con: duckdb.DuckDBPyConnection | None = get_duck_connection()
if con:
    duck_read(con, csv_path)

    # print(type(ddb))
    # print(ddb)
    # ddb.show()

    # print(ddb.types)
    # print(ddb.columns)

    # FIXME - mypy error: Name "duckdb.duckdb.DuckDBPyRelation" is not defined  [name-defined]
    # ddb: duckdb.duckdb.DuckDBPyRelation  = con.sql("""SELECT * FROM contacts;""")
    ddb = duck_run_query(con, """SELECT * FROM contacts;""")

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

    # ddb3: duckdb.duckdb.DuckDBPyRelation = ddb2.sql_query('SELECT * FROM ddb2 WHERE Birthday NOTNULL OR \"Event 1 - Value\" NOTNULL OR \"Custom Field 1 - Value\" NOTNULL')
    # ddb3: duckdb.duckdb.DuckDBPyRelation = ddb2.sql('SELECT * FROM ddb2 WHERE Birthday NOTNULL ')

    # ddb3: duckdb.duckdb.DuckDBPyRelation = ddb2.bool_and('Birthday')
    # ddb3.show()

    # print(ddb2.describe())
    # duckdb.sql("SELECT * FROM ddb2 WHERE ").show()
    # ddb2.filter('Birthday NOTNULL').filter('\"Event 1 - Value\" NOTNULL').filter('\"Custom Field 1 - Value\" NOTNULL').show()
