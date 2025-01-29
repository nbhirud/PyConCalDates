from pyconcaldates.duck_operations import duck_read

# from duckdb.duckdb import DuckDBPyRelation
import duckdb

# TODO - mypy (static type checker)
# TODO - ruff (linter, formatter)(instead of Flake8, isort, and Black - check if it can really do black's work)
# TODO - bandit (security checker?)


# Extract dates from contacts and add them to calendar
# GNU GENERAL PUBLIC LICENSE Version 3

contacts_csv: str = "data/contacts.csv"


ddb: duckdb.duckdb.DuckDBPyRelation = duck_read(contacts_csv)
print(type(ddb))
print(ddb)
