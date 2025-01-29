# from duckdb import read_csv
# from duckdb.duckdb import DuckDBPyRelation
import duckdb

# import pandas as pd
# import duckdb

# using loadtxt()
# arr = pd.read_csv(contacts_csv)
# print(arr)
# df.select_dtypes(include=['datetime64'])


def duck_read(csv_path: str) -> duckdb.duckdb.DuckDBPyRelation:
    ddb = duckdb.read_csv(csv_path)
    return ddb


# import duckdb
# import pandas as pd
# pandas_df = pd.DataFrame({"a": [42]})
# duckdb.sql("SELECT * FROM pandas_df")
