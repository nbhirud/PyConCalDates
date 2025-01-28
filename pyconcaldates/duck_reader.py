
# import pandas as pd
import duckdb

# using loadtxt()
# arr = pd.read_csv(contacts_csv)
# print(arr)
# df.select_dtypes(include=['datetime64'])


def duck_read(csv_path: str):
    ddb = duckdb.read_csv(csv_path)
    return ddb

