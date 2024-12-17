import pandas as pd
# import numpy as np
import humanize
import sys

def model(dbt, session):
    dbt.config(materialized="table")

    name_variable_ip = "c-ip"
    name_variable_id = "id"

    df_input = dbt.ref("ingest")
    df = df_input.df()

    df["sc-status"] = df["sc-status"].astype(str)
    variable_id = name_variable_ip
    first_column = df.pop(variable_id)
    df.insert(0, variable_id, first_column)
    df["datetime"] = pd.to_datetime(df["date"] + " " + df["time"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    print("df taile avant enlever date =", humanize.naturalsize(sys.getsizeof(df)))
    df = df.drop("date", axis=1)   
    print("df taile apres enlever date =", humanize.naturalsize(sys.getsizeof(df)))
    df = df.sort_values([variable_id, "datetime"])

    final_df = df

    return final_df