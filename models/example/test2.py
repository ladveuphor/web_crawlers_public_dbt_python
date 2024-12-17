import pandas as pd

def model(dbt, session):
    dbt.config(materialized="table")

    df = dbt.source("web_crawlers", "test")
    
    pandas_df = df.df()
    pandas_df["surname"] = pandas_df["name"] 
    pandas_df["numero"] = 1
    final_df = pandas_df

    return final_df