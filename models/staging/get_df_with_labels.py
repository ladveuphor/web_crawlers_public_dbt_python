import pandas as pd
import numpy as np
import sys
sys.path.append('./python_modules/')
import lib_labels
# importer un fichier situé à deux dossiers avant le répertoire courant

def model(dbt, session):
    dbt.config(materialized="table")

    root = ""

    df_input = dbt.ref("get_df_with_features")
    df_features = df_input.df()
    # df = df.head(10000)

    # Get labels and add them to df
    df_features_labels = lib_labels.get_df_with_labels(df_features)
    
    final_df = df_features_labels 

    return final_df