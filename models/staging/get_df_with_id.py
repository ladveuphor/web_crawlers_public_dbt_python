import pandas as pd
import numpy as np

def model(dbt, session):
    dbt.config(materialized="table")

    name_variable_ip = "c-ip"
    seuil_session_time = 30

    df_input = dbt.ref("get_df_pre_processed")
    df = df_input.df()

    #On cree la premiere condition pour un nouvel id (nouvelle session) : un nouvel user agent
    #Ecart de temps entre chaque requete, si ecart > 30 min, on etablit que c'est une nouvelle session
    df["datetime_diff"] = df.groupby([name_variable_ip])['datetime'].diff()

    # Conversion du temps en secondes
    df["datetime_diff"] = df["datetime_diff"].dt.total_seconds() 

    #On remplace les NaN par 0
    df = df.fillna({"datetime_diff": 0})

    #On cree la deuxieme condition pour un nouvel id : un nouvel user agent
    df["ip_num"] = df[name_variable_ip].rank().astype(int)
    df["new_ip"] = df["ip_num"].diff()
    df = df.fillna({"new_ip": 0})
    df["new_ip"] = df["new_ip"].astype(int)
    df["new_ip"].value_counts()
    df["new_ip"] = np.select([df["new_ip"] > 0], [1], default=0)

    #On cree l'id grace aux deux conditions precedentes 
    # seuil_session_time = 30 #30 min : on dit qu'une nouvelle session demarre apres 30 min d'inactivite/sans requete
    seuil_session_time *= 60 
    df["new_session"] = np.select([(df["datetime_diff"] > seuil_session_time) | (df["new_ip"] == 1)], [1], default=0)
    df["id"] = df["new_session"].cumsum()
    first_column = df.pop("id") #enlever une colonne
    df.insert(0, "id", first_column)

    # df.columns
    df = df.drop(['datetime_diff', 'ip_num', 'new_ip', 'new_session'], axis=1)

    final_df = df

    return final_df