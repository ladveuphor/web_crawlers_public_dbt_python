import pandas as pd
import numpy as np
import pyarrow
import sys
import os
import humanize


def get_df_pre_processed(df, name_variable_ip, name_variable_user_agent, path=""):
    if(path != ""):
        os.chdir(path)

    #Etapes de pre-traitement : correction de types de colonnes, nouvelle colonne datatime, trier les lignes, renommer les colonnes
    # df = df.drop(df.iloc[:, 0:5], axis=1)
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
    return(df)


#Entrees :
    #seuil_session_time (int) : duree d'inactivite (en sec) a partir de laquelle on considere qu'il y a une nouvelle session
#Sortie :
    #df (dataframe) : dataframe d'entree avec une nouvelle colonne id
def get_df_with_id(df, seuil_session_time, name_variable_ip):
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
    # df = df[['id', 'datetime', 'c-ip', 'cs-method', 'cs-uri-stem', 'cs-uri-query', 'cs-username', 'cs(User-Agent)', 
    #          'cs_Referer', 'sc-status', 'time']]
    return(df)