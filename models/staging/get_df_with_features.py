import pandas as pd
import numpy as np
import sys
import os
sys.path.append('./python_modules/')
import lib_features
# importer un fichier situé à deux dossiers avant le répertoire courant

def model(dbt, session):
    dbt.config(materialized="table")

    print((f"Répertoire courant : {os.getcwd()}"))
    # dbt.log(f"Répertoire courant : {os.getcwd()}")
    

    # root = "../../"
    root = ""
    f = open(root + "data/user_agent_robots.txt", "r")
    file_content = f.read()
    list_public_user_agents = file_content.split("\n")
    f = open(root + "data/ip_robots.txt", "r")
    file_content = f.read()
    list_public_ip = file_content.split("\n")
    f.close()

    #Creation des parametres du modele d'apprentissage supervise
    minimal_session_time = 5 #min
    name_variable_id = "id"
    name_variable_ip = "c-ip"
    name_variable_user_agent = "cs_User_Agent "
    list_robot_words = ["bot", "spider", "crawler", "Crawler", "robot", "worm", "search", "track", "harvest", "hack", "trap", "archive", 
                    "scrap", "scan", "crawl"]

    df_input = dbt.ref("get_df_with_id")
    df = df_input.df()
    # df = df.head(10000)

    df_features = lib_features.get_df_with_features(df, list_public_user_agents, list_public_ip,
                                                    minimal_session_time, name_variable_id, 
                                                    name_variable_ip, name_variable_user_agent, 
                                                    list_robot_words)
    
    final_df = df_features 

    return final_df