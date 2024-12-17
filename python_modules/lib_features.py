import pandas as pd
import numpy as np
import pyarrow
import sys
import os
import humanize

import time 
import Levenshtein 
import textdistance

### Fonctions pour créer les features

#Creation du dataset final avec les nouvelles variables
#Creation de la variable total_requests (nombre de pages visitees / nombre de clics)
# df_final = df.groupby([id_variable])[id_variable].count().to_frame()

def create_variable_total_requests(df, id_variable):
    df_final = df.groupby([id_variable])[id_variable].count().to_frame().rename(columns={id_variable: "total_requests"}).reset_index()
    return(df_final)

# df_final


#Creation de la variable number_of_empty_referer
def create_is_empty_referrer(df, id_variable):
    # a. Creation de la colonne is_empty_referrer = 1 si le referrer est vide, 0 sinon
    conditions_list = [df['cs_Referer'] == "-"]
    values_list = [1]
    df['is_empty_referrer'] = np.select(conditions_list, values_list, default=0)

    # b. Somme (agregation) des is_empty_referrer pour chaque id (groupby)
    df_empty_referrer = df.groupby(id_variable)['is_empty_referrer'].sum()
    df_empty_referrer = df_empty_referrer.to_frame().reset_index()
    df_empty_referrer.columns = ["id", "number_of_empty_referrer"]
    df_empty_referrer["is_empty_referrer"] = np.select([df_empty_referrer["number_of_empty_referrer"] > 0], [1], default=0)
    df_empty_referrer = df_empty_referrer.drop(["number_of_empty_referrer"], axis=1)
    return(df_empty_referrer)
    
# df_empty_referrer


#Creation de la %_HTTP_4xx_requests = taux de reponses serveur du type 4xx (erreur)
def create_HTTP_4xx_requests_variables(df, id_variable):
    # a. Creation de la colonne is_4xx_answer = 1 si le la reponse du serveur est du type 4xx (erreur), 0 sinon
    conditions_list = [df['sc-status'].str.get(0) == "4"]
    values_list = [1]
    df['is_4xx_answer'] = np.select(conditions_list, values_list, default=0)

    # b. Somme (agregation) des is_4xx_answer pour chaque id (groupby)
    # Trois cas possibles
    # Cas 1 (session avec que des reponses de serveur 4xx) : 
    # somme de 0 + 0 + ... + 0 => number_of_4xx_answer = 0 
    # Cas 2 (session avec aucune reponse de serveur 4xx) : 
    # somme d'entiers strictement positifs (ex : 2 + 12 + ...) => number_of_4xx_answer = n > 0
    # Cas 3 (session avec a la fois des reponses de serveur 4xx et des reponses de serveur non 4xx)
    # somme de 0 et d'entiers strictement positifs (ex : 0 + 12 + 0 + 0 + 3 + ...) => number_of_4xx_answer = n > 0
    df_4xx_requests = df.groupby(id_variable)['is_4xx_answer'].sum()
    df_4xx_requests = df_4xx_requests.to_frame().reset_index()
    df_4xx_requests.columns = [id_variable, "number_of_4xx_answer"]

    return(df_4xx_requests)

# df_4xx_requests


#Creation de la %_HTTP_3xx_requests = taux de reponses serveur du type 3xx (erreur)
def create_HTTP_3xx_requests_variables(df, id_variable):
    # a. Creation de la colonne is_3xx_answer = 1 si le la reponse du serveur est du type 3xx (erreur), 0 sinon
    conditions_list = [df['sc-status'].str.get(0) == "3"]
    values_list = [1]
    df['is_3xx_answer'] = np.select(conditions_list, values_list, default=0)

    # b. Somme (agregation) des is_3xx_answer pour chaque id (groupby)
    # Trois cas possibles
    # Cas 1 (session avec que des reponses de serveur 3xx) : 
    # somme de 0 + 0 + ... + 0 => number_of_3xx_answer = 0 
    # Cas 2 (session avec aucune reponse de serveur 3xx) : 
    # somme d'entiers strictement positifs (ex : 2 + 12 + ...) => number_of_3xx_answer = n > 0
    # Cas 3 (session avec a la fois des reponses de serveur 3xx et des reponses de serveur non 3xx)
    # somme de 0 et d'entiers strictement positifs (ex : 0 + 12 + 0 + 0 + 3 + ...) => number_of_3xx_answer = n > 0
    df_3xx_requests = df.groupby(id_variable)['is_3xx_answer'].sum()
    df_3xx_requests = df_3xx_requests.to_frame().reset_index()
    df_3xx_requests.columns = [id_variable, "number_of_3xx_answer"]

    return(df_3xx_requests)

# df_3xx_requests


#Creation de la variable HTTP_GET_requests 
def create_HTTP_GET_requests(df, id_variable):
    df_GET = df.groupby([id_variable, 'cs-method']).size().to_frame().reset_index().rename(columns={0: "HTTP_GET_requests"})
    df_GET["HTTP_GET_requests"] = np.select([df_GET["cs-method"] == "GET"], [df_GET["HTTP_GET_requests"]], default=0)
    df_GET = df_GET.groupby(id_variable)['HTTP_GET_requests'].sum().to_frame().reset_index()
    return(df_GET)

# df_GET


#Creation de la variable HTTP_POST_requests 
def create_HTTP_POST_requests(df, id_variable):
    df_POST = df.groupby([id_variable, 'cs-method']).size().to_frame().reset_index().rename(columns={0: "HTTP_POST_requests"})
    df_POST["HTTP_POST_requests"] = np.select([df_POST["cs-method"] == "POST"], [df_POST["HTTP_POST_requests"]], default=0)
    df_POST = df_POST.groupby(id_variable)['HTTP_POST_requests'].sum().to_frame().reset_index()
    return(df_POST)

# df_POST


#Creation de la variable HTTP_HEAD_requests 
def create_HTTP_HEAD_requests(df, id_variable):
    df_HEAD = df.groupby([id_variable, 'cs-method']).size().to_frame().reset_index().rename(columns={0: "HTTP_HEAD_requests"})
    df_HEAD["HTTP_HEAD_requests"] =  np.select([df_HEAD["cs-method"] == "HEAD"], [df_HEAD["HTTP_HEAD_requests"]], default=0)
    df_HEAD = df_HEAD.groupby(id_variable)['HTTP_HEAD_requests'].sum().to_frame().reset_index()
    return(df_HEAD)

# df_HEAD


#Creation de la variable total_session_bytes 
def create_total_session_bytes(df, id_variable):
    df_total_session_bytes = df.groupby([id_variable])["sc-bytes"].sum().to_frame().reset_index().rename(columns={"sc-bytes": "total_session_bytes"})
    return(df_total_session_bytes)

# df_total_session_bytes


#Creation de la variable nb_image_requests, nb_js_requests, nb_css_requests, requested_page_std 
#et robots_txt_found : 1 si robots.txt trouve dans une requete, i0 sinon (pour chaque utilisateur)

def create_url_variables(df, id_variable):
    image_formats = ["png", "jpg", "svg", "jpeg", "gif", "bmp", "tif"]
    df_url_variables = df.groupby([id_variable, 'cs-uri-stem']).size().to_frame().reset_index()
    df_url_variables = df_url_variables.rename(columns={0: "cs-uri-stem_counts"})

    df_url_variables["requested_page_depth"] = df_url_variables["cs-uri-stem"].str.count("/")
    df_url_variables["requested_page_url_split"] = df_url_variables["cs-uri-stem"].str.split("/")
    df_url_variables["requested_page_type"] = df_url_variables["requested_page_url_split"].str[-1].str.split(".").str[-1]

    df_url_variables["is_image_request"] = np.select([df_url_variables["requested_page_type"].isin(image_formats)], [1], default=0)
    df_url_variables["is_js_request"] = np.select([df_url_variables["requested_page_type"] == "js"], [1], default=0)
    df_url_variables["is_css_request"] = np.select([df_url_variables["requested_page_type"] == "css"], [1], default=0)

    df_url_variables["robots_txt_found"] = np.select([df_url_variables['cs-uri-stem'].str.contains("robots.txt") == True], [1], default=0)

    df_url_variables = df_url_variables.groupby(id_variable).agg(
                              nb_image_requests=('is_image_request', 'sum'),
                              nb_js_requests=('is_js_request', 'sum'),
                              nb_css_requests=('is_css_request', 'sum'),
                              requested_page_std=('requested_page_depth', lambda x: np.std(x, ddof=0)),
                              robots_txt_found=('robots_txt_found', 'sum')
    )
    return(df_url_variables)

# df_url_variables


def create_in_robots_user_agent_list(df, list_public_user_agents, name_variable_user_agent, name_variable_id):
    # a. Creation de la colonne in_robots_user_agent_list = 1 si l'user agent est dans la liste publique, 0 sinon
    # Comparaison par pourcentage de ressemblance (de maniere non vectorisee avec une boucle for)
    # Temps d'execution = avec au maximum 10 millions de lignes => 10 000 000 * 1046 = 2 000 000 000 = 2 milliards de calcul
    # car il y a 10 millions user agents, 10 lignes dans le dataframe df
    # Particularite : prise en compte de la non unicite des user agents de chaque cle primaire, un id de session peut maintenant avoir plusieurs user agents
    # (contrairement a avant ou la cle primaire etait l'user agent)
    # Distance utilisee = distance de Levenshtein = mesure de similarite, plus elle est elevee plus les deux elements sont similaires

# Idee pour reduire drastiquement le temps de calcul (qui durerait des heures si l'on faisait une boucle for naive) : 
# Dans le dataframe, de nombreux user agents apparaissent de nombreuses fois (doublons), donc il ne sert a rien de calculer toutes leurs
# distances (de les comparer) avec les user agents de la liste publique.
# Il suffit donc de calculer chaque distance seulement une seule fois.
# Concretement on calculera juste une seule fois pour chaque user agent toutes ses distances, et on les stockera toutes dans une liste. 
# Et cette liste sera elle meme stockee dans un dictionnaire de cette forme : cle = user agent, valeur = liste des distances
# Enfin, on parcourera la colonne user agent du dataframe et determinera grace a sa liste de distances correspondantes si ce user agent
# ressemble tres fortement a un des user agents robots de la liste publique. Si oui on lui donnera la valeur 1, sinon 0. 
    user_agents = df[name_variable_user_agent]
    labels_robots = []
    
    # Creation d'un dictionnaire pour un seul user agent, calcul de ses distances avec tous les users agents de la liste publique
    user_agents_unique = df[name_variable_user_agent].unique()
    dictionary_distances = {} #cle = user_agent, valeur = liste des distances avec tous les user agent de la liste publique
    
    for user_agent in user_agents_unique:
        list_distances = []
        
        plausible_user_agents = [user_agent_public for user_agent_public in list_public_user_agents 
                                 if(abs(len(user_agent_public) - len(user_agent)) < 4 and user_agent_public[0] == user_agent[0])] 
        # print("len plausible_user_agents =", len(plausible_user_agents))

        for user_agent_public in plausible_user_agents:
            list_distances.append(Levenshtein.ratio(user_agent, user_agent_public))
        dictionary_distances[user_agent] = list_distances #enregistre la nouvelle distance

    # Calcul de la variable in_robots_user_agent_list pour chaque user agent : check si l'user agent est dans la liste robot 
    # (stockee dans le dictionnaire calculee juste avant)
    user_agents = df[name_variable_user_agent]
    labels_robots = []
    for user_agent in user_agents: #parcourt les users agents du dataframe
        if(any(dist > 0.95 for dist in dictionary_distances[user_agent])):
            labels_robots.append(1)
        else:
            labels_robots.append(0) 
          
    df['in_robots_user_agent_list'] = labels_robots
    
    # b. Somme (agregation) des in_robots_user_agent_list pour chaque id (groupby)
    df_in_robots_user_agent_list = df.groupby(name_variable_id)['in_robots_user_agent_list'].sum()
    df_in_robots_user_agent_list = df_in_robots_user_agent_list.to_frame().reset_index()
    df_in_robots_user_agent_list.columns = [name_variable_id, "number_of_in_robots_user_agent_list"]
    df_in_robots_user_agent_list["in_robots_user_agent_list"] = np.select([df_in_robots_user_agent_list["number_of_in_robots_user_agent_list"] > 0], 
                                                                          [1], default=0)
    df_in_robots_user_agent_list = df_in_robots_user_agent_list.drop(["number_of_in_robots_user_agent_list"], axis=1)
    # df_in_robots_user_agent_list[df_in_robots_user_agent_list["number_of_in_robots_user_agent_list"] > 0]
    # df_in_robots_user_agent_list.value_counts("in_robots_user_agent_list")
    
    return(df_in_robots_user_agent_list)


# Creation de la colonne in_robots_ip_list = 1 si l'ip est dans la liste publique, 0 sinon
# Creation de la colonne in_robots_ip_list = 1 si l'ip est dans la liste publique, 0 sinon
def create_in_robots_ip_list(df, list_public_ip, name_variable_ip, name_variable_id):
    ip = df[name_variable_ip]
    labels_robots = []

    # Creation d'un dictionnaire pour un seul ip, calcul de ses distances avec tous les ip de la liste publique
    ip_unique = df[name_variable_ip].unique()
    dictionary_distances = {} #cle = ip, valeur = liste des distances avec tous les ip de la liste publique

    for ip in ip_unique:
        list_distances = []
        plausible_ip = [ip_public for ip_public in list_public_ip if((abs(len(ip_public) - len(ip)) < 2) and ip_public[0:10] == ip[0:10])] 
        # print("len plausible_ip =", len(plausible_ip))

        for ip_public in plausible_ip:
            list_distances.append(ip == ip_public)
        dictionary_distances[ip] = list_distances #enregistre la nouvelle distance

    # Calcul de la variable in_robots_ip_list pour chaque ip : check si l'ip est dans la liste robot 
    # (stockee dans le dictionnaire calculee juste avant)
    ip = df[name_variable_ip]
    labels_robots = []
    for ip in ip: #parcourt les ip du dataframe
        if(any(True for dist in dictionary_distances[ip])):
            labels_robots.append(1)
        else:
            labels_robots.append(0) 
          
    df['in_robots_ip_list'] = labels_robots

    # b. Somme (agregation) des in_robots_ip_list pour chaque id (groupby)
    df_in_robots_ip_list = df.groupby(name_variable_id)['in_robots_ip_list'].sum()
    df_in_robots_ip_list = df_in_robots_ip_list.to_frame().reset_index()
    df_in_robots_ip_list.columns = [name_variable_id, "number_of_in_robots_ip_list"]
    df_in_robots_ip_list["in_robots_ip_list"] = np.select([df_in_robots_ip_list["number_of_in_robots_ip_list"] > 0], 
                                                                          [1], default=0)
    df_in_robots_ip_list = df_in_robots_ip_list.drop(["number_of_in_robots_ip_list"], axis=1)
    # df_in_robots_ip_list[df_in_robots_ip_list["number_of_in_robots_ip_list"] > 0]
    # df_in_robots_ip_list.value_counts("in_robots_ip_list")

    return(df_in_robots_ip_list)


#Creation de la variable max_requests_per_page = le nombre max de requetes pour une seule page
def create_max_requests_per_page(df, id_variable):
    df_max_requests_per_page = df.groupby([id_variable, "cs-uri-stem"]).size().to_frame().reset_index()
    #peut etre rajouter tri dans l'ordre decroissant des nombres d'occurrences de chaque url pour chaque utilisateur

    df_max_requests_per_page = df_max_requests_per_page.rename(columns={0: "cs-uri-stem-count"})

    df_max_requests_per_page = df_max_requests_per_page.groupby(id_variable).agg(
                                max_requests_per_page=('cs-uri-stem-count', 'max'),
                                average_requests_per_page=('cs-uri-stem-count', 'mean')
    )

    df_max_requests_per_page = df_max_requests_per_page.rename(columns={"cs-uri-stem-count": "max_requests_per_page"})
    return(df_max_requests_per_page)

# df_max_requests_per_page


#Creation de la variable robot_word_in_user_agent : 1 si les mots robot ou bot se trouvent dans l'user agent, 0 sinon
#L'user agent est contenu par definition dans l'id id_variable
def create_robot_word_in_user_agent(df, name_variable_id, name_variable_user_agent, list_robot_words):
    # robot_words = ["bot"]
    # new potential words : "scan", "request"
    df['robot_word_in_user_agent'] = df[name_variable_user_agent].str.contains('|'.join(list_robot_words)).astype(int)
    df.loc[df["robot_word_in_user_agent"] == 1, [name_variable_id, name_variable_user_agent, "robot_word_in_user_agent"]]

    pd.set_option('display.max_rows', 15) #il faut changer les deux pour que ca marche
    pd.set_option('display.min_rows', 15)
    df_robot_word_in_user_agent = df.groupby(name_variable_id)['robot_word_in_user_agent'].sum()
    df_robot_word_in_user_agent = df_robot_word_in_user_agent.to_frame().reset_index()
    df_robot_word_in_user_agent.columns = [name_variable_id, "number_of_robot_word_in_user_agent"]

    df_robot_word_in_user_agent["robot_word_in_user_agent"] = np.select([df_robot_word_in_user_agent["number_of_robot_word_in_user_agent"] > 0], 
                                                                      [1], default=0)
    df_robot_word_in_user_agent = df_robot_word_in_user_agent.drop(["number_of_robot_word_in_user_agent"], axis=1)

    return(df_robot_word_in_user_agent)

# df_robot_word_in_user_agent


#Creation des variables std_of_inter_request_times
def create_std_of_inter_request_times(df, id_variable):
    dftmp = df.copy()
    dftmp["time"] = pd.to_timedelta(dftmp['time'])
    dftmp["inter_request_times"] = dftmp.groupby([id_variable])['time'].diff()
    dftmp["inter_request_times"] = dftmp["inter_request_times"].dt.total_seconds()

    df_inter_request_times = dftmp.groupby(id_variable).agg(
                                std_of_inter_request_times=('inter_request_times', 'std')
    )
    return(df_inter_request_times)


#Creation de la variable session_time (en secondes)
def create_session_time(df, id_variable):
    df_session_time = df.groupby(id_variable).agg(
                          first_http_request=('datetime', 'first'),
                          last_http_request=('datetime', 'last')
    )
    # print(df_session_time)
    # print(type(df_session_time["last_http_request"].iloc[0]))
    df_session_time["session_time"] = (df_session_time["last_http_request"] - df_session_time["first_http_request"]).dt.total_seconds()
    # df_session_time["session_time"] /= 60 #si on veut convertir en minutes
    df_session_time = df_session_time.drop(["first_http_request", "last_http_request"], axis=1)
    return(df_session_time)


#Renvoie un dataframe de sessions avec les features pret pour l'entrainement
#Pour chaque session (chaque ligne), on a des informations qui la decrivent
#Entrees :
    #df (dataframe) : un dataframe de logs  
    #list_public_user_agents (liste de string) : liste publique disponible en ligne des user agents des robots deja connus de tous
    #minimal_session_time (int) : duree minimale d'une session en min () 
    #name_variable_id (str) : nom de la variable id qui sert de cle primaire pour identifier de maniere unique chaque session
    #name_variable_user_agent (str) : nom de la variable pour l'user agent
    #list_robot_words (liste de str) : liste des mots du champ lexical des robots a detecter dans l'user agent
#Sortie :
    #df (dataframe) : le dataset qui contient les logs d'une periode
def get_df_with_features(df, list_public_user_agents, list_public_ip, minimal_session_time, name_variable_id, name_variable_ip, name_variable_user_agent, 
                         list_robot_words):
    #Pretraitements
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["sc-status"] = df["sc-status"].astype(str)
    df = df.sort_values([name_variable_id, "datetime"])
    df_final = create_variable_total_requests(df, name_variable_id)
    
    #Rajout de la colonne user-agent (contient tous les user agents de chaque session)
    df_user_agents = df.drop_duplicates(subset=[name_variable_id, name_variable_user_agent])
    df_user_agents = df_user_agents.groupby([name_variable_id], as_index = False).agg({name_variable_user_agent: "\n".join})

    #Rajout de la colonne c-ip (ip client)
    df_client_ip_source = df.drop_duplicates(subset=[name_variable_id, name_variable_ip])
    df_client_ip_source = df_client_ip_source[["id", "c-ip"]]

    #Rajout des colonnes pour chaque parametre
    df_empty_referrer = create_is_empty_referrer(df, name_variable_id)
    df_4xx_requests = create_HTTP_4xx_requests_variables(df, name_variable_id)
    df_3xx_requests = create_HTTP_3xx_requests_variables(df, name_variable_id)
    df_GET = create_HTTP_GET_requests(df, name_variable_id)
    df_POST = create_HTTP_POST_requests(df, name_variable_id)
    df_HEAD = create_HTTP_HEAD_requests(df, name_variable_id)
    # df_total_session_bytes = create_total_session_bytes(df, name_variable_id)
    df_url_variables = create_url_variables(df, name_variable_id)
    df_max_requests_per_page = create_max_requests_per_page(df, name_variable_id)
    df_inter_request_times = create_std_of_inter_request_times(df, name_variable_id)
    df_inter_request_times = df_inter_request_times.fillna({"std_of_inter_request_times": 0})
    df_session_time = create_session_time(df, name_variable_id)
    
    # Rajout des colonnes pour les parametres en lien avec l'user agent et l'ip
    # print("dans main, taille de df_final =", df_final.shape)
    df_in_robots_user_agent_list = create_in_robots_user_agent_list(df, list_public_user_agents, name_variable_user_agent, name_variable_id)
    df_robot_word_in_user_agent = create_robot_word_in_user_agent(df, name_variable_id, name_variable_user_agent, list_robot_words)
    df_in_robots_ip_list = create_in_robots_ip_list(df, list_public_ip, name_variable_ip, name_variable_id)

    #On regroupe toutes les nouvelles colonnes dans le dataset final
    df_final = df_final.merge(df_4xx_requests, on=name_variable_id).merge(df_3xx_requests, on=name_variable_id)
    df_final = df_final.merge(df_HEAD, on=name_variable_id).merge(df_GET, on=name_variable_id).merge(df_POST, on=name_variable_id)
    # df_final = df_final.merge(df_total_session_bytes, on=name_variable_id)
    df_final = df_final.merge(df_url_variables, on=name_variable_id)
    df_final = df_final.merge(df_max_requests_per_page, on=name_variable_id)
    df_final = df_final.merge(df_robot_word_in_user_agent, on=name_variable_id)
    df_final = df_final.merge(df_session_time, on=name_variable_id)
    df_final = df_final.merge(df_inter_request_times, on=name_variable_id)
    df_final = df_final.merge(df_empty_referrer, on=name_variable_id)
    df_final = df_final.merge(df_in_robots_user_agent_list, on=name_variable_id)
    df_final = df_final.merge(df_in_robots_ip_list, on=name_variable_id)
    df_final = df_final.merge(df_client_ip_source, on=name_variable_id)
    df_final = df_final.merge(df_user_agents, on=name_variable_id)  
    print("df_final colonnes :")
    # print(df_final.columns)

    df_final["%_HTTP_4xx_requests"] = 100 * (df_final["number_of_4xx_answer"] / df_final["total_requests"])
    df_final["%_HTTP_3xx_requests"] = 100 * (df_final["number_of_3xx_answer"] / df_final["total_requests"])
    df_final["%_empty_referer"] = 100 * (df_final["is_empty_referrer"] / df_final["total_requests"])
    df_final["%_HTTP_HEAD_requests"] = 100 * (df_final["HTTP_HEAD_requests"] / df_final["total_requests"]) 
    df_final["%_HTTP_POST_requests"] = 100 * (df_final["HTTP_POST_requests"] / df_final["total_requests"]) 
    df_final["%_HTTP_GET_requests"] = 100 * (df_final["HTTP_GET_requests"] / df_final["total_requests"])
    df_final["%_image_requests"] = 100 * df_final["nb_image_requests"] / df_final["total_requests"]
    df_final["%_css_requests"] = 100 * df_final["nb_css_requests"] / df_final["total_requests"]
    df_final["%_js_requests"] = 100 * df_final["nb_js_requests"] / df_final["total_requests"]
    # df_final["HTML_to_image_ratio"] = 100 * df_final["nb_js_requests"] / df_final["total_requests"]
    df_final["browsing_speed"] = df_final["total_requests"] / df_final["session_time"]
    
    df_final = df_final.drop(["number_of_4xx_answer", "number_of_3xx_answer", "nb_image_requests", "nb_js_requests", "nb_css_requests"], axis=1)
    df_final = df_final.drop(["HTTP_HEAD_requests", "HTTP_POST_requests", "HTTP_GET_requests", "is_empty_referrer"], axis=1)
    df_final = df_final.replace(np.inf, 0) #pour remplacer les inf par 0, inf dans browsing_speed

    #On garde seulement : 
    # 1) Les sessions assez longues (pas trop courtes comme 1 seconde, etc.)
    minimal_session_time *= 60 #conversion depuis les min en sec
    df_final = df_final[df_final["session_time"] > minimal_session_time]
    # 2) Les sessions avec un nombre suffisant de requetes
    df_final = df_final.sort_values(name_variable_id)
    df_final = df_final[df_final["total_requests"] > 4]

    df_final["number_of_user_agents"] = df_final[name_variable_user_agent].str.count("\n")
    df_final["number_of_user_agents"] += 1

    df_final = df_final[[name_variable_id, name_variable_ip, name_variable_user_agent, 
                         "number_of_user_agents", 'total_requests', 
                        'requested_page_std', 'robots_txt_found',
                        'max_requests_per_page', 'average_requests_per_page',
                        'robot_word_in_user_agent', 'session_time',
                        'std_of_inter_request_times', 'in_robots_user_agent_list',
                        'in_robots_ip_list', '%_HTTP_4xx_requests',
                        '%_HTTP_3xx_requests', '%_empty_referer', '%_HTTP_HEAD_requests',
                        '%_HTTP_POST_requests', '%_HTTP_GET_requests', '%_image_requests',
                        '%_css_requests', '%_js_requests', 'browsing_speed']] 

    return(df_final)


