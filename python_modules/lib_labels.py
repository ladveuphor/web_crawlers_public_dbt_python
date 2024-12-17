import pandas as pd
import numpy as np
import pyarrow
import sys
import os
import humanize


def get_df_with_labels(df):
    #Nouveaux criteres d'annotation de robots %_HTTP_4xx_requests
    df["is_robot"] = np.select([(df["robots_txt_found"] == 1) 
                                    | (df["robot_word_in_user_agent"] == 1)
                                    | (df["in_robots_user_agent_list"] == 1)
                                    | (df["%_empty_referer"] == 100)
                                    | (df["%_HTTP_4xx_requests"] == 100)
                                    | (df["%_HTTP_HEAD_requests"] == 100)], 
                                    [1], default=0)
    return(df)