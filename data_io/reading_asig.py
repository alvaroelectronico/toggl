import datetime

import pandas as pd
from datetime import *


"""
TO-DELETE
"""
# file_path = ".\\data\\asignacion.xlsx"

"""
"""

# This function gets all the information corresponding to the assigned time to compared with logged time.
def get_asignacion(file_path):

    # df = pd.read_excel(file_path, sheet_name='asig', usecols="A:E")
    df = pd.read_excel(file_path, sheet_name='asig')
    df.columns = df.iloc[2, :]
    df = df.iloc[3:, :]
    df = df[~df['WS'].isna()]
    # cols_week = [c for c in df.columns in c]
    cols_week = ['WS', 'client', 'project', 'asig_s']
    df_week = df[cols_week]
    df_week.rename({'asig_s': (datetime.today() - timedelta(days=datetime.today().weekday() % 7)).strftime("%Y-%m-%d")},
               axis=1, inplace=True)

    df_week = df_week.fillna(0)
    df_week = df_week.set_index(["WS", "client", "project"])
    df_week = df_week.stack()
    df_week = df_week.reset_index()
    df_week.columns = ['WS', 'client', 'project', 'lunes_semana', 'h_asig']
    df_week = df_week[df_week.h_asig > 0]
    df_week.sort_values(by=['lunes_semana', 'WS', 'client', 'project'], ascending=True, inplace=True)
    df_week['type'] = 's'
    cols_day = ['WS', 'client', 'project', 'asig_d']
    df_day = df[cols_day]
    df_day.rename({'asig_d': datetime.today().strftime("%Y-%m-%d")},
               axis=1, inplace=True)

    df_day = df_day.fillna(0)
    df_day = df_day.set_index(["WS", "client", "project"])
    df_day = df_day.stack()
    df_day = df_day.reset_index()
    df_day.columns = ['WS', 'client', 'project', 'lunes_semana', 'h_asig']
    df_day = df_day[df_day.h_asig > 0]
    df_day.sort_values(by=['lunes_semana', 'WS', 'client', 'project'], ascending=True, inplace=True)
    df_day['type'] = 'd'

    df_sum = df_day.append(df_week)

    return df_sum