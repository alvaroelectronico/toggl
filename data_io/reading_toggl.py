import datetime
import os
from time import *

import numpy as np
import pandas as pd
import json
from datetime import *
from data.config import TOGGL_TOKEN_PATH, TOGGL_CACHE_PATH, START_DATE_DEF, END_DATE_DEF, USE_CACHE
from toggl.TogglPy import Toggl

# Request parameters. So far, only start date

"""
This function returns a toggl object using the toggle token
"""
def get_toggl_obj(toggl_token_path=TOGGL_TOKEN_PATH):

    # create a Toggl object and set our API key
    toggl = Toggl()

    # read Toggl token from txt file and coneect
    f = open(toggl_token_path)
    toggl_token = f.read()
    f.close()

    toggl.setAPIKey(toggl_token)

    return toggl

"""
This function returns a dataframe with all toggl entries either
getting data from Toggle (using the Toggl API)
or reading from json files.
When reading from Toggl directly info is exported as json files
"""
def get_toggl_df(toggl, start_date=START_DATE_DEF, end_date=END_DATE_DEF, use_cache=USE_CACHE):
    if use_cache and os.path.exists(TOGGL_CACHE_PATH):
        df_toggl = read_cache_toggl(TOGGL_CACHE_PATH)
        df_toggl = df_toggl[(df_toggl.date >= start_date) & (df_toggl.date <= end_date)]
        df_toggl.lunes_semana = pd.to_datetime(df_toggl.lunes_semana)
        df_toggl['lunes_semana'] = df_toggl['lunes_semana'].astype("datetime64[ns]")
        return df_toggl

    request_config = dict(
        start_date="{}T00:00:00+02:00".format(start_date),  #start_date="{}T00:00:00+02:00".format(start_date),
        end_date="{}T23:59:59+02:00".format(end_date),      #end_date="{}T23:59:59+02:00".format(end_date)
    )

    # This returns information for all entries
    time_entries = toggl.request(
        "https://api.track.toggl.com/api/v8/time_entries", parameters=request_config
    )

    # This list contains al entry ids to get the detailed info of each of them later on
    time_entry_ids = [i["id"] for i in time_entries]

    if len(time_entries) == 0:
        df_toggl = pd.DataFrame(
            columns=[
                "date",
                "client",
                "project",
                "h_toggl",
                "description",
                "start",
                "lunes_semana",
                "workspace"
            ]
        )
        df_toggl
        return df_toggl

    entries = list()
    # n_prints = 4
    # id_prints = [i for i in range(len(entries)) if np.mod(i+1, len(entries)/n_prints) == 0]
    # list_id_prints = {i: 100/n_prints*(1+id_prints.index(i)) for i in id_prints }

    for i in time_entry_ids:
        entries.append(
            toggl.request(
                "https://api.track.toggl.com/api/v8/time_entries/{}".format(i)
            )["data"]
        )
        # if i in id_prints:
        #     print("{}% downloaded".format(list_id_prints[i]))
        sleep(0.2)

    df_toggl = pd.DataFrame.from_dict(entries)
    df_toggl["date"] = df_toggl["start"].apply(
        lambda x: datetime.fromisoformat(x).date()
    )

    # Getting names of projects
    all_pid = [int(x) for x in df_toggl.pid.unique() if str(x) != "nan"]
    dct_cid_pid = {p: toggl.getProject(p)["data"] for p in all_pid}
    df_cid_pid_dict = {
        "pid": dct_cid_pid.keys(),
        "project": [dct_cid_pid[k]["name"] for k in dct_cid_pid.keys()],
        "cid": [dct_cid_pid[k]["cid"] for k in dct_cid_pid.keys()],
    }
    df_cid_pid = pd.DataFrame.from_dict(df_cid_pid_dict)

    # Merging project name and client id into df_toggl
    df_toggl = pd.merge(
        df_toggl, df_cid_pid[["pid", "project", "cid"]], on=["pid"], how="left"
    )

    # All clients retrieved from toggl
    all_clients = toggl.getClients()
    df_clients = pd.DataFrame.from_dict(all_clients)
    df_clients.rename({"id": "cid", "name": "client"}, axis=1, inplace=True)

    # Merging client column into df_toggl
    df_toggl = pd.merge(df_toggl, df_clients[["cid", "client"]], on=["cid"], how="left")

    df_toggl["h_toggl"] = df_toggl["duration"] / 3600
    df_toggl["lunes_semana"] = df_toggl["date"].apply(
        lambda x: x - timedelta(days=x.weekday() % 7)
    )

    # Getting all workspaces
    all_workspaces = toggl.getWorkspaces()
    df_worspaces = pd.DataFrame.from_dict(all_workspaces)
    df_worspaces.rename({"id": "wid", "name": "workspace"}, axis=1, inplace=True)
    # Merging workspaces into df_toggl
    df_toggl = pd.merge(
        df_toggl, df_worspaces[["wid", "workspace"]], how="left", on="wid"
    )
    # If the timer is running duration is negative. This removes that entry:
    df_toggl = df_toggl.drop(df_toggl[df_toggl.h_toggl < 0].index, axis=0)
    # Merging workspaces into toggl

    df_toggl = df_toggl[
        [
            "date",
            "client",
            "project",
            "h_toggl",
            "description",
            "start",
            "lunes_semana",
            "workspace",
        ]
    ]
    df_toggl.lunes_semana = pd.to_datetime(df_toggl.lunes_semana)
    df_toggl["lunes_semana"] = df_toggl["lunes_semana"].astype("datetime64[ns]")
    df_toggl_to_json_files(df_toggl, path=TOGGL_CACHE_PATH)
    return df_toggl

""""
Given a dataframe with Toggl info, data is stored in as many json files as days
"""

def df_toggl_to_json_files(df_toggl, path=TOGGL_CACHE_PATH):
    ok = 0
    dates = pd.date_range(df_toggl.date.min(), df_toggl.date.max()).tolist()
    dates = [datetime.date(d) for d in dates]
    df_toggl.lunes_semana = df_toggl.lunes_semana.apply(lambda x: datetime.strftime(x, "%Y-%m-%d"))

    for d in dates:
        df_to_json = df_toggl[df_toggl.date == d]
        entries = df_to_json.to_json()
        file_path = "{}/{}.json".format(path, d)
        with open(file_path, "w") as f:
            f.write(entries)
    ok = 1
    return ok

""""
Returning a dataframe with Toggl information extracted from cache json files
"""
def read_cache_toggl(path=TOGGL_CACHE_PATH, start_date=START_DATE_DEF, end_date=END_DATE_DEF):
    dates = pd.date_range(start_date, end_date).tolist()
    dates = [datetime.date(d) for d in dates]
    df_toggl = pd.DataFrame()

    for d in dates:
        file_path = "{}/{}.json".format(path, d)
        try:
            with open(file_path, "r") as f:
                json_string = f.read()
            df = pd.read_json(json_string)
            df_toggl = pd.concat([df, df_toggl])
        except:
            pass

    # df_toggl["lunes_semana"] = df_toggl["lunes_semana"].astype("datetime64[ns]")

    return df_toggl