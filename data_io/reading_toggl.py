import datetime
import os
from time import *
import pytz

import numpy as np
import pandas as pd
import json
from datetime import *
from data.config import (
    TOGGL_TOKEN_PATH,
    TOGGL_CACHE_PATH,
    START_DATE_DEF,
    END_DATE_DEF,
    USE_CACHE,
)
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

def get_toggl_df(
    toggl, start_date=START_DATE_DEF, end_date=END_DATE_DEF, use_cache=USE_CACHE
):
    if use_cache and os.path.exists(TOGGL_CACHE_PATH):
        dates = pd.date_range(start_date, end_date)
        dates = [d.strftime("%Y-%m-%d") for d in dates]
        df_toggl = pd.DataFrame()
        for d in dates:
            df = read_cache_toggl(d,  TOGGL_CACHE_PATH)
            if df.shape[0]>0:
                # df_toggl = df_toggl.(df)
                df_toggl = df_toggl.append(df)
        df_toggl.reset_index(inplace=True)
        df_toggl.drop("index", axis=1, inplace=True)
        return df_toggl

    request_config = dict(
        start_date="{}T00:00:00+00:00".format(
            start_date
        ),
        end_date="{}T23:59:59+00:00".format(
            end_date
        ),
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
                "workspace",
            ]
        )
        df_toggl
        return df_toggl

    entries = list()

    for i in time_entry_ids:
        entries.append(
            toggl.request(
                "https://api.track.toggl.com/api/v8/time_entries/{}".format(i)
            )["data"]
        )
        sleep(0.2)

    df_toggl = pd.DataFrame.from_dict(entries)

    madrid_tzinfo = pytz.timezone("Europe/Madrid")
    df_toggl.start = df_toggl.start.apply(
        lambda x: pd.to_datetime(x).astimezone(madrid_tzinfo)
    )

    df_toggl["date"] = df_toggl["start"].apply(
        lambda x: pd.to_datetime(x).strftime("%Y-%m-%d")
    )

    df_toggl["date"] = df_toggl["date"].apply(lambda x: pd.to_datetime(x))

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
        lambda x: pd.to_datetime(x) - timedelta(days=pd.to_datetime(x).weekday() % 7)
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

    df_toggl_to_json_files(df_toggl, path=TOGGL_CACHE_PATH)

    return df_toggl


""""
Given a dataframe with Toggl info, data is stored in as many json files as days
"""

def df_toggl_to_json_files(df_toggl, path=TOGGL_CACHE_PATH):
    df = df_toggl.copy(deep=True)
    df.date = df.date.apply(lambda x: x.strftime("%Y-%m-%d"))
    df.lunes_semana = df.lunes_semana.apply(lambda x: x.strftime("%Y-%m-%d"))
    df.start = df.start.apply(lambda x: x.strftime("%Y-%m-%d %H:%M"))

    dates = pd.date_range(df.date.min(), df.date.max()).tolist()
    dates = [d.strftime("%Y-%m-%d") for d in dates]

    for d in dates:
        df_to_json = df[df.date == d]
        if df_to_json.shape[0]>0:
            entries = df_to_json.to_json(orient='records', lines=True, date_format='iso')
            file_path = "{}/{}.json".format(path, d)
            with open(file_path, "w") as f:
                f.write(entries)
    ok = 1
    return ok


""""
Returning a dataframe with Toggl information extracted from cache json files
"""

def read_cache_toggl(date, path=TOGGL_CACHE_PATH):
    df_toggl = pd.DataFrame()

    # for d in dates:
    file_path = "{}\{}.json".format(path, date)
    try:
        with open(file_path, "r") as f:
            json_string = f.read()
        df_toggl = pd.read_json(json_string, orient='records', lines=True)
    except:
        pass

    try:
        df_toggl.date = df_toggl.date.apply(lambda x: pd.to_datetime(x))
        df_toggl.start = df_toggl.start.apply(lambda x: pd.to_datetime(x))
        df_toggl.lunes_semana = df_toggl.lunes_semana.apply(lambda x: pd.to_datetime(x))
    except:
        pass

    # df_toggl = df_toggl.dropna(subset=['h_toggl'])
    return df_toggl
