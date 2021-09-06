import datetime
from time import *
import pandas as pd
import json
from datetime import *


# Request parameters. So far, only start date


def get_toggl(toggl, start_date="2021-09-03", end_date="2021-09-03"):
    request_config = dict(
        start_date="{}T00:00:00+02:00".format(start_date),
        end_date="{}T23:59:59+02:00".format(end_date),
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
                "h_registradas",
                "description",
                "start",
                "lunes_semana",
                "workspace"
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
        #print(time_entry_ids.index(i))
        sleep(0.1)

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

    df_toggl["h_registradas"] = df_toggl["duration"] / 3600
    df_toggl["lunes_semana"] = df_toggl["date"].apply(
        lambda x: x - timedelta(days=x.weekday() % 7)
    )
    df_toggl["lunes_semana"] = df_toggl["lunes_semana"].astype("datetime64[ns]")

    # Getting all workspaces
    all_workspaces = toggl.getWorkspaces()
    df_worspaces = pd.DataFrame.from_dict(all_workspaces)
    df_worspaces.rename({"id": "wid", "name":"workspace"}, axis=1, inplace=True)
    # Merging workspaces into df_toggl
    df_toggl = pd.merge(df_toggl, df_worspaces[["wid", "workspace"]], how="left", on="wid")

    # Merging workspaces into toggl

    df_toggl = df_toggl[
        [
            "date",
            "client",
            "project",
            "h_registradas",
            "description",
            "start",
            "lunes_semana",
            "workspace",
        ]
    ]

    return df_toggl


def df_toggl_to_json_files(path, df_toggl):
    ok = 0
    dates = pd.date_range(df_toggl.date.min(), df_toggl.date.max()).tolist()
    dates = [datetime.date(d) for d in dates]

    for d in dates:
        df_to_json = df_toggl[df_toggl.date == d]
        entries = df_to_json.to_json()
        file_path = "{}/{}.json".format(path, d)
        with open(file_path, "w") as f:
            f.write(entries)
    ok = 1
    return ok


def read_cache_toggl(path, start_date, end_date):
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

    return df_toggl


# """
# TODO: turn this three lines into a function
# """
# toggl_entries = get_json_entries(toggl, start_date, end_date)
# df_toggl = toggl_entries_to_df(toggl_entries)

# df_toggl = read_cache_toggl(path, start_date, end_date)
