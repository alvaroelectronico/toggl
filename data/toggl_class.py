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

class ToggleObj:

    def __init__(self, toggl_token_path=TOGGL_TOKEN_PATH, toggl_cache_path=TOGGL_CACHE_PATH,
                 start_date = START_DATE_DEF, end_date = END_DATE_DEF, days_no_cache = 3):

        # create a Toggl object and set our API key
        self.toggl_cache_path = toggl_cache_path
        self.toggl = Toggl()

        # read Toggl token from txt file and coneect
        f = open(toggl_token_path)
        toggl_token = f.read()
        f.close()
        self.toggl.setAPIKey(toggl_token)

        self.df_toggl = pd.DataFrame()
        self.df_summary_all = pd.DataFrame()
        self.df_summary_day = pd.DataFrame()
        self.df_summary_to_xlsx = pd.DataFrame()
        self.df_summary_week = pd.DataFrame()

        self.start_date = start_date
        self.end_date = end_date
        self.days_no_cache = days_no_cache
        self.dates_cache = list()
        self.dates_no_cache = list()
        self.get_dates_cache_no_cache()
        self.get_df_toggl()


    def read_cache_toggl_day(self, date):
        """"
        Returning a dataframe with Toggl information extracted from cache json files
        """
        df_toggl = pd.DataFrame()

        # for d in dates:
        file_path = "{}\{}.json".format(self.toggl_cache_path, date)
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

        return df_toggl

    def read_toggl_info(self, start_date=START_DATE_DEF, end_date=END_DATE_DEF, use_cache=USE_CACHE):
        '''
        This function returns a dataframe with all toggl entries either getting data from Toggle (using the Toggl API)
        or reading from json files. When reading from Toggl directly info is exported as json files
        '''
        if use_cache and os.path.exists(self.toggl_cache_path):
            dates = pd.date_range(start_date, end_date)
            dates = [d.strftime("%Y-%m-%d") for d in dates]
            df_toggl = pd.DataFrame()
            for d in dates:
                df = self.read_cache_toggl_day(d)
                if df.shape[0] > 0:
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
        time_entries = self.toggl.request(
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
                self.toggl.request(
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
        dct_cid_pid = {p: self.toggl.getProject(p)["data"] for p in all_pid}
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
        all_clients = self.toggl.getClients()
        df_clients = pd.DataFrame.from_dict(all_clients)
        df_clients.rename({"id": "cid", "name": "client"}, axis=1, inplace=True)

        # Merging client column into df_toggl
        df_toggl = pd.merge(df_toggl, df_clients[["cid", "client"]], on=["cid"], how="left")

        df_toggl["h_toggl"] = df_toggl["duration"] / 3600
        df_toggl["lunes_semana"] = df_toggl["date"].apply(
            lambda x: pd.to_datetime(x) - timedelta(days=pd.to_datetime(x).weekday() % 7)
        )

        # Getting all workspaces
        all_workspaces = self.toggl.getWorkspaces()
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

        self.df_toggl_to_json_files()

        return df_toggl

    def get_df_toggl(self):
        if len(self.dates_cache) > 0:
            start_date = self.dates_cache[0]
            end_date = self.dates_cache[len(self.dates_cache) - 1]
            print("reading cache for {} to {}".format(start_date, end_date))
            df_toggl = self.read_toggl_info(start_date, end_date, use_cache=True)
        else:
            df_toggl = pd.DataFrame()
        for d in self.dates_no_cache:
            d_str = d.strftime("%Y-%m-%d")
            print("reading {}".format(d_str))
            df = self.read_toggl_info(d_str, d_str, use_cache=False)
            print("read {}".format(d_str))
            df_toggl = df_toggl.append(df)
        df_toggl.reset_index(inplace=True)
        df_toggl.drop("index", axis=True, inplace=True)
        self.df_toggl = df_toggl
        self.get_agg_dfs()

    def df_toggl_to_json_files(self):
        '''
        Given a dataframe with Toggl info, data is stored in as many json files as days
        '''

        df = self.df_toggl.copy(deep=True)
        df.date = df.date.apply(lambda x: x.strftime("%Y-%m-%d"))
        df.lunes_semana = df.lunes_semana.apply(lambda x: x.strftime("%Y-%m-%d"))
        df.start = df.start.apply(lambda x: x.strftime("%Y-%m-%d %H:%M"))

        dates = pd.date_range(df.date.min(), df.date.max()).tolist()
        dates = [d.strftime("%Y-%m-%d") for d in dates]

        for d in dates:
            df_to_json = df[df.date == d]
            if df_to_json.shape[0]>0:
                entries = df_to_json.to_json(orient='records', lines=True, date_format='iso')
                file_path = "{}/{}.json".format(self.toggl_cache_path, d)
                with open(file_path, "w") as f:
                    f.write(entries)
        ok = 1
        return ok

    def get_dates_cache_no_cache(self):
        dates = pd.date_range(start_date, end_date)
        self.days_cache = max(0, len(dates) - self.days_no_cache)
        self.dates_cache = dates[0: self.days_cache]
        self.dates_no_cache = dates[self.days_cache:]

    def get_agg_dfs(self):
        df_summary_week = (
            self.df_toggl[["lunes_semana", "client", "project", "h_toggl"]]
                .groupby(by=["lunes_semana", "client", "project"], as_index=False)
                .sum()
        )

        df_summary_to_xlsx = df_summary_week.copy(deep=True)

        # Agreggating info by date, client, project
        df_summary_day = (
            self.df_toggl[["date", "client", "project", "h_toggl"]]
                .groupby(by=["date", "client", "project"], as_index=False)
                .sum()
        )

        df_summary_all = (
            self.df_toggl[["date", "client", "project", "h_toggl"]]
                .groupby(by=["client", "project"], as_index=False)
                .sum()
        )
        df_summary_day.sort_values(by=['date'], ascending=False, inplace=True)
        df_summary_week.sort_values(by=['lunes_semana'], ascending=False, inplace=True)
        self.df_toggl.sort_values(by=['date'], ascending=False, inplace=True)
        df_summary_to_xlsx.sort_values(by=['lunes_semana'], ascending=False, inplace=True)

        df_summary_day.date = df_summary_day.date.apply(lambda x: x.strftime("%d/%m/%Y"))
        df_summary_week.lunes_semana = df_summary_week.lunes_semana.apply(lambda x: x.strftime("%d/%m/%Y"))
        df_summary_to_xlsx.lunes_semana = df_summary_to_xlsx.lunes_semana.apply(lambda x: x.strftime("%d/%m/%Y"))

        self.df_summary_all, self.df_summary_day, self.df_summary_to_xlsx, self.df_summary_week = \
            df_summary_all, df_summary_day, df_summary_to_xlsx, df_summary_week

days_no_cache = 3
start_date = pd.to_datetime('2021-09-01')
end_date = pd.to_datetime(datetime.today() + timedelta(days=1))
toggl2122 = ToggleObj(TOGGL_TOKEN_PATH, TOGGL_CACHE_PATH, start_date, end_date, days_no_cache)