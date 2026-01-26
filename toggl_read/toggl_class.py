import numpy as np
from time import *
import pandas as pd
from datetime import *
import os
import requests
from base64 import b64encode
from toggl_read.config import TOGGL_USER_ALVARO

api_token = "7d7d23795bc4db14300ad9e56bb34cf8:api_token".encode()

toggl_api_headers = {
                "content-type": "application/json",
                "Authorization": "Basic %s" % b64encode(api_token).decode("ascii"),
            }

from toggl_read.config import (
    TOGGL_TOKEN_PATH,
    TOGGL_CACHE_PATH,
    START_DATE_DEF,
    END_DATE_DEF,
)
from toggl.TogglPy import Toggl

class ToggleObj:
    def __init__(
        self,
        toggl_token_path=TOGGL_TOKEN_PATH,
        toggl_cache_path=TOGGL_CACHE_PATH,
        start_date=START_DATE_DEF,
        end_date=END_DATE_DEF,
        days_no_cache=3,
        export_cache_to_json=True,
        get_entries=True
    ):

        # create a Toggl object and set our API key
        self.toggl_cache_path = toggl_cache_path
        self.export_cache_to_json = export_cache_to_json
        self.toggl = Toggl()

        # read Toggl token from txt file and coneect
        f = open(toggl_token_path)
        toggl_token = f.read()
        f.close()
        self.toggl.setAPIKey(toggl_token)

        self.df_toggl = pd.DataFrame()

        self.start_date = start_date
        self.end_date = end_date
        self.days_no_cache = days_no_cache
        self.dates_cache = list()
        self.dates_no_cache = list()
        self._get_dates_cache_no_cache(self.start_date, self.end_date)

        if get_entries:
            self.get_df_toggl()
            self.check_entries_without_client(self.df_toggl)


    def check_entries_without_client(self, df):
        
        if not df.empty:
            df2 = df[(df.client.isna()) | (df.project.isna())]

            if df2.shape[0] > 0:
                for i in df2['date'].unique():
                    i_np = np.datetime64(i)  # Convert to NumPy datetime object
                    date_str = np.datetime_as_string(i_np, unit='D')
                    print(f"{date_str} contiene registros sin fecha en el cliente o sin project")

    def read_cache_day(self, date):
        """ "
        Returning a dataframe with Toggl information extracted from cache json files
        """

        file_path = "{}\{}.json".format(self.toggl_cache_path, date)
        try:
            # with open(file_path, "r") as f:
            #     json_string = StringIO(f.read())
            # df_toggl = pd.read_json(json_string, orient="records", lines=True)

            df_toggl = pd.read_json(file_path, orient="records", lines=True)
        except:
            return None

        try:
            df_toggl.date = df_toggl.date.apply(lambda x: pd.to_datetime(x))
            df_toggl.start = df_toggl.start.apply(lambda x: pd.to_datetime(x))
            df_toggl.lunes_semana = df_toggl.lunes_semana.apply(
                lambda x: pd.to_datetime(x)
            )
        except:
            return None

        return df_toggl

    def read_cache(self, start_date, end_date):
        if os.path.exists(self.toggl_cache_path):
            dates = pd.date_range(start_date, end_date)
            dates = [d.strftime("%Y-%m-%d") for d in dates]
            df_toggl = pd.DataFrame()
            for d in dates:
                df = self.read_cache_day(d)
                if df is not None:
                    # df_toggl = df_toggl.append(df)
                    df_toggl = pd.concat([df_toggl, df])
            df_toggl.reset_index(inplace=True)
            df_toggl.drop("index", axis=1, inplace=True)
            return df_toggl

    def read_toggl(self, start_date, end_date):
        """
        This function returns a dataframe with all toggl_cache entries using the Toggl API
        Code insipired in https://bitbucket.org/baobabsoluciones/dt_misiones_ia/src/master/toggl_tools.py
        """
        # Getting workspaces info
        ws = requests.get(
            "https://api.track.toggl.com/api/v9/workspaces",
            headers=toggl_api_headers,
        )
        all_ws = ws.json()

        # Select columns which we need
        all_ws_filtered = [
            {
                "workspace": item["name"],
                "workspace_id": item["id"],
                "organization": item["organization_id"],
                "timeslot": item["at"],
            }
            for item in all_ws
        ]
        df_workspaces = pd.DataFrame(data=all_ws_filtered)
        df_workspaces = df_workspaces[df_workspaces.workspace.isin(["Socios", "AG"])].copy(deep=True)
        workspaces_ids = df_workspaces["workspace_id"].tolist()

        # Getting data from workspaces
        df_entries = pd.DataFrame()
        for i, workspace_id in enumerate(workspaces_ids):
            workspace_name = df_workspaces[df_workspaces.workspace_id == workspace_id]['workspace'].tolist()[0]
            data = requests.get(
                "https://api.track.toggl.com/api/v9/workspaces/"
                + str(workspaces_ids[i])
                + "/clients",
                headers=toggl_api_headers,
            )
            # if df.shape[0] == 0:
            df = pd.DataFrame(data=data.json())
            # else:
            #     df = pd.concat([df, pd.DataFrame(data=data.json())])

            url_details = "&client_ids=" + ','.join(str(value) for value in df.id.unique())
            #
            # # Getting records from workspaces
            page = 1
            total_count = 50
            while (total_count - (page - 1) * 50) > 0:

                print("Reading Toggl reports info from {} to {}. Worspace {}. Page {}".format(start_date, end_date, workspace_id, page))
                url = (
                        "https://api.track.toggl.com/reports/api/v2/details?page="
                        + str(page)
                        + "&workspace_id="
                        + str(workspace_id)
                        + "&since="
                        + str(start_date)
                        + "&until="
                        + str(end_date)
                        + url_details
                        + "&user_agent=alvaro.garcia@baobabsoluciones.es"
                )
                data = requests.get(
                    url,
                    headers=toggl_api_headers,
                )

                workspace = data.json()
                total_count = workspace["total_count"]

                # select columns which we need
                workspace_filtered = [
                    {
                        "description": item["description"],
                        "start": item["start"],
                        "client": item["client"],
                        "project": item["project"],
                        "duration": item["dur"],
                        "user_id": item["uid"],
                    }
                    for item in workspace["data"]
                ]
                df_workspace = pd.DataFrame(data=workspace_filtered)
                df_workspace['workspace'] = workspace_name
                if df_workspace.shape[0] > 0:
                    df_workspace = df_workspace[df_workspace['user_id']==TOGGL_USER_ALVARO]

                    df_entries = pd.concat(
                         [df_entries, df_workspace], ignore_index=True
                    )
                sleep(3)
                page += 1
        # ********************************************************************************************************
        if not df_entries.empty:
            df_entries["date"] = df_entries["start"].apply(
                lambda x: pd.to_datetime(x).strftime("%Y-%m-%d")
            )
            df_entries["date"] = df_entries["date"].apply(lambda x: pd.to_datetime(x))
            df_entries["h_toggl"] = df_entries["duration"] / 1000 / 60 / 60 # Toggl provides dur as milisecs
            df_entries["lunes_semana"] = df_entries["date"].apply(
                lambda x: pd.to_datetime(x)
                          - timedelta(days=pd.to_datetime(x).weekday() % 7)
            )
            print("info read")
            df_entries = df_entries[['date', 'client', 'project', 'h_toggl', 'description', 'start', 'lunes_semana', 'workspace']]

            if self.export_cache_to_json:
                self.df_toggl_to_json_files(df_entries)

        return df_entries

    def get_df_toggl(self):
        # If there are days to read from cache, this info is retrieved and stored in a dataframe
        # If there are no days, an empty dataframe is generated
        if len(self.dates_cache) > 0:
            start_date = self.dates_cache[0]
            end_date = self.dates_cache[len(self.dates_cache) - 1]
            print("reading Toggl cache for {} to {}".format(start_date, end_date))
            df_toggl = self.read_cache(start_date, end_date)
        else:
            df_toggl = pd.DataFrame()

        # If there are days to read directly from Toggle, information from each of those days is retrived as
        # a dataframe and appended to the existing one
        if len(self.dates_no_cache) > 0:
            start_date = self.dates_no_cache[0]
            end_date = self.dates_no_cache[len(self.dates_no_cache) - 1]
            # df_toggl = df_toggl.append(self.read_toggl(start_date, end_date))
            df_toggl_no_cache = self.read_toggl(start_date, end_date)
            df_toggl = pd.concat([df_toggl, df_toggl_no_cache])

        df_toggl.reset_index(inplace=True)
        df_toggl.drop("index", axis=True, inplace=True)
        self.df_toggl = df_toggl

    def df_toggl_to_json_files(self, df_toggl):
        """
        Given a dataframe with Toggl info, toggl_read is stored in as many json files as days
        """
        if self.toggl_cache_path is not None:
            df = df_toggl.copy(deep=True)
            # Todos los valores en la columna 'date' son objetos datetime
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            # Ahora se puede aplicar strftime para formatear las fechas
            df['date'] = df['date'].apply(lambda x: x.strftime("%Y-%m-%d") if not pd.isnull(x) else '')
            df.lunes_semana = df.lunes_semana.apply(lambda x: x.strftime("%Y-%m-%d"))
            df['start'] = pd.to_datetime(df['start'], errors='coerce', utc=True)
            df['start'] = df['start'].apply(lambda x: x.strftime("%Y-%m-%d") if not pd.isnull(x) else '')


            dates = pd.date_range(df.date.min(), df.date.max()).tolist()
            dates = [d.strftime("%Y-%m-%d") for d in dates]

            for d in dates:
                df_to_json = df[df.date == d]
                if df_to_json.shape[0] > 0:
                    entries = df_to_json.to_json(
                        orient="records", lines=True, date_format="iso"
                    )
                    file_path = "{}/{}.json".format(self.toggl_cache_path, d)
                    with open(file_path, "w") as f:
                        f.write(entries)
        else:
            print("no json files generated (no cache path given)")

    def df_toggl_to_csv(self, file_name):
        self.df_toggl.to_csv(file_name)

    def _get_dates_cache_no_cache(self, start_date, end_date):
        dates = pd.date_range(start_date, end_date)
        self.days_cache = max(0, len(dates) - self.days_no_cache)
        self.dates_cache = dates[0 : self.days_cache]
        self.dates_no_cache = dates[self.days_cache :]


if __name__ == "__main__":
    days_no_cache = 1
    start_date = pd.to_datetime("2024-06-01")
    end_date = pd.to_datetime("2024-06-07")
    # gsheet_to_read = ID_GSHEET_2324
    # export_to_ghseet = False

    toggl_read = ToggleObj(
        TOGGL_TOKEN_PATH,
        TOGGL_CACHE_PATH,
        start_date,
        end_date,
        days_no_cache,
        # id_gsheet=gsheet_to_read,
        # export_to_ghseet=export_to_ghseet,
    )
    toggl_read.read_toggl_NEW(start_date, end_date)
    print("done")
