import pandas as pd
import data_io.reading_toggl as tg
import data_io.reading_plan as xl
from datetime import *
from data.config import TOGGL_TOKEN_PATH, ASIGNACION_PATH

start_date = "2021-09-06"
#start_date = (datetime.today() - timedelta(days=datetime.today().weekday() % 7)).strftime("%Y-%m-%d")
end_date = datetime.today().strftime("%Y-%m-%d")
# end_date = start_date

# Retrieving the toggle object
toggl = tg.get_toggl_obj(TOGGL_TOKEN_PATH)

# Getting a dataframe with Toggl info
df_toggl = tg.get_toggl_df(toggl, start_date, end_date, use_cache=True)
df_toggl_cache = tg.get_toggl_df(toggl, start_date, end_date, use_cache=True)

# TODO: this line of code is a copy of that of df_toggl_cache. It works here, but now in the function
# df_toggl["lunes_semana"] = df_toggl["lunes_semana"].astype("datetime64[ns]")

# Agreggating info by lunes_semana, client, project
df_summary = (
    df_toggl[["lunes_semana", "client", "project", "h_registradas"]]
    .groupby(by=["lunes_semana", "client", "project"], as_index=False)
    .sum()
)

# Agregatting info by workspaces
df_summary_ws = (
    df_toggl[["lunes_semana", "client", "project", "h_registradas", "workspace"]]
    .groupby(by=["lunes_semana", "workspace"], as_index=False)
    .sum()
)

print(df_summary_ws.head())

# Getting information from asignacion.xlsx
asignacion_path = ASIGNACION_PATH
df_asig = xl.get_asignacion(asignacion_path)

# Merging Toggl logged time and assigned time
df_toggl_asig = pd.merge(
    df_asig, df_summary, how="outer", on=["lunes_semana", "client", "project"]
)
df_toggl_asig['h_registradas'] = df_toggl_asig['h_registradas'].fillna(0)
df_toggl_asig['h_asignadas'] = df_toggl_asig['h_asignadas'].fillna(0)
df_toggl_asig['description'] = df_toggl_asig['description'].fillna("")
df_toggl_asig['h_pendientes'] = df_toggl_asig['h_asignadas'] - df_toggl_asig['h_registradas']
df_toggl_asig = df_toggl_asig[['lunes_semana', 'client', 'project', 'h_asignadas', 'h_registradas', 'h_pendientes', 'description']]
df_toggl_asig.loc['total'] = df_toggl_asig.sum(numeric_only=True)

print(df_toggl_asig.head())
