import pandas as pd
from toggl.TogglPy import Toggl
import data_io.reading_toggl as tg
import data_io.reading_plan as xl

# create a Toggl object and set our API key
toggl = Toggl()

# read Toggl token from txt file and coneect
f = open("../data/toggl_token.txt")
toggl_token = f.read()
f.close()
toggl.setAPIKey(toggl_token)

start_date = "2021-08-30"
end_date = "2021-09-06"
path = "../data/toggl"

df_toggl = tg.get_toggl(toggl, start_date, end_date)
export_to_json = tg.df_toggl_to_json_files(path, df_toggl)
df_summary = (
    df_toggl[["lunes_semana", "client", "project", "h_registradas"]]
    .groupby(by=["lunes_semana", "client", "project"], as_index=False)
    .sum()
)

df_summary_ws = (
    df_toggl[["lunes_semana", "client", "project", "h_registradas", "workspace"]]
    .groupby(by=["lunes_semana", "workspace"], as_index=False)
    .sum()
)

print(df_summary_ws.head())

asignacion_path = "../data/asignacion.xlsx"
df_asig = xl.get_asignacion(asignacion_path)

df_toggl_asig = pd.merge(
    df_asig, df_summary, how="outer", on=["lunes_semana", "client", "project"]
)


df_toggl = tg.read_cache_toggl(path, start_date, end_date)
