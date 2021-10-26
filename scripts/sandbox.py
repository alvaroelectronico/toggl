import pandas as pd
#from data_io.reading_toggl import *
from sympy.core.trace import Tr

import data_io.reading_toggl as tg
import data_io.reading_asig as xl
from datetime import *
from data.config import TOGGL_TOKEN_PATH, ASIGNACION_PATH

# Retrieving the toggle object
toggl = tg.get_toggl_obj(TOGGL_TOKEN_PATH)


def read_all_but_today_with_cache():
    # Get info from Toggle for today and yesterada
    start_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = datetime.today().strftime("%Y-%m-%d")
    df_toggl = tg.get_toggl_df(toggl, start_date, end_date, use_cache=False)

    start_date = "2021-09-01"
    end_date = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
    df_toggl2 = tg.get_toggl_df(toggl, start_date, end_date, use_cache=True)

    df_toggl = df_toggl.append(df_toggl2)

    df_toggl.reset_index(inplace=True)
    df_toggl.drop("index", axis=True, inplace=True)
    return df_toggl

def read_all_without_cache():
    start_date = "2021-09-07"
    end_date = datetime.today()
    dates = pd.date_range(start_date, end_date)
    df_toggl = pd.DataFrame()
    for d in dates:
        d_str = d.strftime("%Y-%m-%d")
        print("reading {}".format(d_str))
        df = tg.get_toggl_df(toggl, d_str, d_str, use_cache=False)
        print("read {}".format(d_str))
        df_toggl = df_toggl.append(df)
    df_toggl.reset_index(inplace=True)
    df_toggl.drop("index", axis=True, inplace=True)
    return df_toggl

def read_toggl(dates_cache, dates_no_cache):
    start_date = dates_cache[0]
    end_date = dates_cache[len(dates_cache)-1]
    df_toggl = tg.get_toggl_df(toggl, start_date, end_date, use_cache=True)
    for d in dates_no_cache:
        d_str = d.strftime("%Y-%m-%d")
        print("reading {}".format(d_str))
        df = tg.get_toggl_df(toggl, d_str, d_str, use_cache=False)
        print("read {}".format(d_str))
        df_toggl = df_toggl.append(df)
    df_toggl.reset_index(inplace=True)
    df_toggl.drop("index", axis=True, inplace=True)
    return df_toggl


def write_xlsx(df_toggl):
    # TODO: this line of code is a copy of that of df_toggl_cache. It works here, but now in the function
    # df_toggl["lunes_semana"] = df_toggl["lunes_semana"].astype("datetime64[ns]")

    # Agreggating info by lunes_semana, client, project
    df_summary_week = (
        df_toggl[["lunes_semana", "client", "project", "h_toggl"]]
        .groupby(by=["lunes_semana", "client", "project"], as_index=False)
        .sum()
    )

    # Agregatting info by workspaces
    df_summary_week_ws = (
        df_toggl[["lunes_semana", "client", "project", "h_toggl", "workspace"]]
        .groupby(by=["lunes_semana", "workspace"], as_index=False)
        .sum()
    )

    print(df_summary_week.head())

    # Getting information from asignacion.xlsx
    asignacion_path = ASIGNACION_PATH
    df_asig = xl.get_asignacion(asignacion_path)
    df_asig_week = df_asig.loc[df_asig.type == 's']
    df_asig_week = df_asig_week.drop(['type'], axis=1)
    df_asig_week["lunes_semana"] = df_asig_week["lunes_semana"].astype("datetime64[ns]")


    # Merging Toggl logged time and assigned time
    df_toggl_asig = pd.merge(
        df_asig_week, df_summary_week, how="outer", on=["lunes_semana", "client", "project"]
    )
    df_toggl_asig['h_toggl'] = df_toggl_asig['h_toggl'].fillna(0)
    df_toggl_asig['h_asig'] = df_toggl_asig['h_asig'].fillna(0)
    # df_toggl_asig['description'] = df_toggl_asig['description'].fillna("")
    df_toggl_asig['h_pendientes'] = df_toggl_asig['h_asig'] - df_toggl_asig['h_toggl']
    df_toggl_asig = df_toggl_asig[['lunes_semana', 'client', 'project', 'h_asig', 'h_toggl', 'h_pendientes']]
    # df_toggl_asig.loc['total'] = df_toggl_asig.sum(numeric_only=True)

    print(df_toggl_asig.head())


    df_summary_to_xlsx = df_summary_week.copy(deep=True)
    df_summary_to_xlsx.lunes_semana = df_summary_to_xlsx.lunes_semana.apply (lambda x: x.strftime("%d/%m/%Y"))
    df_summary_to_xlsx.to_excel(".\\data\\toggl_weekly.xlsx", sheet_name="toggl")

    # Agreggating info by date, client, project
    df_summary_day = (
        df_toggl[["date", "client", "project", "h_toggl"]]
        .groupby(by=["date", "client", "project"], as_index=False)
        .sum()
    )

    df_summary_all = (
        df_toggl[["date", "client", "project", "h_toggl"]]
        .groupby(by=["client", "project"], as_index=False)
        .sum()
    )

    df_summary_day.date = df_summary_day.date.apply (lambda x: x.strftime("%d/%m/%Y"))
    df_summary_day.to_excel(".\\data\\toggl_daily.xlsx", sheet_name="toggl")
    df_summary_all.to_excel(".\\data\\toggl_all.xlsx", sheet_name="toggl")
    return df_summary_day, df_summary_week, df_summary_all


# # df_toggl = read_all_without_cache()
# df_toggl = read_all_but_today_with_cache()

last_days_no_cache = 0
start_date = pd.to_datetime('2021-09-01')
end_date = pd.to_datetime(datetime.today())
dates_cache = pd.date_range(start_date, end_date - timedelta(days=last_days_no_cache+1))
dates_no_cache = pd.date_range(end_date - timedelta(days=last_days_no_cache), pd.to_datetime(datetime.today()))
df_toggl = read_toggl(dates_cache, dates_no_cache)
df_toggl = df_toggl.sort_values(by=['date'])
df_summary_day, df_summary_week, df_toggl_all = write_xlsx(df_toggl)

# df_toggl = tg.get_toggl_df(toggl, start_date='2021-09-12', end_date='2021-09-12', use_cache=False)

"""
dates = pd.date_range("2021-08-26", "2021-09-17")
for d in dates:
    date = d.strftime("%Y-%m-%d")
    print("reading {}".format(date))
    df_toggl = tg.get_toggl_df(toggl, start_date=date, end_date=date, use_cache=False)
    print("read {}".format(date))

df_toggl = tg.get_toggl_df(toggl, start_date='2021-09-16', end_date='2021-09-16', use_cache=False)
df_toggl.groupby(by='date')['h_toggl'].sum()

df_toggl = tg.get_toggl_df(toggl, start_date='2021-09-01', end_date='2021-09-17', use_cache=True)
df_toggl.groupby(by='date')['h_toggl'].sum()

df_toggl_05 = tg.get_toggl_df(toggl, start_date='2021-09-04', end_date='2021-09-04', use_cache=False)
df_toggl2 = tg.get_toggl_df(toggl, start_date='2021-09-04', end_date='2021-09-15', use_cache=True)
"""


