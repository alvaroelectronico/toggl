import pandas as pd
import data_io.reading_toggl as tg
import data_io.reading_asig as xl
from datetime import *
from data.config import TOGGL_TOKEN_PATH, ASIGNACION_PATH, ID_SHEET_TOGGL_WEEKLY, ID_GSHEET, ID_SHEET_TOGGL_ALL, ID_SHEET_TOGGL_DAILY
import os
import data_io.drive_io as dr

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

def read_all_without_cache(start_date):
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

def get_df_to_export(df_toggl):
    df_summary_week = (
        df_toggl[["lunes_semana", "client", "project", "h_toggl"]]
        .groupby(by=["lunes_semana", "client", "project"], as_index=False)
        .sum()
    )

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

    df_summary_to_xlsx = df_summary_week.copy(deep=True)

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
    df_summary_week.lunes_semana = df_summary_week.lunes_semana.apply(lambda x: x.strftime("%d/%m/%Y"))
    df_summary_to_xlsx.lunes_semana = df_summary_to_xlsx.lunes_semana.apply(lambda x: x.strftime("%d/%m/%Y"))
    return df_summary_all, df_summary_day, df_summary_to_xlsx, df_summary_week

def write_xlsx(df_toggl):
    # TODO: this line of code is a copy of that of df_toggl_cache. It works here, but now in the function
    df_summary_all, df_summary_day, df_summary_to_xlsx, df_summary_week = get_df_to_export(df_toggl)

    get_abs_path = lambda f: os.path.realpath(os.path.join(os.path.dirname(__file__), '..', 'data', f))
        # try:
    df_summary_to_xlsx.to_excel(get_abs_path("toggl_weekly.xlsx"), sheet_name="toggl")
    df_summary_day.to_excel(get_abs_path("toggl_daily.xlsx"), sheet_name="toggl")
    df_summary_all.to_excel(get_abs_path("toggl_all.xlsx"), sheet_name="toggl")
    # except:
    #     df_summary_to_xlsx.to_excel(".\data\\toggl_weekly.xlsx", sheet_name="toggl")
    #     df_summary_day.to_excel(".\data\\toggl_daily.xlsx", sheet_name="toggl")
    #     df_summary_all.to_excel(get_abs_path(".\data\\toggl_all.xlsx"), sheet_name="toggl")

    return df_summary_day, df_summary_week, df_summary_all

def write_gsheet(df_toggl):
    client = dr.get_client()
    gsheet = dr.get_gsheet(client, ID_GSHEET)
    sheet_weekly = dr.get_sheet(gsheet, ID_SHEET_TOGGL_WEEKLY)
    sheet_daily = dr.get_sheet(gsheet, ID_SHEET_TOGGL_DAILY)
    sheet_all = dr.get_sheet(gsheet, ID_SHEET_TOGGL_ALL)
    df_summary_all, df_summary_day, df_summary_to_xlsx, df_summary_week = get_df_to_export(df_toggl)
    a =  dr.df_to_gsheet(df_summary_week, sheet_weekly)
    a *= dr.df_to_gsheet(df_summary_day, sheet_daily)
    a *= dr.df_to_gsheet(df_summary_all, sheet_all)
    return df_toggl

def read_toggl_write_gsheet(start_date='2021-09-01', end_date = pd.to_datetime(datetime.today()), last_days_no_cache=0):
    dates_cache = pd.date_range(start_date, end_date - timedelta(days=last_days_no_cache + 1))
    dates_no_cache = pd.date_range(end_date - timedelta(days=last_days_no_cache), end_date)
    df_toggl = read_toggl(dates_cache, dates_no_cache)
    df_toggl = df_toggl.sort_values(by=['date'])
    a = write_gsheet(df_toggl)
    return a



last_days_no_cache = 0
start_date = pd.to_datetime('2021-09-01')
end_date = pd.to_datetime(datetime.today())
# end_date = pd.to_datetime('2021-11-16')
df_toggl = read_toggl_write_gsheet(start_date, end_date, last_days_no_cache)


# df_toggl = read_all_without_cache(start_date="2021-10-18")
# df_toggl = read_all_but_today_with_cache()

# df_summary_day, df_summary_week, df_toggl_all = write_xlsx(df_toggl)
#df_weekly_ws = df_toggl.groupby(by=['lunes_semana', 'workspace'], as_index=False)['h_toggl'].sum()
#df_summary_all, df_summary_day, df_summary_to_xlsx, df_summary_week = get_df_to_export(df_toggl)


