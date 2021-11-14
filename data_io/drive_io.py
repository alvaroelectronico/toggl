'''
https://www.analyticsvidhya.com/blog/2020/07/read-and-update-google-spreadsheets-with-python/
'''
import df2gspread.df2gspread
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials


def get_client():
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

    # add credentials to the account
    creds = ServiceAccountCredentials.from_json_keyfile_name('.\data\\togglag_credentials.json', scope)

    # authorize the clientsheet
    client = gspread.authorize(creds)
    return client

def get_gsheet(client, id_gsheet):
    gsheet = client.open_by_key(id_gsheet)
    return gsheet

def get_sheet(gsheet, id_sheet):
    sheet = gsheet.get_worksheet_by_id(id_sheet)
    return sheet

def read_sheet(sheet):
    records_data = sheet.get_all_records()
    df = pd.DataFrame.from_dict(records_data)
    return df

def df_to_gsheet(df, sheet):
    def iter_pd(df):
        for val in df.columns:
            yield val
        for row in df.to_numpy():
            for val in row:
                if pd.isna(val):
                    yield ""
                else:
                    yield val

    (row, col) = df.shape
    cells = sheet.range("A1:{}".format(gspread.utils.rowcol_to_a1(row + 1, col)))
    for cell, val in zip(cells, iter_pd(df)):
        cell.value = val
    sheet.update_cells(cells)
    return 0


# id_gsheet = "1xxqT3cYf1CxJH9H1Ncgx32Lg833ycJVAwfCeXX2ee6Q"
# id_sheet_asig = 392173937
# id_sheet_prueba = 508370956

# client = get_client()
# gsheet_asig = get_gsheet(client, id_gsheet)
# sheet_asig = get_sheet(gsheet_asig, id_sheet_asig)
# sheet_prueba = get_sheet(gsheet_asig, id_sheet_prueba)
#
# df_asig = read_sheet(sheet_asig)
# print(df_asig.head())
# write = df_to_gsheet(df_asig.head(), sheet_prueba)

