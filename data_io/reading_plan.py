import pandas as pd

def get_asignacion(file_path):
    df = pd.read_excel(file_path, sheet_name='asig_semana', usecols="A:E")
    df = df.drop(df_asig[df_asig.lunes_semana.isna()].index)
    return df