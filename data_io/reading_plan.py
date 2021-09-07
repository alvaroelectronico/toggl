import pandas as pd

# This function gets all the information corresponding to the assigned time to compared with logged time.
def get_asignacion(file_path):
    df = pd.read_excel(file_path, sheet_name='asig_semana', usecols="A:E")
    df = df.drop(df[df.lunes_semana.isna()].index)
    return df