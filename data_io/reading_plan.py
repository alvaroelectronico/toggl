import pandas as pd

def get_asignacion(file_path):
    df = pd.read_excel(file_path, sheet_name='asig_semana')
    return df