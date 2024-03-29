from datetime import *
import os
import pandas as pd

location = os.path.dirname(__file__)
get_abs_path = lambda f: os.path.join(location, f)

# Credential files
# TOGGL_TOKEN_PATH = "a"
TOGGL_TOKEN_PATH = get_abs_path("toggl_token.txt")
JSON_CRED_PATH = get_abs_path('togglag_credentials.json')

# Cache folder
TOGGL_CACHE_PATH = get_abs_path("toggl")
ASIGNACION_PATH = get_abs_path("asignacion.xlsx")

# Configuration
USE_CACHE = False
START_DATE_DEF = '2021-09-01'
END_DATE_DEF = datetime.today().strftime("%Y-%m-%d")

# Output files
ID_GSHEET_2122 = "1xxqT3cYf1CxJH9H1Ncgx32Lg833ycJVAwfCeXX2ee6Q"
ID_GSHEET_2223 = "1w4XmbxQv4SuilRu1Fp4xXgndU9Ss3-fcG7p7FBAR4mY"
ID_SHEET_ASIG = 392173937
ID_SHEET_TOGGL_WEEKLY = 266709097
ID_SHEET_TOGGL_DAILY = 1350237449
ID_SHEET_TOGGL_ALL = 1650948761

