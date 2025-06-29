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
TOGGL_CACHE_PATH = get_abs_path("toggl_cache")
ASIGNACION_PATH = get_abs_path("asignacion.xlsx")
TOGGL_USER_ALVARO = 2833532

# Configuration
USE_CACHE = False
START_DATE_DEF = '2021-09-01'
END_DATE_DEF = datetime.today().strftime("%Y-%m-%d")


