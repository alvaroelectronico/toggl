from datetime import *
import os
import pandas as pd

location = os.path.dirname(__file__)
get_abs_path = lambda f: os.path.join(location, f)

# Cache folder
TIMEFORK_CACHE_PATH = get_abs_path("timefork_cache")

# Configuration
START_DATE_DEF = '2021-09-01'
END_DATE_DEF = datetime.today().strftime("%Y-%m-%d")

