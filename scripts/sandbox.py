import pandas as pd
from datetime import *

import data.toggl_class as tg
from data.config import (
    TOGGL_TOKEN_PATH,
    TOGGL_CACHE_PATH,
    ID_GSHEET_2223
)

days_no_cache = 3
start_date = pd.to_datetime('2022-09-01')
end_date = pd.to_datetime(datetime.today())
# end_date = pd.to_datetime(datetime.today() + timedelta(days=1))
toggl2223 = tg.ToggleObj(TOGGL_TOKEN_PATH, TOGGL_CACHE_PATH, start_date, end_date, days_no_cache,
                         id_gsheet=ID_GSHEET_2223, export_to_ghseet=True)