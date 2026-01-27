import pandas as pd
from datetime import *
import numpy as np

import toggl_read.toggl_class as tg
from timelog_read.config import ID_GSHEET_2526
from timelog_read.timelog_class import TimelogObj

def main():
    # This has to be changed depending on the academic year to read
    gsheet_to_read = ID_GSHEET_2526
    csv_file_path = "..\\data\\toggl_all_2526.csv"

    start_date = pd.to_datetime("2025-09-01")
    end_date = pd.to_datetime(datetime.today() + timedelta(days=1))
    days_no_cache = 300
    export_to_ghseet = True
    export_to_csv = True
    export_cache_to_json = True
    get_entries = True


    timelog_read = TimelogObj(
        start_date=start_date,
        end_date=end_date,
        days_no_cache=days_no_cache,
        export_cache_to_json=export_cache_to_json,
        export_to_gsheet=export_to_ghseet,
        id_gsheet=gsheet_to_read
    )

    if export_to_csv:
        timelog_read.df_toggl.to_csv(csv_file_path)

    print("done")

if __name__ == "__main__":
    main()
