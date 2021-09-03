from toggl.TogglPy import Toggl
import pandas as pd
import json
from datetime import *

# create a Toggl object and set our API key
toggl = Toggl()

# read Toggl token from txt file and coneect
f = open("..\data\\toggl_token.txt")
toggl_token = f.read()
f.close()
toggl.setAPIKey(toggl_token)

# Request parameters. So far, only start date

def get_json_entries(toggl, start_date="2021-09-03", end_date="2021-09-03"):
    request_config = dict(
        start_date="{}T00:00:00+02:00".format(start_date),
        end_date="{}T23:59:59+02:00".format(end_date)
    )

    # This returns information for all entries
    time_entries = toggl.request("https://api.track.toggl.com/api/v8/time_entries", parameters=request_config)

    # This list contains al entry ids to get the detailed info of each of them later on
    time_entry_ids = [i['id'] for i in time_entries]

    # json format dict with all data from entries
    all_entries = [toggl.request("https://api.track.toggl.com/api/v8/time_entries/{}".format(i))['data']
                   for i in time_entry_ids]

    return all_entries

def entries_to_json_file(entries, file_name):
    json_output = json.dumps(entries)
    with open('{}.json'.format(file_name), 'w') as f:
        f.write(json_output)


start_date = "2021-09-01"
end_date = "2021-09-03"
path = "..\data"

entries = get_json_entries(toggl, start_date=start_date, end_date=end_date)
df = pd.DataFrame.from_dict(entries)
df["date"] = df["start"].apply(
            lambda x: datetime.fromisoformat(x).date()
        )
dates = pd.date_range(start_date, end_date).tolist()
dates = [datetime.date(d) for d in dates]

for d in dates:
    df_to_json = df[df.date == d]
    entries = df_to_json.to_json
    file_path = "{}/{}".format(path, d)
    entries_to_json_file(entries, file_path)



