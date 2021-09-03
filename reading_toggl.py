from toggl.TogglPy import Toggl
import pandas as pd

# create a Toggl object and set our API key
toggl = Toggl()

# read Toggl token from txt file and coneect
f = open("..\data\\toggl_token.txt")
toggl_token = f.read()
f.close()
toggl.setAPIKey(toggl_token)

# Request parameters. So far, only start date
request_config = dict(
    since="2021-01-01"
)

# This returns information for all entries
time_entries = toggl.request("https://api.track.toggl.com/api/v8/time_entries", parameters=request_config)

# This list contains al entry ids to get the detailed info of each of them later on
time_entry_ids = [i['id'] for i in time_entries]

# json format dict with all data from entries
all_entries = [toggl.request("https://api.track.toggl.com/api/v8/time_entries/{}".format(i))['data']
               for i in time_entry_ids]

# json to dataframe
df_toggl = pd.DataFrame.from_dict(all_entries)

# Checking if since and until worked fine
print("Records from: {} to {}".format(df_toggl.start.min(), df_toggl.start.max()))
print(df_toggl.start.tail(1))