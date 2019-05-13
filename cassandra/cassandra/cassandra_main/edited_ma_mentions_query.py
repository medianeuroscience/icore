# Frederic R. Hopp
# Media Neuroscience Lab
# August 2018
# Query tool to obtain relevant
# More fine grained timestamps of events for MA Thesis
# Must define n (chunk size), YEAR and MONTHS below
# Adjust chunk size (n) if query becomes too large


# SET n, months, and file name at bottom

from parse_gdelt import *
from collections import defaultdict
from cassandra.concurrent import execute_concurrent_with_args  # added so we can use concurrency in the query
import pandas as pd
import numpy as np
import glob


# make a new connection to the DB and create a session for that connection
c, s = establish_session()

n = 100

path = 'events_*'
files = glob.glob(path)
frames = []
for file in files:
    frames.append(pd.read_pickle(file))
events = pd.concat(frames).set_index('date')
events = events[events.num_articles >= 10].reset_index()
events = events.drop_duplicates('event_id')
print("Loaded Events.")

frames = np.array_split(events, 5)
events = []

final_frames = []

for frame in frames:

    q = s.prepare("select event_id, event_mention "
                  "from event_mention_date where event_id = ? allow filtering;")

    args = []
    for id in frame.event_id.values:
        args.append((id,))

    results = execute_concurrent_with_args(s, q, args, concurrency=n)

    data_dict = defaultdict(dict)

    for result in results:
        # the execute_concurrent functions return a higher-level object than session.execute
        # need to check that the query ran successfully, then access all rows in
        # result (nested ResultSet object in property .result_or_exc)
        if result.success:
            for row in result.result_or_exc:
                data_dict[row.event_id]['mention_date'] = row.event_mention
                data_dict[row.event_id]['event_id'] = row.event_id

        else:  # if the query failed, bubble up the exception that was thrown
            print('query failed!')
            raise result.result_or_exc

    #daychunk_dicts.append(data_dict)
    #print('success of querying chunk {}'.format(chunk))

# combine data for a given month and turn into dataframe for preprocessing
#month_data = {k: v for d in daychunk_dicts for k, v in d.items()}
#print('monthly query succeeded, starting preprocessing now')

    df = pd.DataFrame.from_dict(data_dict).T
    print(df.head())

    # Type Conversions
    df['mention_date'] = pd.to_datetime(df['mention_date'])

    # Dropping and FillNaN
    df = df.replace('', np.nan, regex=True)
    df = df.replace(-99, np.nan, regex=True)

    #df = df.set_index('mention_date')
    #df = df.sort_index()
    print(df.tail()['mention_date'])

    final_frames.append(df)
    #print('successfully queried {} of {}'.format(month, year))


final_data = pd.concat(final_frames, ignore_index=True)
final_data.to_pickle('mentions.pkl')