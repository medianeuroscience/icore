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


# make a new connection to the DB and create a session for that connection
c, s = establish_session()

# split months into sets of chunks of days to avoid memory failure
# (lower number = smaller batches), also determines the query concurreny
n = 32

year = 2018
months = [x for x in range(1, 10)]  # range is n-1
days = [x for x in range(0, 32)]
day_chunks = [days[i * n:(i + 1) * n] for i in range((len(days) + n - 1) // n)]

final_frames = []
month_dicts = []

for month in months:
    daychunk_dicts = []
    for chunk in day_chunks:
        daterange = [datetime(year, month, 1) + timedelta(x) for x in chunk]

        q = s.prepare("select event_id, event_timestamp, event_mention, "
                      "from event_mention_date where event_timestamp = ?;")

        args = []

        for dt in daterange:
            args.append((dt,))

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

        daychunk_dicts.append(data_dict)
        print('success of querying chunk {}'.format(chunk))

    # combine data for a given month and turn into dataframe for preprocessing
    month_data = {k: v for d in daychunk_dicts for k, v in d.items()}
    print('monthly query succeeded, starting preprocessing now')

    df = pd.DataFrame.from_dict(month_data).T

    # Type Conversions
    df['mention_date'] = pd.to_datetime(df['mention_date'])

    # Dropping and FillNaN
    df = df.replace('', np.nan, regex=True)
    df = df.replace(-99, np.nan, regex=True)

    #df = df.set_index('mention_date')
    #df = df.sort_index()
    print(df.tail()['mention_date'])

    final_frames.append(df)
    print('successfully queried {} of {}'.format(month, year))

month_dicts = []

final_year = pd.concat(final_frames, ignore_index=True)
final_year.to_pickle('mentions_{}.pkl'.format(year))