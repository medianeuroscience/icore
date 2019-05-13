# Frederic R. Hopp
# Media Neuroscience Lab
# August 2018
# Query tool to obtain relevant
# GDELT Events for MA Thesis
# Must define n, YEAR and MONTHS below
# Adjust chunk size (n) if query becomes too large

# SET n, year, part(year half)

from parse_gdelt import *
from collections import defaultdict
from cassandra.concurrent import execute_concurrent_with_args  # added so we can use concurrency in the query
import pandas as pd
import numpy as np

# PREPROCESSING
CAMEO_COUNTRY = 'CAMEO.country.txt'
CAMEO_EVENT = 'CAMEO.eventcodes.txt'
CAMEO_GS = 'CAMEO.goldsteinscale.txt'
CAMEO_TYPE = 'CAMEO.type.txt'
FIPS = 'FIPS.country.txt'

# make a new connection to the DB and create a session for that connection
c, s = establish_session()

# split months into sets of chunks of days to avoid memory failure
# (lower number = smaller batches), also determines the query concurreny
n = 32

year = 2018
#TODO: Run event query for every second half of the left years.
part = 2

if part == 1:
    months = [x for x in range(1, 6)]  # range is n-1
else:
    months = [x for x in range(5, 10)]

days = [x for x in range(0, 32)]
day_chunks = [days[i * n:(i + 1) * n] for i in range((len(days) + n - 1) // n)]

final_frames = []
month_dicts = []

for month in months:
    daychunk_dicts = []
    for chunk in day_chunks:
        daterange = [datetime(year, month, 1) + timedelta(x) for x in chunk]

        q = s.prepare(
            'select event_id, event_day, source_url, num_articles, num_sources, event_tone_avg, event_goldstein, '
            'event_quadclass, action_geo_name, event_code, event_base_code,event_root_code '
            'from event_by_day where event_day = ?;')

        args = []

        for dt in daterange:
            args.append((dt,))

        results = execute_concurrent_with_args(s, q, args, concurrency=n)

        data_dict = defaultdict(dict)  # type: dict()

        for result in results:
            if result.success:
                for row in result.result_or_exc:
                    data_dict[row.event_id]['date'] = row.event_day
                    data_dict[row.event_id]['num_articles'] = row.num_articles
                    data_dict[row.event_id]['num_sources'] = row.num_sources
                    data_dict[row.event_id]['event_tone'] = row.event_tone_avg
                    data_dict[row.event_id]['event_goldstein'] = row.event_goldstein
                    data_dict[row.event_id]['quadclass'] = row.event_quadclass
                    data_dict[row.event_id]['event_code'] = row.event_code
                    data_dict[row.event_id]['event_base_code'] = row.event_base_code
                    data_dict[row.event_id]['event_root_code'] = row.event_root_code
                    data_dict[row.event_id]['action_geo_name'] = row.action_geo_name
                    data_dict[row.event_id]['event_url'] = row.source_url
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
    fips = pd.read_table(FIPS, header=None)
    event_code = pd.read_table(CAMEO_EVENT, dtype=str)
    quad_class = {1: 'Verbal Cooperation', 2: 'Material Cooperation', 3: 'Verbal Conflict', 4: 'Material Conflict'}

    # Mappings from Codebooks
    df['country'] = df['action_geo_name'].str[:2].map(fips.set_index(0)[1])
    df = df[df['country'] == 'United States']
    df['quadclass'].replace(quad_class, inplace=True)
    df['event_type'] = df['event_code'].map(event_code.set_index('CAMEOEVENTCODE')['EVENTDESCRIPTION'])

    # Type Conversions
    df['date'] = pd.to_datetime(df['date'])
    df['event_code'] = df['event_code'].astype(str)
    df.event_base_code = df.event_base_code.astype('str')

    # Dropping and FillNaN
    df = df.replace('', np.nan, regex=True)
    df = df.replace(-99, np.nan, regex=True)
    # del df['cameo_codes']

    # Final Indexing
    print(df.head().event_type)
    final_frames.append(df)
    print('successfully queried {} of {}'.format(month, year))

month_dicts = []

final_year = pd.concat(final_frames, ignore_index=True)
final_year.to_pickle('events_{}_{}.pkl'.format(year,part))
