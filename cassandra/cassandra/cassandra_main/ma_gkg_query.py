# Frederic R. Hopp
# Media Neuroscience Lab
# August 2018
# Query tool to obtain relevant
# GDELT GKG RECORDS for MA Thesis
# Must define n, YEAR and MONTHS below
# Adjust chunk size (n) if query becomes too large

from parse_gdelt import *
from collections import defaultdict
from cassandra.concurrent import execute_concurrent_with_args  # added so we can use concurrency in the query
import pandas as pd

# PREPROCESSING
gcam = pd.read_csv('GCAM-MASTER-CODEBOOK.csv')

# make a new connection to the DB and create a session for that connection
c, s = establish_session()
s.default_timeout = 60.0

# split months into sets of chunks of days to avoid memory failure, also determines the query concurreny
n = 2

year = 2018
months = [x for x in range(1, 10)]  # range is n-1
days = [x for x in range(0, 32)]
day_chunks = [days[i * n:(i + 1) * n] for i in range((len(days) + n - 1) // n)]

# define the gcam variables of interest
mft_vars = ['c25.{}'.format(i) for i in range(1, 12)]  # gcam variable names for mft are c25.1 through c25.11
mft_names = ['Care', 'Harm', 'Fairness', 'Cheating', 'Loyalty', 'Betrayal', 'Authority', 'Subversion', 'Purity',
             'Degradation',
             'MoralityGeneral']
mft_ind = ['Care', 'Harm', 'Fairness', 'Cheating']
mft_bind = ['Loyalty', 'Betrayal', 'Authority', 'Subversion', 'Purity', 'Degradation']

LIWC_vars = ['c5.{}'.format(i) for i in range(1, 63)]
liwc_vars = ['c5.{}'.format(i) for i in range(1, 63)]
liwc_names = gcam[gcam.Variable.isin(liwc_vars)].DimensionHumanName.values

final_frames = []
month_dicts = []

for month in months:
    daychunk_dicts = []
    for chunk in day_chunks:
        daterange = [datetime(year, month, 1) + timedelta(x) for x in chunk]

        q = s.prepare(
            "select gkg_id, url, mft_data, gcam_data, tone_avg, source, source_location "
            "from gkg_record_by_day where gkg_day = ? and source_location='US';")

        args = []

        for dt in daterange:
            args.append((dt,))

        results = execute_concurrent_with_args(s, q, args, concurrency=n)

        data_dict = defaultdict(dict)

        for result in results:
            if result.success:
                for row in result.result_or_exc:  # type:
                    date = row.gkg_id
                    data_dict[date]['url'] = row.url
                    data_dict[date]['tone'] = row.tone_avg
                    data_dict[date]['source'] = row.source

                    # while we're at it, let's make this a list so we can get
                    # some additional info beyond just the sum of mft vars...
                    mft_values = []
                    for k in mft_vars:
                        if row.mft_data and row.mft_data.get(k) is not None:
                            v = float(row.mft_data[k])  # type: float
                        else:
                            v = 0.
                        data_dict[date][k] = v
                        mft_values.append(v)

                    # liwc_values
                    for k in LIWC_vars:
                        if row.gcam_data and row.gcam_data.get(k) is not None:
                            v = float(row.gcam_data[k])
                        else:
                            v = 0.
                        data_dict[date][k] = v

            else:  # if the query failed, bubble up the exception that was thrown
                print('query failed!')
                raise result.result_or_exc

        daychunk_dicts.append(data_dict)
        print('success of querying chunk {}'.format(chunk))

    # combine data for a given month and turn into dataframe for preprocessing
    month_data = {k: v for d in daychunk_dicts for k, v in d.items()}
    print('monthly query succeeded, starting preprocessing now')

    df = pd.DataFrame.from_dict(month_data).T
    print(df.head())
    df = df.apply(pd.to_numeric, errors='ignore')
    try:
        df = df.drop_duplicates(subset='url')
    except KeyError as e:
        print('no url column found -- empty month?')

    else:
        print('dropped duplicate urls.')

    gkg_ids = df.index.values
    # df['gkg_id'] = gkg_ids
    df['date'] = gkg_ids
    df['date'] = df['date'].map(lambda z: z[:14])
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d%H%M%S')
    df = df.set_index(df['date'], drop=False)

    print("reindexing successfull, continuing...")

    # RENAMING VARIABLES
    newcols = dict(zip(mft_vars, mft_names))
    df.rename(columns=newcols, inplace=True)

    newcols = dict(zip(liwc_vars, liwc_names))
    df.rename(columns=newcols, inplace=True)

    df = df.sort_index()

    final_frames.append(df)
    print('successfully queried {} of {}'.format(month, year))

month_dicts = []

final_year = pd.concat(final_frames, ignore_index=True).set_index('date')
final_year.to_pickle('gkg_{}_1.pkl'.format(year))
