#Automated query of gkg records for the last 7 days

from mg_parse_gkg_cassandra import *
from collections import defaultdict 
from cassandra.concurrent import execute_concurrent_with_args # added so we can use concurrency in the query
 #import numpy as np # we'll use numpy to get some extra info about mft variable distribution in this document
import pickle 

# make a new connection to the DB and create a session for that connection
c,s = establish_session()

daterange = [datetime.today().replace(hour=0,minute=0,second=0,microsecond=0)-timedelta(x) for x in range(1,5)] 
#daterange = [datetime(2018,2,1)+timedelta(x) for x in range(29,32)]

#define the gcam variables of interest 
mft_vars = ['c25.{}'.format(i) for i in range(1,12)] # gcam variable names for mft are c25.1 through c25.11
LIWC_vars = ['c5.{}'.format(i) for i in range(1,63)]
wordnet_affect_vars = ['c14.{}'.format(i) for i in range(1,281)]

# prepare the CQL query w/in the active session -- this will catch any major mistakes in the query  
q = s.prepare('select gkg_id, url, event_ids, mft_data, gcam_data, event_locations, event_actors, named_entities, tone_avg, source, source_location,  wordcount, themes from gkg_record_by_day where gkg_day = ?;')

args = []

for dt in daterange:
        args.append((dt,))

# bind the generic query to each particular argument and execute them concurrently, up to 32 at a time (in this case, will run all 31 days at once)
results = execute_concurrent_with_args(s, q, args, concurrency=4) # tricky quirk of the cassandra driver -- must cast the date argument to a tuple, which NB requires the trailing comma!

# initialize a nested dictionary for output
data_dict = defaultdict(dict)

for result in results:
    # the execute_concurrent functions return a higher-level object than session.execute
    # need to check that the query ran successfully, then access all rows in result (nested ResultSet object in property .result_or_exc)
    if result.success:
        for row in result.result_or_exc:
            data_dict[row.gkg_id]['url'] = row.url
            data_dict[row.gkg_id]['entities'] = row.named_entities
            data_dict[row.gkg_id]['themes'] = row.themes
            data_dict[row.gkg_id]['tone'] = row.tone_avg
            data_dict[row.gkg_id]['wordcount'] = row.wordcount
            data_dict[row.gkg_id]['source'] = row.source
            data_dict[row.gkg_id]['source_location'] = row.source_location
           
            # while we're at it, let's make this a list so we can get some additional info beyond just the sum of mft vars...
            mft_values = []
            for k in mft_vars:
                if row.mft_data and row.mft_data.get(k) != None:
                    v = float(row.mft_data[k])
                else:
                    v = 0.
                data_dict[row.gkg_id][k] = v
                mft_values.append(v)

            # liwc_values
            for k in LIWC_vars:
                if row.gcam_data and row.gcam_data.get(k) != None:
                    v = float(row.gcam_data[k])
                else:
                    v = 0.
                data_dict[row.gkg_id][k] = v

            # wordnet_values
            for k in wordnet_affect_vars:
                if row.gcam_data and row.gcam_data.get(k) != None:
                    v = float(row.gcam_data[k])
                else:
                    v = 0.
                data_dict[row.gkg_id][k] = v
        

    else: # if the query failed, bubble up the exception that was thrown
        print 'query failed!'
        raise result.result_or_exc        

for k, v in data_dict.items():
        for k1, v1 in v.items():
                if k1 == 'themes' and v1 != None:
                        data_dict[k][k1] = set(v1)
                if k1 == 'entities' and v1 != None:
                        data_dict[k][k1] = set(v1)    


#pickle.dump(data_dict, open('gkg_2018_02_09.pkl', 'wb'))
                        
import datetime
pickle.dump(data_dict, open('gkg_{}_1.pkl'.format(datetime.date.today()), 'wb'))



        
