from mg_parse_gkg_cassandra import *
from collections import defaultdict 
from cassandra.concurrent import execute_concurrent_with_args # added so we can use concurrency in the query
#import numpy as np # we'll use numpy to get some extra info about mft variable distribution in this document
import pickle 

# make a new connection to the DB and create a session for that connection

daterange1 = [datetime(2015,12,1)+timedelta(x) for x in range(0,3)] # set timeframe for query here
daterange2 = [datetime(2015,12,1)+timedelta(x) for x in range(3,6)]
daterange3 = [datetime(2015,12,1)+timedelta(x) for x in range(6,9)]
daterange4 = [datetime(2015,12,1)+timedelta(x) for x in range(9,12)]
daterange5 = [datetime(2015,12,1)+timedelta(x) for x in range(12,15)]
daterange6 = [datetime(2015,12,1)+timedelta(x) for x in range(15,18)]
daterange7 = [datetime(2015,12,1)+timedelta(x) for x in range(18,21)]
daterange8 = [datetime(2015,12,1)+timedelta(x) for x in range(21,24)]
daterange9 = [datetime(2015,12,1)+timedelta(x) for x in range(24,27)]
daterange10 = [datetime(2015,12,1)+timedelta(x) for x in range(27,30)]
daterange11 = [datetime(2015,12,1)+timedelta(x) for x in range(30,31)]

dateranges = [daterange1]+[daterange2]+[daterange3]+[daterange4]+[daterange5]+[daterange6]+[daterange7]+[daterange8]+[daterange9]+[daterange10]+[daterange11]
  
  
#define the gcam variables of interest 
mft_vars = ['c25.{}'.format(i) for i in range(1,12)] # gcam variable names for mft are c25.1 through c25.11
LIWC_vars = ['c5.{}'.format(i) for i in range(1,63)]
wordnet_affect_vars = ['c14.{}'.format(i) for i in range(1,281)]

# prepare the CQL query w/in the active session -- this will catch any major mistakes in the query  

pickles = [x for x in range(0,12)]
c,s = establish_session()
q = s.prepare('select gkg_id, url, mft_data, gcam_data, event_ids, event_themes, event_locations, event_actors, named_entities, tone_avg, source, source_location,  wordcount, themes from gkg_record_by_da\
y where gkg_day = ?;')

for p in pickles:
    args = []
    for dates in dateranges[p]:
        args.append((dates,))
    print(args)
# bind the generic query to each particular argument and execute them concurrently, up to 32 at a time (in this case, will run all 6 days at once)
    print('starting with the {} gkg query'.format(p))
    results = execute_concurrent_with_args(s, q, args, concurrency=3) # tricky quirk of the cassandra driver -- must cast the date argument to a tuple, which NB requires the trailing comma!
                        
# initialize a nested dictionary for output
    data_dict = defaultdict(dict)
    for result in results:
        if result.success:
            for row in result.result_or_exc:
                data_dict[row.gkg_id]['url'] = row.url
                data_dict[row.gkg_id]['entities'] = row.named_entities
                data_dict[row.gkg_id]['themes'] = row.themes
                data_dict[row.gkg_id]['tone'] = row.tone_avg
                data_dict[row.gkg_id]['wordcount'] = row.wordcount
                data_dict[row.gkg_id]['source'] = row.source
                data_dict[row.gkg_id]['source_location'] = row.source_location
                data_dict[row.gkg_id]['event_ids'] = row.event_ids
                data_dict[row.gkg_id]['event_themes'] = row.event_themes
                data_dict[row.gkg_id]['event_locations'] = row.event_locations
                data_dict[row.gkg_id]['event_actors'] = row.event_actors
            
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

    pickle.dump(data_dict, open('gkg_2015_dec.pkl{}'.format(p),'wb'))
    print('successfully created the {} gkg file, continuing...'.format(p))



        
