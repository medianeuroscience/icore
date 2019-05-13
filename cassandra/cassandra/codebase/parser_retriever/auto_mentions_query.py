#Automated mention query script based on the last 7 days 

from mg_parse_gkg_cassandra import *
from collections import defaultdict 
from cassandra.concurrent import execute_concurrent_with_args # added so we can use concurrency in the query
import pickle 

# make a new connection to the DB and create a session for that connection
c,s = establish_session()

daterange = [datetime.today().replace(hour=0,minute=0,second=0,microsecond=0)-timedelta(x) for x in range(1,8)] 

# prepare the CQL query w/in the active session -- this will catch any major mistakes in the query  
q = s.prepare('select event_ids, gkg_day, url from gkg_record_by_day where gkg_day = ?;')

args = []

for dt in daterange:
        args.append((dt,))

# bind the generic query to each particular argument and execute them concurrently, up to 32 at a time (in this case, will run all 31 days at once)
results = execute_concurrent_with_args(s, q, args, concurrency=7) # tricky quirk of the cassandra driver -- must cast the date argument to a tuple, which NB requires the trailing comma!

# initialize a nested dictionary for output
data_dict = defaultdict(dict)

for result in results:
# the execute_concurrent functions return a higher-level object than session.execute
# need to check that the query ran successfully, then access all rows in result (nested ResultSet object in property .result_or_exc)
        if result.success:
                for row in result.result_or_exc:
                        data_dict[row.url]['event_ids'] = row.event_ids
                        data_dict[row.url]['date'] = row.gkg_day 
            

        else: # if the query failed, bubble up the exception that was thrown
                print 'query failed!'
                raise result.result_or_exc        
    

import datetime
pickle.dump(data_dict, open('mentions_{}.pkl'.format(datetime.date.today()), 'wb'))
